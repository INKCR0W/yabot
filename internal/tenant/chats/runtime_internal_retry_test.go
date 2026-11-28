package chats

import (
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/store"
)

func TestRuntimeAbandonsRepeatedMigrationOperationErrors(t *testing.T) {
	t.Parallel()
	const id = -100452
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
	const privateData = "private test chat title from a SQLite error"
	h.exec(`CREATE TRIGGER reject_migration BEFORE UPDATE ON chats WHEN NEW.state = 'removed'
		BEGIN SELECT raise(ABORT, '` + privateData + `'); END`)
	h.chats.ReportMigrated(id, id-1)
	h.tick()
	h.settleEffects(id)
	h.assertCounter(`yabot_chats_write_failures_total{outcome="op_error"}`, 1)
	member := h.update(runtimeUpdate(id, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, true))
	for range 3 {
		h.advance(5 * time.Second)
		h.tick()
		h.settleEffects(id)
	}
	if err := member.Wait(runtimeWait); err != nil {
		t.Fatalf("member update after abandoned migration: %v", err)
	}
	if got := h.snapshot(id); got.State != Active || got.MigratedTo != 0 || got.Watermark.ID != 2 {
		t.Fatalf("state after abandoning migration: %+v", got)
	}
	h.assertCounter(`yabot_chats_write_failures_total{outcome="op_error"}`, 4)
	h.assertCounter("yabot_chats_write_abandoned_total", 1)
	const sqliteConstraintTrigger = 1811
	if n := h.logCount(slog.LevelError, "chat write failed", "chat_id", id, "operation", "upsert_chat", "sqlite_code", sqliteConstraintTrigger); n != 4 {
		t.Errorf("write failure logs with the operation and SQLite code = %d, want 4", n)
	}
	h.assertLog(slog.LevelError, "chat event abandoned after write failures", "chat_id", id, "operation", "upsert_chat", "sqlite_code", sqliteConstraintTrigger)
	h.logs.mu.Lock()
	logText := h.logs.records.String()
	h.logs.mu.Unlock()
	if strings.Contains(logText, privateData) {
		t.Error("logs contain the private data embedded in the SQLite error")
	}
}

func TestRuntimeReplaysInternalEventAfterBatchCommitFailure(t *testing.T) {
	t.Parallel()
	const chatID, migratedTo = int64(-100453), int64(-100454)
	writes := make(chan error, 2)
	h := newRuntimeHarness(t, runtimeOptions{
		beforeStart: func(h *runtimeHarness) { h.seedActive(chatID, 1) },
		configure: func(d *Deps) {
			actual := d.Store
			d.Store = maintenanceStore{Store: actual, write: func(op store.Op, done func(error)) error {
				return actual.Write(op, func(err error) {
					done(err)
					writes <- err
				})
			}}
		},
	})
	h.exec(`CREATE TABLE migration_targets (id INTEGER PRIMARY KEY)`)
	h.exec(`CREATE TABLE migration_audit (target_id INTEGER REFERENCES migration_targets(id) DEFERRABLE INITIALLY DEFERRED)`)
	h.exec(`CREATE TRIGGER audit_migration AFTER UPDATE ON chats WHEN NEW.removed_reason = 'migrated'
		BEGIN INSERT INTO migration_audit VALUES (NEW.migrated_to); END`)
	before, version := h.snapshot(chatID), h.chats.Version()
	h.chats.ReportMigrated(chatID, migratedTo)
	h.tick()
	if err := runtimeReceive(t, writes); !errors.Is(err, store.ErrAborted) {
		t.Fatalf("internal migration failed with %v, want a transient batch abort", err)
	}
	h.barrier(chatID)
	h.assertCounter("yabot_store_batch_aborts_total", 1)
	h.assertCounter(`yabot_chats_write_failures_total{outcome="retry"}`, 1)
	h.assertCounter(`yabot_chats_write_failures_total{outcome="op_error"}`, 0)
	if got := h.snapshot(chatID); got != before || h.chats.Version() != version {
		t.Fatalf("aborted migration published a change: %+v", got)
	}
	var state ChatState
	var target int64
	readMigration := func() {
		t.Helper()
		err := h.store.Read().QueryRowContext(t.Context(), `SELECT state, coalesce(migrated_to, 0) FROM chats WHERE chat_id = ?`, chatID).
			Scan(&state, &target)
		if err != nil {
			t.Fatal(err)
		}
	}
	readMigration()
	if state != Active || target != 0 {
		t.Fatalf("aborted migration stored state %s and target %d", state, target)
	}

	h.exec(`INSERT INTO migration_targets (id) VALUES (?)`, migratedTo)
	h.advance(5 * time.Second)
	h.tick()
	h.settleEffects(chatID)
	if got := h.snapshot(chatID); got.State != Removed || got.RemovedReason != RemovedMigrated ||
		got.MigratedTo != migratedTo || got.MemberEpoch != before.MemberEpoch || h.chats.Version() != version+1 {
		t.Fatalf("internal migration did not replay after recovery: %+v", got)
	}
	readMigration()
	if state != Removed || target != migratedTo {
		t.Fatalf("replayed migration stored state %s and target %d", state, target)
	}
	h.assertCounter("yabot_store_batch_aborts_total", 1)
}

func TestRuntimeInternalWriteBackoffPreservesMailboxOrder(t *testing.T) {
	t.Parallel()
	const chatID = -100442
	var writes atomic.Int32
	h := newRuntimeHarness(t, runtimeOptions{
		beforeStart: func(h *runtimeHarness) { h.seedActive(chatID, 1) },
		configure: func(d *Deps) {
			actual := d.Store
			d.Store = maintenanceStore{Store: actual, write: func(op store.Op, done func(error)) error {
				if writes.Add(1) <= 2 {
					return store.ErrBusy
				}
				return actual.Write(op, done)
			}}
		},
	})
	h.chats.ReportMigrated(chatID, chatID-1)
	h.tick()
	h.barrier(chatID)
	queued := h.update(runtimeUpdate(chatID, 2, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false))
	for _, step := range []struct {
		advance time.Duration
		writes  int32
	}{
		{0, 1},
		{199 * time.Millisecond, 1},
		{time.Millisecond, 2},
		{399 * time.Millisecond, 2},
	} {
		h.advance(step.advance)
		h.tick()
		h.barrier(chatID)
		if got := writes.Load(); got != step.writes {
			t.Fatalf("write attempts after tick: %d, want %d", got, step.writes)
		}
		if got := h.snapshot(chatID); got.State != Active || h.water(chatID).ID != 1 {
			t.Fatalf("chat changed during backoff: %+v", got)
		}
		h.assertNoRequest()
	}
	h.advance(time.Millisecond)
	h.tick()
	if err := queued.Wait(runtimeWait); err != nil {
		t.Fatalf("membership queued behind migration: %v", err)
	}
	version := h.chats.Version()
	changes, _, full := h.chats.ChangesSince(version - 2)
	if full || len(changes) != 2 || changes[0].ChatID != chatID || changes[1].ChatID != chatID ||
		changes[0].State != Removed || changes[1].State != Onboarding {
		t.Fatalf("replay and queued membership were applied out of order: %+v", changes)
	}
	if got := writes.Load(); got != 4 || h.water(chatID).ID != 2 {
		t.Fatalf("replay and queued membership made %d writes, watermark = %d", got, h.water(chatID).ID)
	}
	h.assertCounter(`yabot_chats_write_failures_total{outcome="retry"}`, 2)
}
