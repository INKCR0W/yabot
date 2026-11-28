package chats

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/sender"
)

func TestRuntimePreloadRejectsInvalidStoredRows(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, statement, value, errText string
	}{
		{"invalid column", "UPDATE chats SET bot_rights = ? WHERE chat_id = -601", "raw invalid value", "decode stored chat record -601"},
		{"invalid state", "UPDATE chats SET state = ? WHERE chat_id = -601", "raw invalid state", "invalid state for stored chat record -601"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRuntimeHarness(t, runtimeOptions{deferChatsStart: true})
			h.seedActive(-601, 18)
			h.seedActive(-602, 18)
			h.exec("PRAGMA ignore_check_constraints = ON")
			h.exec(tc.statement, tc.value)
			h.exec("PRAGMA ignore_check_constraints = OFF")
			err := h.chats.Start(t.Context())
			if err == nil || !strings.Contains(err.Error(), tc.errText) || strings.Contains(err.Error(), tc.value) {
				t.Fatalf("preload error = %v", err)
			}
			h.assertCounter("yabot_chats_load_errors_total", 1)
			h.assertLog(slog.LevelError, "chats preload failed", "error", tc.errText)
			if got := h.store.Read().Stats().InUse; got != 0 {
				t.Fatalf("failed preload retained %d reader connections", got)
			}
			if h.chats.Version() != 0 {
				t.Fatal("preload published a partial view after a malformed row")
			}
			h.chats.ReportUnknown(-601)
			h.tick()
			h.barrier(-601)
			h.assertNoRequest()
		})
	}
}

func TestRuntimePreloadReadsRemovedRecordAndPreservesWatermark(t *testing.T) {
	t.Parallel()
	const id int64 = -603
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
		date := h.now().Unix()
		h.exec(`INSERT INTO chats (
			chat_id, type, title, state, bot_rights, guard_assigned, owner_id, added_at, updated_at, ever_active,
			removed_at, removed_reason, migrated_to, purge_after, member_event_date, member_event_update_id
		) VALUES (?, 'group', 'Stored title', 'removed', 8, 1, 11, ?, ?, 1, ?, 'migrated', ?, ?, ?, 21)`,
			id, date-3, date-2, date-2, id-1, date+100, date-1)
	}})
	date := h.now().Unix()
	expected := ChatSnapshot{
		ChatID: id, Type: models.ChatTypeGroup, Title: "Stored title", State: Removed, BotRights: InviteUsers,
		GuardAssigned: true, OwnerID: 11, AddedAt: date - 3, UpdatedAt: date - 2, EverActive: true,
		RemovedAt: date - 2, RemovedReason: RemovedMigrated, MigratedTo: id - 1, PurgeAfter: date + 100,
		MemberEpoch: 1, Watermark: dispatch.Order{Date: date - 1, ID: 21},
	}
	if got := h.snapshot(id); got != expected {
		t.Fatalf("preloaded snapshot = %+v, want %+v", got, expected)
	}
	h.apply(runtimeUpdate(id, 20, date, models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false))
	if got := h.snapshot(id); got != expected {
		t.Fatalf("older join changed preloaded record: %+v", got)
	}
	h.assertNoRequest()
}

type preloadDispatcher struct {
	Dispatcher
	submit func(dispatch.Key, dispatch.Task) error
	runAll func(context.Context, dispatch.AsyncTask) error
}

func (d preloadDispatcher) Submit(key dispatch.Key, task dispatch.Task) error {
	if d.submit != nil {
		return d.submit(key, task)
	}
	return d.Dispatcher.Submit(key, task)
}

func (d preloadDispatcher) RunOnAll(ctx context.Context, task dispatch.AsyncTask) error {
	if d.runAll != nil {
		return d.runAll(ctx, task)
	}
	return d.Dispatcher.RunOnAll(ctx, task)
}

func TestRuntimePreloadHandlesDispatcherFailures(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                  string
		submitErr, barrierErr error
		cancelAfterBusy       bool
		want                  error
	}{
		{name: "submission closed", submitErr: dispatch.ErrClosed, want: dispatch.ErrClosed},
		{name: "barrier closed", barrierErr: dispatch.ErrClosed, want: dispatch.ErrClosed},
		{name: "canceled after busy", submitErr: dispatch.ErrBusy, cancelAfterBusy: true, want: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const id = -604
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var submitted bool
			h := newRuntimeHarness(t, runtimeOptions{deferChatsStart: true, configure: func(deps *Deps) {
				actual := deps.Dispatcher
				deps.Dispatcher = preloadDispatcher{
					Dispatcher: actual,
					submit: func(key dispatch.Key, task dispatch.Task) error {
						if !submitted && tc.submitErr != nil {
							submitted = true
							if tc.cancelAfterBusy {
								cancel()
							}
							return tc.submitErr
						}
						return actual.Submit(key, task)
					},
					runAll: func(ctx context.Context, task dispatch.AsyncTask) error {
						if tc.barrierErr != nil {
							return tc.barrierErr
						}
						return actual.RunOnAll(ctx, task)
					},
				}
			}})
			h.seedActive(id, 1)
			err := h.chats.Start(ctx)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Start error = %v, want %v", err, tc.want)
			}
			if _, exists := h.chats.State(id); exists {
				t.Fatal("failed preload published a snapshot")
			}
			h.assertCounter("yabot_chats_load_errors_total", 1)
			if tc.submitErr == dispatch.ErrBusy {
				h.assertCounter("yabot_chats_preload_busy_total", 1)
			}
		})
	}
}

func TestRuntimePreloadReportsDatabaseQueryFailure(t *testing.T) {
	t.Parallel()
	h := newRuntimeHarness(t, runtimeOptions{deferChatsStart: true})
	h.exec("DROP TABLE chats")
	ctx, cancel := context.WithTimeout(t.Context(), runtimeWait)
	defer cancel()
	if err := h.chats.Start(ctx); err == nil || !strings.Contains(err.Error(), "query chats for preload") {
		t.Fatalf("preload error = %v, want a database query error", err)
	}
	h.assertCounter("yabot_chats_load_errors_total", 1)
}

func TestRuntimePreloadDoesNotPublishWhenIterationFailsAfterAValidRow(t *testing.T) {
	t.Parallel()
	const id = -610
	h := newRuntimeHarness(t, runtimeOptions{deferChatsStart: true})
	h.seedActive(id, 1)
	h.exec("ALTER TABLE chats RENAME TO preload_fixture")
	h.exec(`CREATE VIEW chats AS
		SELECT chat_id, type, title, state, bot_rights, guard_assigned, owner_id, added_at,
			updated_at, ever_active, removed_at, removed_reason, migrated_to, purge_after, member_event_date, member_event_update_id
		FROM preload_fixture
		UNION ALL
		SELECT -611, 'group', '', 'active', abs(-9223372036854775808), 0, NULL, NULL,
			1, 0, NULL, NULL, NULL, NULL, 0, 0`)
	err := h.chats.Start(t.Context())
	if err == nil || !strings.Contains(err.Error(), "read chats for preload") {
		t.Fatalf("preload error = %v, want SQLite iteration error", err)
	}
	if _, exists := h.chats.State(id); exists || h.chats.Version() != 0 {
		t.Fatal("preload published a partial view after a database iteration error")
	}
	h.assertCounter("yabot_chats_load_errors_total", 1)
}

func TestRuntimePreloadCancellationDoesNotSubmitRows(t *testing.T) {
	t.Parallel()
	var submissions int
	h := newRuntimeHarness(t, runtimeOptions{deferChatsStart: true, configure: func(d *Deps) {
		underlying := d.Dispatcher
		d.Dispatcher = preloadDispatcher{Dispatcher: underlying, submit: func(key dispatch.Key, task dispatch.Task) error {
			submissions++
			return underlying.Submit(key, task)
		}}
	}})
	h.seedActive(-608, 1)
	ctx, cancel := context.WithCancel(t.Context())
	err := readRecords(ctx, h.store.Read(), func(record Record) error {
		cancel()
		return h.chats.submitPreload(ctx, record)
	})
	if !errors.Is(err, context.Canceled) || submissions != 0 {
		t.Fatalf("canceled preload: dispatcher submissions %d, error %v", submissions, err)
	}
	if err := h.chats.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { runtimeStop(t, h.chats.Stop) })
	if got := h.snapshot(-608); got.MemberEpoch != 1 {
		t.Fatalf("canceled preload affected membership epoch: %+v", got)
	}
}

func TestRuntimePreloadBacksOffBetweenBusySubmissions(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		counters := metrics.NewSet()
		var attemptedAt []time.Time
		chats := Chats{
			metrics: counters,
			dispatch: preloadDispatcher{submit: func(dispatch.Key, dispatch.Task) error {
				attemptedAt = append(attemptedAt, time.Now())
				if len(attemptedAt) == 1 {
					return dispatch.ErrBusy
				}
				return nil
			}},
		}
		if err := chats.submitPreload(t.Context(), Record{ChatID: -609}); err != nil {
			t.Fatalf("preload submit error = %v", err)
		}
		if len(attemptedAt) != 2 || !attemptedAt[1].After(attemptedAt[0]) {
			t.Fatalf("busy preload retried without a delay: %v", attemptedAt)
		}
		if got := counters.GetOrCreateCounter("yabot_chats_preload_busy_total").Get(); got != 1 {
			t.Fatalf("busy preload count = %d, want 1", got)
		}
	})
}

func TestRuntimeDefaultIntervalsAndLogging(t *testing.T) {
	t.Parallel()
	const active, idle = -605, -606
	h := newRuntimeHarness(t, runtimeOptions{
		beforeStart: func(h *runtimeHarness) {
			h.seedActive(active, 21)
			h.exec(`INSERT INTO chats (chat_id, type, state, updated_at) VALUES (?, 'supergroup', 'onboarding', ?)`,
				idle, h.now().Add(-7*24*time.Hour+time.Second).Unix())
		},
		configure: func(d *Deps) {
			d.RemovedRetention, d.IdleLeaveAfter, d.ProbePeriodic, d.ProbePeriodicRPS = 0, 0, 0, 0
			d.Log = nil
		},
	})
	req := h.startReportedProbe(active)
	req.Done(sender.DroppedResult(sender.Busy, sender.ErrBusy))
	h.settleEffects(active)
	h.assertNoLog(slog.LevelError, "dispatch task panicked")
	if got, err := h.janitor["chats_idle"].run(t.Context()); err != nil || got != 0 {
		t.Fatalf("idle task just before boundary = %d, %v", got, err)
	}
	h.advance(time.Second)
	if got, err := h.janitor["chats_idle"].run(t.Context()); err != nil || got != 1 {
		t.Fatalf("idle task at boundary = %d, %v", got, err)
	}
	if req = h.nextRequest("getChat"); req.ChatID != idle {
		t.Fatalf("expected idle probe, got chat %d", req.ChatID)
	}
	req.Done(sender.DroppedResult(sender.Busy, sender.ErrBusy))
	h.settleEffects(idle)
	h.advance(24*time.Hour - 2*time.Second)
	if got, err := h.janitor["chats_probe"].run(t.Context()); err != nil || got != 0 {
		t.Fatalf("periodic task just before boundary = %d, %v", got, err)
	}
	h.advance(time.Second)
	if got, err := h.janitor["chats_probe"].run(t.Context()); err != nil || got != 1 {
		t.Fatalf("periodic task at boundary = %d, %v", got, err)
	}
	req = h.nextRequest("getChat")
	if req.ChatID != active {
		t.Fatalf("expected periodic probe, got chat %d", req.ChatID)
	}
	req.Done(sender.DroppedResult(sender.Busy, sender.ErrBusy))
	h.settleEffects(active)
	h.apply(runtimeUpdate(active, 22, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
	if removed := h.snapshot(active); removed.PurgeAfter-removed.RemovedAt != int64((30*24*time.Hour)/time.Second) {
		t.Fatalf("default removal retention = %d seconds", removed.PurgeAfter-removed.RemovedAt)
	}
}
