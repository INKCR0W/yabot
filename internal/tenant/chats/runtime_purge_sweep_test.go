package chats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/janitor"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
	"github.com/INKCR0W/yabot/internal/tenant/contract"
)

type runtimePurger func(context.Context, int64, func() bool) error

func (p runtimePurger) PurgeChat(ctx context.Context, id int64, valid func() bool) error {
	return p(ctx, id, valid)
}

func seedRemovedForSweep(h *runtimeHarness, id int64, deadline any) {
	h.t.Helper()
	now := h.now().Unix()
	h.exec(`INSERT INTO chats (chat_id, type, state, updated_at, removed_at, removed_reason, purge_after,
		member_event_date, member_event_update_id) VALUES (?, 'channel', 'removed', ?, ?, 'left', ?, ?, 1)`,
		id, now, now, deadline, now)
}

func TestRuntimePurgeSweepScansDueRemovedChatsUsingIndex(t *testing.T) {
	t.Parallel()
	const firstID int64 = -200
	failed := errors.New("injected purge failure")
	var calls []int64
	h := newRuntimeHarness(t, runtimeOptions{
		beforeStart: func(h *runtimeHarness) {
			cutoff := h.now().Unix()
			seedRemovedForSweep(h, firstID, cutoff-1)
			for id := firstID + 1; id < firstID+17; id++ {
				seedRemovedForSweep(h, id, cutoff)
			}
			seedRemovedForSweep(h, firstID-1, cutoff+1)
			seedRemovedForSweep(h, firstID-2, nil)
			h.seedActive(firstID-3, 1)
			h.exec(`UPDATE chats SET purge_after = ? WHERE chat_id = ?`, cutoff-1, firstID-3)
		},
		configure: func(d *Deps) {
			d.Purgers = []contract.ChatPurger{runtimePurger(func(_ context.Context, id int64, valid func() bool) error {
				calls = append(calls, id)
				if !valid() {
					t.Errorf("candidate %d is not valid", id)
				}
				if id == firstID && len(calls) == 1 {
					return failed
				}
				return nil
			})}
		},
	})
	for _, after := range []*purgeCandidate{nil, {id: firstID, purgeAfter: h.now().Unix()}} {
		query, args := purgeScanQuery(h.now().Unix(), after)
		var id, parent, unused int
		var detail string
		err := h.store.Read().QueryRowContext(t.Context(), "EXPLAIN QUERY PLAN "+query, args...).Scan(&id, &parent, &unused, &detail)
		if err != nil || !strings.Contains(detail, "SEARCH chats USING COVERING INDEX chats_state_purge_after") {
			t.Fatalf("purge scan plan = %q (%v), want a search using the purge index", detail, err)
		}
	}

	run := h.janitor["chats_purge"].run
	if n, err := run(t.Context()); n != 15 || !errors.Is(err, janitor.ErrBacklog) || len(calls) != 16 {
		t.Fatalf("first sweep deleted %d with %v, candidate calls = %v", n, err, calls)
	}
	if n, err := run(t.Context()); n != 1 || err != nil || len(calls) != 17 || calls[16] != firstID+16 {
		t.Fatalf("continuation deleted %d with %v, candidate calls = %v", n, err, calls)
	}
	if n, err := run(t.Context()); n != 1 || err != nil || len(calls) != 18 || calls[17] != firstID {
		t.Fatalf("next sweep deleted %d with %v, candidate calls = %v", n, err, calls)
	}
	for id := firstID - 3; id < firstID; id++ {
		assertRuntimePurgeState(h, id, h.snapshot(id), true)
	}
}

func TestRuntimePurgeSweepInvalidatesOldRemoval(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		leaveAgain bool
		finish     bool
	}{
		{name: "rejoin before the next batch"},
		{name: "rejoin and leave again in the same second", leaveAgain: true},
		{name: "rejoin and leave again after the last batch", leaveAgain: true, finish: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const id int64 = -250
			var h *runtimeHarness
			var calls []string
			var latest ChatSnapshot
			var version uint64
			h = newRuntimeHarness(t, runtimeOptions{
				beforeStart: func(h *runtimeHarness) { seedRemovedForSweep(h, id, h.now().Unix()) },
				configure: func(d *Deps) {
					d.Purgers = []contract.ChatPurger{
						runtimePurger(func(_ context.Context, chatID int64, valid func() bool) error {
							calls = append(calls, "first")
							if chatID != id || !valid() {
								t.Error("first purger cannot clear the original removal")
							}
							return nil
						}),
						runtimePurger(func(_ context.Context, chatID int64, valid func() bool) error {
							calls = append(calls, "second")
							if chatID != id || !valid() {
								t.Error("second purger cannot clear the original removal")
							}
							h.apply(purgeMemberUpdate(h, id, 2, models.ChatMemberTypeLeft, models.ChatMemberTypeAdministrator, true))
							h.nextRequest("getChat").Done(purgeLeaveResult(telegram.Retry))
							if tc.leaveAgain {
								h.apply(purgeMemberUpdate(h, id, 3, models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
							}
							latest, version = h.snapshot(id), h.chats.Version()
							if tc.finish {
								return nil
							}
							if valid() {
								t.Error("old removal is still valid before the next batch")
								return nil
							}
							return fmt.Errorf("batch stopped: %w", contract.ErrAborted)
						}),
					}
				},
			})
			original := h.snapshot(id)
			n, err := h.janitor["chats_purge"].run(t.Context())
			if n != 0 || err != nil || !slices.Equal(calls, []string{"first", "second"}) {
				t.Fatalf("sweep deleted %d with %v, purger order = %v", n, err, calls)
			}
			if latest.MemberEpoch <= original.MemberEpoch ||
				(tc.leaveAgain && (latest.State != Removed || latest.RemovedAt != original.RemovedAt)) ||
				(!tc.leaveAgain && latest.State != Active) {
				t.Fatalf("initial removal = %+v, new membership = %+v", original, latest)
			}
			assertRuntimePurgeState(h, id, latest, true)
			if got := h.chats.Version(); got != version {
				t.Fatalf("stale sweep changed view version from %d to %d", version, got)
			}
			if tc.finish {
				h.assertCounter(`yabot_chats_events_total{outcome="stale_completion"}`, 1)
			} else {
				h.assertCounter(`yabot_chats_purge_total{outcome="aborted"}`, 1)
				h.assertLog(slog.LevelInfo, "chat data purge aborted", "chat_id", id)
			}
		})
	}
}

func TestRuntimePurgeSweepSkipsCandidateWhoseSnapshotChanged(t *testing.T) {
	t.Parallel()
	for _, leaveAgain := range []bool{false, true} {
		name := "active snapshot"
		if leaveAgain {
			name = "new removal is not due"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			const firstID, secondID int64 = -300, -299
			var h *runtimeHarness
			var calls []int64
			h = newRuntimeHarness(t, runtimeOptions{
				beforeStart: func(h *runtimeHarness) {
					seedRemovedForSweep(h, firstID, h.now().Unix())
					seedRemovedForSweep(h, secondID, h.now().Unix())
				},
				configure: func(d *Deps) {
					d.Purgers = []contract.ChatPurger{runtimePurger(func(_ context.Context, id int64, _ func() bool) error {
						calls = append(calls, id)
						if id != firstID {
							return nil
						}
						h.apply(purgeMemberUpdate(h, secondID, 2, models.ChatMemberTypeLeft, models.ChatMemberTypeAdministrator, true))
						h.nextRequest("getChat").Done(purgeLeaveResult(telegram.Retry))
						if leaveAgain {
							h.apply(purgeMemberUpdate(h, secondID, 3, models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
						}
						return nil
					})}
				},
			})
			if n, err := h.janitor["chats_purge"].run(t.Context()); n != 1 || err != nil || !slices.Equal(calls, []int64{firstID}) {
				t.Fatalf("sweep deleted %d with %v, purger calls = %v", n, err, calls)
			}
			assertRuntimePurgeState(h, firstID, ChatSnapshot{}, false)
			assertRuntimePurgeState(h, secondID, h.snapshot(secondID), true)
		})
	}
}

func TestRuntimePurgeSweepRetriesPurgersAfterFailure(t *testing.T) {
	t.Parallel()
	const id int64 = -400
	const raw = "private purge driver details"
	for _, shutdown := range []bool{false, true} {
		name := "operation error"
		if shutdown {
			name = "shutdown during purge"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var calls []int
			h := newRuntimeHarness(t, runtimeOptions{
				beforeStart: func(h *runtimeHarness) { seedRemovedForSweep(h, id, h.now().Unix()) },
				configure: func(d *Deps) {
					d.Purgers = []contract.ChatPurger{
						runtimePurger(func(_ context.Context, _ int64, _ func() bool) error {
							calls = append(calls, 1)
							return nil
						}),
						runtimePurger(func(ctx context.Context, _ int64, _ func() bool) error {
							calls = append(calls, 2)
							if len(calls) != 2 {
								return nil
							}
							if shutdown {
								cancel()
								return &store.OpError{Err: ctx.Err()}
							}
							return &store.OpError{Err: errors.New(raw)}
						}),
						runtimePurger(func(_ context.Context, _ int64, _ func() bool) error {
							calls = append(calls, 3)
							return nil
						}),
					}
				},
			})
			run := h.janitor["chats_purge"].run
			before, version := h.snapshot(id), h.chats.Version()
			var wantErr error
			if shutdown {
				wantErr = context.Canceled
			}
			if n, err := run(ctx); n != 0 || !errors.Is(err, wantErr) || !slices.Equal(calls, []int{1, 2}) {
				t.Fatalf("first sweep deleted %d with %v, purger calls = %v", n, err, calls)
			}
			assertRuntimePurgeState(h, id, before, true)
			if shutdown {
				h.assertNoLog(slog.LevelError, "chat data purge failed", "chat_id", id)
				h.assertCounter(`yabot_chats_purge_total{outcome="error"}`, 0)
			} else {
				h.assertLog(slog.LevelError, "chat data purge failed", "chat_id", id)
				h.assertCounter(`yabot_chats_purge_total{outcome="error"}`, 1)
				h.logs.mu.Lock()
				logged := strings.Contains(h.logs.records.String(), raw)
				h.logs.mu.Unlock()
				if logged {
					t.Fatal("purge logged raw driver details")
				}
			}
			h.advance(time.Minute)
			if n, err := run(t.Context()); n != 1 || err != nil || !slices.Equal(calls, []int{1, 2, 1, 2, 3}) {
				t.Fatalf("next sweep deleted %d with %v, purger calls = %v", n, err, calls)
			}
			assertPublishedClear(h, id, version)
		})
	}
}

func TestRuntimePurgeSweepReplaysDeleteWithoutRerunningPurgers(t *testing.T) {
	t.Parallel()
	const id int64 = -500
	writeDone := make(chan error, 1)
	var calls int
	h := newRuntimeHarness(t, runtimeOptions{
		tickets:     1,
		beforeStart: func(h *runtimeHarness) { seedRemovedForSweep(h, id, h.now().Unix()) },
		configure: func(d *Deps) {
			d.Purgers = []contract.ChatPurger{runtimePurger(func(_ context.Context, _ int64, _ func() bool) error {
				calls++
				return nil
			})}
			actual := d.Store
			d.Store = maintenanceStore{Store: actual, write: func(op store.Op, done func(error)) error {
				return actual.Write(op, func(err error) {
					done(err)
					writeDone <- err
				})
			}}
		},
	})
	h.exec(`CREATE TRIGGER reject_chat_delete BEFORE DELETE ON chats
		BEGIN SELECT raise(ABORT, 'injected delete error'); END`)
	before, version := h.snapshot(id), h.chats.Version()
	type sweepResult struct {
		n   int
		err error
	}
	result := make(chan sweepResult, 1)
	go func() {
		n, err := h.janitor["chats_purge"].run(t.Context())
		result <- sweepResult{n, err}
	}()
	var opErr *store.OpError
	if err := runtimeReceive(t, writeDone); !errors.As(err, &opErr) {
		t.Fatalf("first delete returned %v, want an operation error", err)
	}
	h.barrier(id)
	assertRuntimePurgeState(h, id, before, true)
	if got := h.chats.Version(); got != version {
		t.Fatalf("failed delete changed view version from %d to %d", version, got)
	}
	h.exec(`DROP TRIGGER reject_chat_delete`)
	h.advance(time.Second)
	h.tick()
	if got := runtimeReceive(t, result); got.n != 1 || got.err != nil || calls != 1 {
		t.Fatalf("replayed sweep = %+v, purger calls = %d", got, calls)
	}
	assertPublishedClear(h, id, version)
	h.assertProbeTicket(0)
}

func TestRuntimePurgeClearsDataThroughScheduledJanitor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const id int64 = -600
		var j *janitor.Janitor
		var calls []int64
		h := newRuntimeHarness(t, runtimeOptions{
			beforeStart: func(h *runtimeHarness) {
				h.seedActive(id, 1)
				seedPurgeSettings(h, id)
				h.exec(`INSERT INTO settings_audit (chat_id, feature, diff, created_at) VALUES (?, 'general', '{}', ?)`, id, h.now().Unix())
			},
			configure: func(d *Deps) {
				j = janitor.New(janitor.Deps{Metrics: d.Metrics, Log: d.Log})
				d.Janitor = j
				d.Purgers = []contract.ChatPurger{
					d.Settings,
					runtimePurger(func(_ context.Context, chatID int64, valid func() bool) error {
						calls = append(calls, chatID)
						if !valid() {
							return contract.ErrAborted
						}
						return nil
					}),
				}
			},
		})
		leave, result := startRuntimePurge(h, t.Context(), id)
		runPurgeLeaveRequest(t, leave, 1)
		if err := runtimeReceive(t, result); err != nil {
			t.Fatal(err)
		}
		removed, version := h.snapshot(id), h.chats.Version()
		if removed.State != Removed || removed.PurgeAfter != h.now().Unix() {
			t.Fatalf("purge entry published %+v, want removed with immediate deadline", removed)
		}
		assertPurgeSettings(h, id)
		if err := j.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer runtimeStop(t, j.Stop)

		time.Sleep(time.Minute - time.Nanosecond)
		synctest.Wait()
		assertRuntimePurgeState(h, id, removed, true)
		h.assertCounter(`yabot_janitor_runs_total{task="chats_purge"}`, 0)
		time.Sleep(time.Nanosecond)
		synctest.Wait()

		if !slices.Equal(calls, []int64{id}) {
			t.Fatalf("scheduled purger calls = %v, want [%d]", calls, id)
		}
		assertPublishedClear(h, id, version)
		var data, audit int
		err := h.store.Read().QueryRowContext(t.Context(), `SELECT
			(SELECT count(*) FROM chat_settings WHERE chat_id = ?),
			(SELECT count(*) FROM settings_audit WHERE chat_id = ?)`, id, id).Scan(&data, &audit)
		if err != nil || data != 0 || audit != 0 || h.chats.general.Get(id).Language != "auto" {
			t.Fatalf("purge left settings rows=%d, audit rows=%d, read error=%v", data, audit, err)
		}
		h.assertCounter(`yabot_janitor_rows_total{task="chats_purge"}`, 1)
		time.Sleep(time.Minute - time.Nanosecond)
		synctest.Wait()
		h.assertCounter(`yabot_janitor_runs_total{task="chats_purge"}`, 1)
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		h.assertCounter(`yabot_janitor_runs_total{task="chats_purge"}`, 2)
	})
}

func assertPublishedClear(h *runtimeHarness, id int64, version uint64) {
	h.t.Helper()
	assertRuntimePurgeState(h, id, ChatSnapshot{}, false)
	changes, now, full := h.chats.ChangesSince(version)
	want := []Change{{ChatID: id, State: NoRecord, Version: version + 1}}
	if full || now != version+1 || h.chats.Version() != now || !slices.Equal(changes, want) {
		h.t.Fatalf("clear changes = %v, %d, %t, want %v", changes, now, full, want)
	}
}
