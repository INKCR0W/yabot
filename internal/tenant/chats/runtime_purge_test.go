package chats

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

func TestRuntimePurgeLeaveOutcomes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		initial ChatState
		result  sender.Result
		removed RemovedReason
	}{
		{name: "missing ignores failure", result: purgeLeaveResult(telegram.Fatal)},
		{name: "left", initial: Active, result: purgeLeaveResult(telegram.OK), removed: RemovedByPurge},
		{name: "already gone", initial: Onboarding, result: sender.Result{Outcome: telegram.Outcome{Class: telegram.Done, Reason: telegram.ChatGone}}, removed: RemovedByPurge},
		{name: "forbidden", initial: Degraded, result: purgeLeaveResult(telegram.Forbidden), removed: RemovedByPurge},
		{name: "migrated", initial: Active, result: sender.Result{Outcome: telegram.Outcome{Class: telegram.Migrated, MigrateTo: runtimeMigrationTarget}}, removed: RemovedMigrated},
		{name: "retry", initial: Active, result: purgeLeaveResult(telegram.Retry)},
		{name: "dropped", initial: Onboarding, result: sender.DroppedResult(sender.Expired, nil)},
		{name: "unauthorized", initial: Degraded, result: purgeLeaveResult(telegram.Unauthorized)},
		{name: "conflict", initial: Active, result: purgeLeaveResult(telegram.Conflict)},
		{name: "permission", initial: Active, result: purgeLeaveResult(telegram.Permission)},
		{name: "fatal", initial: Active, result: purgeLeaveResult(telegram.Fatal)},
		{name: "unknown", initial: Active, result: purgeLeaveResult(telegram.Unknown)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const id = -100831
			h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
				if tc.initial != NoRecord {
					h.seedActive(id, 1)
					h.exec(`UPDATE chats SET state=?, guard_assigned=1, ever_active=? WHERE chat_id=?`, tc.initial, tc.initial == Active, id)
				}
			}})
			want, exists := h.chats.State(id)
			h.advance(time.Minute)
			leave, result := startRuntimePurge(h, t.Context(), id)
			leave.Done(tc.result)
			err := runtimeReceive(t, result)
			if exists && tc.removed == "" {
				assertPurgeTelegramError(t, err, tc.result.Outcome)
			} else if err != nil {
				t.Fatalf("Purge returned %v, want success", err)
			}
			if tc.removed != "" {
				want.State, want.BotRights = Removed, 0
				want.RemovedAt, want.UpdatedAt, want.PurgeAfter = h.now().Unix(), h.now().Unix(), h.now().Unix()
				want.RemovedReason, want.MigratedTo = tc.removed, tc.result.Outcome.MigrateTo
			}
			assertRuntimePurgeState(h, id, want, exists)
			assertRuntimePurgeState(h, runtimeMigrationTarget, ChatSnapshot{}, false)
			h.assertNoRequest()
		})
	}
}

func TestRuntimePurgeUsesMembershipAtVerdict(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		initialAbsent bool
		steps         []string
		outcome       telegram.Outcome
		changed       bool
	}{
		{name: "left only", steps: []string{"left"}, outcome: telegram.Outcome{Class: telegram.Retry}},
		{name: "left and rejoined", steps: []string{"left", "joined"}, outcome: telegram.Outcome{Class: telegram.OK}, changed: true},
		{name: "old left ignored after rejoin", steps: []string{"joined", "left"}, outcome: telegram.Outcome{Class: telegram.Migrated, MigrateTo: runtimeMigrationTarget}, changed: true},
		{name: "new record while waiting", initialAbsent: true, steps: []string{"joined"}, outcome: telegram.Outcome{Class: telegram.Unauthorized}, changed: true},
		{name: "permission change preserves epoch", steps: []string{"downgraded"}, outcome: telegram.Outcome{Class: telegram.OK}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const id = -100832
			const setting = `{"language":"en"}`
			h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
				if !tc.initialAbsent {
					h.seedActive(id, 1)
				}
				h.exec(`INSERT INTO chat_settings (chat_id, feature, version, rev, data, etag, updated_at)
					VALUES (?, 'general', 1, 1, ?, 'saved', ?)`, id, setting, h.now().Unix())
			}})
			initial, _ := h.chats.State(id)
			leave, result := startRuntimePurge(h, t.Context(), id)
			for _, step := range tc.steps {
				switch step {
				case "left":
					h.apply(purgeMemberUpdate(h, id, 2, models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
				case "joined":
					h.apply(purgeMemberUpdate(h, id, 3, models.ChatMemberTypeLeft, models.ChatMemberTypeAdministrator, true))
				case "downgraded":
					h.apply(purgeMemberUpdate(h, id, 4, models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, false))
				}
			}
			want := h.snapshot(id)
			version := h.chats.Version()
			if changed := initial.MemberEpoch != want.MemberEpoch; changed != tc.changed {
				t.Fatalf("epoch %d → %d, want changed=%t", initial.MemberEpoch, want.MemberEpoch, tc.changed)
			}
			if want.State == Removed {
				h.exec(`CREATE TRIGGER protect_removed_row BEFORE UPDATE OF removed_reason, owner_id ON chats
					BEGIN SELECT raise(ABORT, 'unexpected change to the removed row'); END`)
			}
			h.advance(time.Minute)
			leave.Done(sender.Result{Outcome: tc.outcome})
			err := runtimeReceive(t, result)
			if tc.changed {
				if !errors.Is(err, ErrMembershipChanged) {
					t.Fatalf("Purge returned %v, want ErrMembershipChanged", err)
				}
			} else {
				if err != nil {
					t.Fatalf("Purge returned %v, want success", err)
				}
				if want.State == Degraded {
					want.State, want.BotRights = Removed, 0
					want.RemovedAt, want.UpdatedAt, want.RemovedReason = h.now().Unix(), h.now().Unix(), RemovedByPurge
					version++
				}
				want.PurgeAfter = h.now().Unix()
			}
			assertRuntimePurgeState(h, id, want, true)
			if got := h.chats.Version(); got != version {
				t.Fatalf("view version = %d, want %d", got, version)
			}
			var data string
			if err := h.store.Read().QueryRowContext(t.Context(), `SELECT data FROM chat_settings WHERE chat_id=?`, id).Scan(&data); err != nil || data != setting {
				t.Fatalf("saved setting = %q (%v), want %q", data, err, setting)
			}
		})
	}
}

func TestRuntimePurgeWaitsForExistingPersist(t *testing.T) {
	t.Parallel()
	for _, full := range []bool{false, true} {
		name := "deferred"
		if full {
			name = "mailbox full"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			const id = -100833
			delayed := &runtimeDelayedStoreDone{callbacks: make(chan func(), 1)}
			delayed.open.Store(true)
			h := newRuntimeHarness(t, runtimeOptions{
				beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) },
				configure: func(d *Deps) {
					delayed.Store = d.Store
					d.Store = delayed
				},
			})
			leave, result := startRuntimePurge(h, t.Context(), id)
			h.apply(purgeMemberUpdate(h, id, 2, models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
			removed := h.snapshot(id)
			delayed.open.Store(false)
			rejoin := h.update(purgeMemberUpdate(h, id, 3, models.ChatMemberTypeLeft, models.ChatMemberTypeAdministrator, true))
			publish := runtimeReceive(t, delayed.callbacks)
			defer publish()
			if full {
				filled := make(chan int, 1)
				if err := h.dispatcher.Submit(keyOf(id), func(_ context.Context, sh *dispatch.Shard) {
					item := dispatch.Item{Run: func(context.Context, *dispatch.Shard) {}, Abort: func(error) {}}
					count := 0
					for sh.Defer(keyOf(id), item) {
						count++
					}
					filled <- count
				}); err != nil {
					t.Fatal(err)
				}
				if count := runtimeReceive(t, filled); count != 16 {
					t.Fatalf("filled %d external slots, want 16", count)
				}
			}
			leave.Done(purgeLeaveResult(telegram.OK))
			h.barrier(id)
			select {
			case err := <-result:
				t.Fatalf("Purge settled before the existing Persist: %v", err)
			default:
			}
			if got := h.snapshot(id); got != removed {
				t.Fatalf("snapshot changed ahead of publication: got %+v, want %+v", got, removed)
			}
			delayed.open.Store(true)
			publish()
			if err := rejoin.Wait(runtimeWait); err != nil {
				t.Fatal(err)
			}
			if err := runtimeReceive(t, result); !errors.Is(err, ErrMembershipChanged) {
				t.Fatalf("deferred Purge returned %v, want ErrMembershipChanged", err)
			}
			got := h.snapshot(id)
			if got.State != Active || got.PurgeAfter != 0 || got.MemberEpoch == removed.MemberEpoch || got.Watermark.ID != 3 {
				t.Fatalf("Purge changed the committed rejoin: %+v", got)
			}
			assertRuntimePurgeState(h, id, got, true)
		})
	}
}

func TestRuntimePurgeRejectsOverloadBeforeLeave(t *testing.T) {
	t.Parallel()
	for _, capacity := range []string{"ticket", "mailbox"} {
		t.Run(capacity, func(t *testing.T) {
			t.Parallel()
			const id = -100843
			h := newRuntimeHarness(t, runtimeOptions{
				tickets: 1, beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) },
			})
			before := h.snapshot(id)
			if capacity == "ticket" {
				holdOnlyRuntimeTicket(t, h, id)
			} else {
				if err := h.dispatcher.Submit(keyOf(id), func(_ context.Context, sh *dispatch.Shard) {
					sh.Hold(keyOf(id))
					item := dispatch.Item{Run: func(context.Context, *dispatch.Shard) {}, Abort: func(error) {}}
					for sh.Defer(keyOf(id), item) {
					}
				}); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), runtimeWait)
			defer cancel()
			if err := h.chats.Purge(ctx, id); !errors.Is(err, ErrBusy) {
				t.Fatalf("Purge returned %v, want ErrBusy", err)
			}
			h.assertNoRequest()
			assertRuntimePurgeState(h, id, before, true)
		})
	}
}

func TestRuntimePurgeCompletesWithFullInbox(t *testing.T) {
	t.Parallel()
	const id = -100844
	h := newRuntimeHarness(t, runtimeOptions{
		tickets: 1, beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) },
	})
	leave, result := startRuntimePurge(h, t.Context(), id)
	entered, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	if err := h.dispatcher.Submit(keyOf(id), func(context.Context, *dispatch.Shard) {
		close(entered)
		<-resume
	}); err != nil {
		t.Fatal(err)
	}
	runtimeReceive(t, entered)
	for {
		err := h.dispatcher.Submit(keyOf(id), func(context.Context, *dispatch.Shard) {})
		if errors.Is(err, dispatch.ErrBusy) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), runtimeWait)
	defer cancel()
	if err := h.chats.Purge(ctx, id); !errors.Is(err, ErrBusy) {
		t.Fatalf("Purge entry with inbox full returned %v, want ErrBusy", err)
	}
	leave.Done(purgeLeaveResult(telegram.OK))
	release()
	if err := runtimeReceive(t, result); err != nil {
		t.Fatalf("Purge completion with inbox full returned %v", err)
	}
	if got := h.snapshot(id); got.State != Removed || got.PurgeAfter != h.now().Unix() {
		t.Fatalf("completion with inbox full published %+v", got)
	}
	h.assertProbeTicket(0)
	h.assertNoRequest()
}

func TestRuntimePurgeCancellationSettlesAcceptedWork(t *testing.T) {
	t.Parallel()
	for _, cancelAfterVerdict := range []bool{false, true} {
		name := "leave in flight"
		if cancelAfterVerdict {
			name = "verdict awaiting publication"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			const id = -100834
			delayed := &runtimeDelayedStoreDone{callbacks: make(chan func(), 1)}
			h := newRuntimeHarness(t, runtimeOptions{
				tickets:     1,
				beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) },
				configure: func(d *Deps) {
					delayed.Store = d.Store
					d.Store = delayed
				},
			})
			defer delayed.resume(h)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			before := h.snapshot(id)
			leave, result := startRuntimePurge(h, ctx, id)
			h.assertProbeTicket(1)
			var publish func()
			if cancelAfterVerdict {
				leave.Done(purgeLeaveResult(telegram.OK))
				publish = runtimeReceive(t, delayed.callbacks)
				defer publish()
			}
			cancel()
			if err := runtimeReceive(t, result); !errors.Is(err, context.Canceled) {
				t.Fatalf("Purge returned %v, want context.Canceled", err)
			}
			if got := h.snapshot(id); got != before {
				t.Fatalf("Purge published before settlement: %+v", got)
			}
			if !cancelAfterVerdict {
				assertRuntimePurgeState(h, id, before, true)
				leave.Done(purgeLeaveResult(telegram.OK))
				publish = runtimeReceive(t, delayed.callbacks)
			}
			delayed.open.Store(true)
			publish()
			h.settleEffects(id)
			want := before
			want.State, want.BotRights, want.RemovedReason = Removed, 0, RemovedByPurge
			want.RemovedAt, want.UpdatedAt, want.PurgeAfter = h.now().Unix(), h.now().Unix(), h.now().Unix()
			assertRuntimePurgeState(h, id, want, true)
			h.assertProbeTicket(0)
			h.apply(purgeMemberUpdate(h, id, 2, models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
			if got := h.water(id).ID; got != 2 {
				t.Fatalf("later member update did not settle: watermark=%d", got)
			}
		})
	}
}

func TestRuntimePurgeUsesInteractiveSenderWithoutRetry(t *testing.T) {
	t.Parallel()
	const id = -100835
	var calls atomic.Int32
	transport := runtimeTransport(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if err := errors.Join(r.ParseMultipartForm(1<<20), r.Body.Close()); err != nil {
			return nil, err
		}
		if err := r.MultipartForm.RemoveAll(); err != nil {
			return nil, err
		}
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/leaveChat") || r.FormValue("chat_id") != "-100835" {
			t.Errorf("unexpected SDK request: %s %s chat_id=%s", r.Method, r.URL.Path, r.FormValue("chat_id"))
		}
		return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(strings.NewReader(runtimeRetryLeaveEnvelope)), Header: make(http.Header)}, nil
	})
	upstream, err := bot.New("12345:test", bot.WithSkipGetMe(), bot.WithServerURL("https://telegram.test"),
		bot.WithHTTPClient(0, &http.Client{Transport: transport}))
	if err != nil {
		t.Fatal(err)
	}
	s, err := sender.New(sender.Deps{
		Bot: upstream, Workers: 1, CapInteractive: 1, CapBackground: 1, GlobalRPS: 25, GlobalBurst: 5,
		ClassWeights: [4]int{8, 4, 2, 1}, ChatQueue: 16, MaxRetries: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	requests, completed := make(chan sender.Request, 1), make(chan sender.Result, 1)
	h := newRuntimeHarness(t, runtimeOptions{
		beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) },
		configure: func(d *Deps) {
			d.Now = time.Now
			d.Sender = purgeSenderFunc(func(request sender.Request) error {
				requests <- request
				done := request.Done
				request.Done = func(result sender.Result) {
					completed <- result
					done(result)
				}
				return s.Enqueue(request)
			})
		},
	})
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { runtimeStop(t, s.Stop) })
	before := h.snapshot(id)
	ctx, cancel := context.WithTimeout(t.Context(), runtimeWait)
	defer cancel()
	err = h.chats.Purge(ctx, id)
	assertPurgeTelegramError(t, err, telegram.Outcome{Class: telegram.Retry})
	request := runtimeReceive(t, requests)
	if request.Class != sender.Interactive || !request.NoRetry || request.Deadline.IsZero() {
		t.Fatalf("leaveChat class=%v NoRetry=%t deadline=%v", request.Class, request.NoRetry, request.Deadline)
	}
	if got := runtimeReceive(t, completed); got.Attempts != 1 || got.Dropped != 0 || calls.Load() != 1 {
		t.Fatalf("leaveChat made %d HTTP calls, result=%+v", calls.Load(), got)
	}
	assertRuntimePurgeState(h, id, before, true)
}

func TestRuntimePurgeReplaysAbortedWrite(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		leftFirst bool
		cancel    bool
	}{
		{name: "left before verdict", leftFirst: true},
		{name: "left after verdict"},
		{name: "caller canceled during backoff", cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const id = -100840
			writeDone := make(chan error, 1)
			var watchWrite atomic.Bool
			h := newRuntimeHarness(t, runtimeOptions{
				tickets: 2, beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) },
				configure: func(d *Deps) {
					writer := d.Store
					d.Store = maintenanceStore{Store: writer, write: func(op store.Op, done func(error)) error {
						if !watchWrite.Swap(false) {
							return writer.Write(op, done)
						}
						return writer.Write(op, func(err error) {
							done(err)
							writeDone <- err
						})
					}}
				},
			})
			h.exec(`CREATE TABLE purge_targets (id INTEGER PRIMARY KEY)`)
			h.exec(`CREATE TABLE purge_audit (target_id INTEGER REFERENCES purge_targets(id) DEFERRABLE INITIALLY DEFERRED)`)
			h.exec(`CREATE TRIGGER audit_purge AFTER UPDATE ON chats
				WHEN NEW.state = 'removed' AND NEW.purge_after <= NEW.removed_at + 3600
				BEGIN INSERT INTO purge_audit VALUES (1); END`)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			leave, result := startRuntimePurge(h, ctx, id)
			left := runtimeUpdate(id, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false)
			if tc.leftFirst {
				h.apply(left)
			}
			before := h.snapshot(id)
			h.advance(time.Minute)
			watchWrite.Store(true)
			leave.Done(purgeLeaveResult(telegram.OK))
			if err := runtimeReceive(t, writeDone); !errors.Is(err, store.ErrAborted) {
				t.Fatalf("first Purge write returned %v, want a batch abort", err)
			}
			h.barrier(id)
			h.assertCounter("yabot_store_batch_aborts_total", 1)
			h.assertCounter(`yabot_chats_write_failures_total{outcome="retry"}`, 1)
			assertRuntimePurgeState(h, id, before, true)
			h.assertProbeTicket(0)
			var receipt dispatch.Receipt
			if !tc.leftFirst {
				receipt = h.update(left)
				h.barrier(id)
				assertRuntimePurgeState(h, id, before, true)
			}
			if tc.cancel {
				cancel()
				if err := runtimeReceive(t, result); !errors.Is(err, context.Canceled) {
					t.Fatalf("Purge returned %v, want context.Canceled", err)
				}
			}
			h.exec(`INSERT INTO purge_targets VALUES (1)`)
			h.advance(time.Second)
			h.tick()
			h.settleEffects(id)
			h.assertNoRequest()
			if !tc.cancel {
				if err := runtimeReceive(t, result); err != nil {
					t.Fatalf("Purge returned %v after transient write failure", err)
				}
			}
			want := before
			want.PurgeAfter = h.now().Unix()
			if !tc.leftFirst {
				if err := receipt.Wait(runtimeWait); err != nil {
					t.Fatalf("left queued during Purge backoff: %v", err)
				}
				want.State, want.BotRights, want.RemovedReason = Removed, 0, RemovedByPurge
				want.RemovedAt, want.UpdatedAt = h.now().Unix(), h.now().Unix()
				want.Watermark = dispatch.Order{Date: int64(left.MyChatMember.Date), ID: left.ID}
			}
			assertRuntimePurgeState(h, id, want, true)
			h.assertCounter("yabot_store_batch_aborts_total", 1)
			h.assertProbeTicket(0)
		})
	}
}

func TestRuntimePurgeReplaysSynchronousWriteRejection(t *testing.T) {
	t.Parallel()
	const id = -100841
	var reject atomic.Bool
	reject.Store(true)
	h := newRuntimeHarness(t, runtimeOptions{
		tickets: 1, beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) },
		configure: func(d *Deps) {
			writer := d.Store
			d.Store = maintenanceStore{Store: writer, write: func(op store.Op, done func(error)) error {
				if reject.Swap(false) {
					return store.ErrBusy
				}
				return writer.Write(op, done)
			}}
		},
	})
	before := h.snapshot(id)
	leave, result := startRuntimePurge(h, t.Context(), id)
	leave.Done(purgeLeaveResult(telegram.OK))
	h.barrier(id)
	h.assertCounter(`yabot_chats_write_failures_total{outcome="retry"}`, 1)
	assertRuntimePurgeState(h, id, before, true)
	h.assertProbeTicket(0)
	h.advance(time.Second)
	h.tick()
	h.settleEffects(id)
	h.assertNoRequest()
	if err := runtimeReceive(t, result); err != nil {
		t.Fatalf("Purge returned %v after transient queue rejection", err)
	}
	if got := h.snapshot(id); got.State != Removed || got.PurgeAfter != h.now().Unix() {
		t.Fatalf("Purge replay published %+v", got)
	}
	h.assertProbeTicket(0)
}

func TestRuntimePurgeAbandonsRepeatedOperationErrors(t *testing.T) {
	t.Parallel()
	const id = -100836
	const raw = "private SQLite failure details"
	h := newRuntimeHarness(t, runtimeOptions{
		tickets: 1,
		beforeStart: func(h *runtimeHarness) {
			h.seedActive(id, 1)
			h.exec(`CREATE TRIGGER reject_purge BEFORE UPDATE ON chats WHEN NEW.removed_reason = 'purge'
				BEGIN SELECT raise(ABORT, 'private SQLite failure details'); END`)
		},
	})
	before := h.snapshot(id)
	leave, result := startRuntimePurge(h, t.Context(), id)
	leave.Done(purgeLeaveResult(telegram.OK))
	h.settleEffects(id)
	h.assertCounter(`yabot_chats_write_failures_total{outcome="op_error"}`, 1)
	for attempt := 1; attempt <= 3; attempt++ {
		assertRuntimePurgeState(h, id, before, true)
		h.assertProbeTicket(0)
		h.advance(5 * time.Second)
		h.tick()
		h.settleEffects(id)
		h.assertNoRequest()
		h.assertCounter(`yabot_chats_write_failures_total{outcome="op_error"}`, uint64(attempt+1))
	}
	err := runtimeReceive(t, result)
	var opErr *store.OpError
	if !errors.As(err, &opErr) || strings.Contains(err.Error(), raw) || strings.Contains(opErr.Unwrap().Error(), raw) {
		t.Fatalf("Purge returned %v, want a redacted store operation error", err)
	}
	h.assertCounter("yabot_chats_write_abandoned_total", 1)
	h.assertLog(slog.LevelError, "chat write failed", "chat_id", id)
	h.assertLog(slog.LevelError, "chat event abandoned after write failures", "chat_id", id)
	h.logs.mu.Lock()
	loggedRaw := strings.Contains(h.logs.records.String(), raw)
	h.logs.mu.Unlock()
	if loggedRaw {
		t.Fatal("Purge logged the original SQLite error")
	}
	assertRuntimePurgeState(h, id, before, true)
	h.assertProbeTicket(0)
	h.apply(runtimeUpdate(id, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, true))
	if got := h.water(id).ID; got != 2 {
		t.Fatalf("later member update did not settle: watermark=%d", got)
	}
}

func TestRuntimePurgeSettlesSynchronousSenderRejection(t *testing.T) {
	t.Parallel()
	const id = -100838
	h := newRuntimeHarness(t, runtimeOptions{
		tickets: 1,
		beforeStart: func(h *runtimeHarness) {
			h.seedActive(id, 1)
			h.sender.reject = "leaveChat"
		},
	})
	before := h.snapshot(id)
	ctx, cancel := context.WithTimeout(t.Context(), runtimeWait)
	defer cancel()
	err := h.chats.Purge(ctx, id)
	assertPurgeTelegramError(t, err, telegram.Outcome{Class: telegram.Retry})
	h.assertCounter(`yabot_chats_effects_total{effect="leave_outcome",outcome="busy"}`, 1)
	assertRuntimePurgeState(h, id, before, true)
	h.assertProbeTicket(0)
}

func TestRuntimePurgeExpeditesRemovedChatWhenSenderRejects(t *testing.T) {
	t.Parallel()
	const id = -100842
	delayed := &runtimeDelayedStoreDone{callbacks: make(chan func(), 1)}
	entryRan := make(chan struct{}, 1)
	attempted := make(chan struct{}, 1)
	var watchEntry atomic.Bool
	h := newRuntimeHarness(t, runtimeOptions{
		tickets:     1,
		beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) },
		configure: func(d *Deps) {
			delayed.Store = d.Store
			d.Store = delayed
			actual := d.Dispatcher
			d.Dispatcher = maintenanceDispatcher{Dispatcher: actual, submit: func(k dispatch.Key, task dispatch.Task) error {
				if !watchEntry.Load() {
					return actual.Submit(k, task)
				}
				return actual.Submit(k, func(ctx context.Context, sh *dispatch.Shard) {
					task(ctx, sh)
					entryRan <- struct{}{}
				})
			}}
			d.Sender = purgeSenderFunc(func(sender.Request) error {
				attempted <- struct{}{}
				return sender.ErrBusy
			})
		},
	})
	h.exec(`CREATE TRIGGER protect_removed_membership BEFORE UPDATE OF removed_at, removed_reason, owner_id ON chats
		WHEN OLD.state = 'removed' BEGIN SELECT raise(ABORT, 'unexpected change to the removed row'); END`)
	before := h.snapshot(id)
	leftAt := h.now().Unix()
	left := h.update(runtimeUpdate(id, 2, leftAt, models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
	publish := runtimeReceive(t, delayed.callbacks)
	defer publish()
	h.advance(time.Minute)
	watchEntry.Store(true)
	result := make(chan error, 1)
	go func() { result <- h.chats.Purge(t.Context(), id) }()
	runtimeReceive(t, entryRan)
	watchEntry.Store(false)
	select {
	case <-attempted:
		t.Fatal("Purge called sender while the left update was still pending")
	default:
	}
	delayed.open.Store(true)
	publish()
	if err := left.Wait(runtimeWait); err != nil {
		t.Fatal(err)
	}
	runtimeReceive(t, attempted)
	if err := runtimeReceive(t, result); err != nil {
		t.Fatalf("Purge returned %v after the sender rejected an already removed chat", err)
	}
	want := before
	want.State, want.BotRights, want.RemovedReason = Removed, 0, RemovedLeft
	want.RemovedAt, want.UpdatedAt, want.PurgeAfter = leftAt, leftAt, h.now().Unix()
	want.Watermark = dispatch.Order{Date: leftAt, ID: 2}
	assertRuntimePurgeState(h, id, want, true)
	h.assertCounter(`yabot_chats_effects_total{effect="leave_outcome",outcome="busy"}`, 1)
	h.assertProbeTicket(0)
}

func TestRuntimePurgeWaitsForCommitPublication(t *testing.T) {
	t.Parallel()
	const id = -100839
	delayed := &runtimeDelayedStoreDone{callbacks: make(chan func(), 1)}
	h := newRuntimeHarness(t, runtimeOptions{
		beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) },
		configure: func(d *Deps) {
			delayed.Store = d.Store
			d.Store = delayed
		},
	})
	before := h.snapshot(id)
	leave, result := startRuntimePurge(h, t.Context(), id)
	leave.Done(purgeLeaveResult(telegram.OK))
	publish := runtimeReceive(t, delayed.callbacks)
	defer publish()
	h.barrier(id)
	select {
	case err := <-result:
		t.Fatalf("Purge returned before the commit callback: %v", err)
	default:
	}
	if got := h.snapshot(id); got != before {
		t.Fatalf("snapshot changed before the commit callback: %+v", got)
	}
	publish()
	if err := runtimeReceive(t, result); err != nil {
		t.Fatal(err)
	}
	want := before
	want.State, want.BotRights, want.RemovedReason = Removed, 0, RemovedByPurge
	want.RemovedAt, want.UpdatedAt, want.PurgeAfter = h.now().Unix(), h.now().Unix(), h.now().Unix()
	assertRuntimePurgeState(h, id, want, true)
}

func TestRuntimePurgeCoexistsWithIdleLeave(t *testing.T) {
	t.Parallel()
	const id = -100837
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
		seedIdleChat(h, id, h.now().Add(-8*24*time.Hour).Unix(), false)
	}})
	if n, err := h.janitor["chats_idle"].run(t.Context()); n != 1 || err != nil {
		t.Fatalf("idle scan accepted %d chats with error %v", n, err)
	}
	finishDeliveryProbe(h, id, models.ChatTypeSupergroup, models.ChatMemberTypeMember, false)
	idle := h.nextRequest("leaveChat")
	leave, result := startRuntimePurge(h, t.Context(), id)
	leave.Done(purgeLeaveResult(telegram.OK))
	if err := runtimeReceive(t, result); err != nil {
		t.Fatal(err)
	}
	want := h.snapshot(id)
	if want.State != Removed || want.RemovedReason != RemovedByPurge || want.PurgeAfter != h.now().Unix() {
		t.Fatalf("snapshot after Purge = %+v, want immediate removal", want)
	}
	h.advance(time.Minute)
	idle.Done(purgeLeaveResult(telegram.Retry))
	h.settleEffects(id)
	want.PurgeAfter = h.now().Unix()
	assertRuntimePurgeState(h, id, want, true)
	h.assertProbeTicket(0)
}

type purgeSenderFunc func(sender.Request) error

func (f purgeSenderFunc) Enqueue(request sender.Request) error { return f(request) }

func startRuntimePurge(h *runtimeHarness, ctx context.Context, id int64) (sender.Request, <-chan error) {
	h.t.Helper()
	result := make(chan error, 1)
	go func() { result <- h.chats.Purge(ctx, id) }()
	leave := h.nextRequest("leaveChat")
	if leave.ChatID != id || leave.Class != sender.Interactive || !leave.NoRetry || !leave.Deadline.Equal(h.now().Add(5*time.Second)) {
		h.t.Fatalf("unexpected Purge leaveChat: chat_id=%d class=%v NoRetry=%t deadline=%v", leave.ChatID, leave.Class, leave.NoRetry, leave.Deadline)
	}
	return leave, result
}

func purgeLeaveResult(class telegram.Class) sender.Result {
	return sender.Result{Outcome: telegram.Outcome{Class: class}}
}

func purgeMemberUpdate(h *runtimeHarness, id, order int64, old, next models.ChatMemberType, invite bool) *models.Update {
	update := runtimeUpdate(id, order, h.now().Unix(), old, next, invite)
	update.MyChatMember.Chat.Type = models.ChatTypeChannel
	return update
}

func assertPurgeTelegramError(t *testing.T, err error, want telegram.Outcome) {
	t.Helper()
	var typed *telegram.Error
	if !errors.As(err, &typed) || typed.Outcome != want {
		t.Fatalf("Purge returned %v (%+v), want Telegram outcome %+v", err, typed, want)
	}
}

func assertRuntimePurgeState(h *runtimeHarness, id int64, want ChatSnapshot, exists bool) {
	h.t.Helper()
	if snap, ok := h.chats.State(id); snap != want || ok != exists {
		h.t.Fatalf("snapshot for %d = %+v (%t), want %+v (%t)", id, snap, ok, want, exists)
	}
	var row Record
	var found bool
	err := readRecords(h.t.Context(), h.store.Read(), func(r Record) error {
		if r.ChatID == id {
			row, found = r, true
		}
		return nil
	})
	if err != nil {
		h.t.Fatal(err)
	}
	wantRow := Record{
		ChatID: want.ChatID, Type: want.Type, Title: want.Title, State: want.State, BotRights: want.BotRights,
		GuardAssigned: want.GuardAssigned, OwnerID: want.OwnerID, AddedAt: want.AddedAt, UpdatedAt: want.UpdatedAt,
		EverActive: want.EverActive, RemovedAt: want.RemovedAt, RemovedReason: want.RemovedReason,
		MigratedTo: want.MigratedTo, PurgeAfter: want.PurgeAfter, Watermark: want.Watermark,
	}
	if row != wantRow || found != exists {
		h.t.Fatalf("stored chat %d = %+v (%t), want %+v (%t)", id, row, found, wantRow, exists)
	}
}
