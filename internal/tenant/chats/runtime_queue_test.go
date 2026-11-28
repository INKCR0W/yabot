package chats

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

func TestRuntimeSetOwnerReportsSubmissionErrors(t *testing.T) {
	t.Parallel()
	const id = -615
	var submissionError error
	var submissions int
	h := newRuntimeHarness(t, runtimeOptions{configure: func(d *Deps) {
		actual := d.Dispatcher
		d.Dispatcher = preloadDispatcher{Dispatcher: actual, submit: func(key dispatch.Key, task dispatch.Task) error {
			submissions++
			if submissionError != nil {
				return submissionError
			}
			return actual.Submit(key, task)
		}}
	}})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := h.setOwner(ctx, id, 1); !errors.Is(err, context.Canceled) || submissions != 0 {
		t.Fatalf("canceled claim submitted %d times and returned %v", submissions, err)
	}
	for _, tc := range []struct{ submit, want error }{
		{dispatch.ErrBusy, ErrBusy},
		{dispatch.ErrClosed, dispatch.ErrClosed},
		{nil, ErrForbidden},
	} {
		submissionError = tc.submit
		if err := h.setOwner(t.Context(), id, 1); !errors.Is(err, tc.want) {
			t.Fatalf("claim with submission error %v returned %v, want %v", tc.submit, err, tc.want)
		}
	}
	h.assertCounter(`yabot_chats_overload_total{source="owner_inbox"}`, 1)
	h.assertNoLog(slog.LevelInfo, "chat owner claim rejected")
}

func TestRuntimeSetOwnerReportsStoreBackpressureAndRemainingErrors(t *testing.T) {
	t.Parallel()
	const id = -616
	h := newRuntimeHarness(t, runtimeOptions{
		tickets:     1,
		beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) },
		configure: func(d *Deps) {
			d.Store = &rejectFirstChatWrite{Store: d.Store, reject: store.ErrBusy}
		},
	})
	if err := h.setOwner(t.Context(), id, 95); !errors.Is(err, ErrBusy) {
		t.Fatalf("claim during write backpressure = %v, want ErrBusy", err)
	}
	h.barrier(id)
	h.assertProbeTicket(0)
	h.assertCounter(`yabot_chats_write_failures_total{outcome="retry"}`, 1)
	h.assertLog(slog.LevelInfo, "chat write will be retried or rejected", "chat_id", id)
	if got := h.snapshot(id).OwnerID; got != 81 {
		t.Fatalf("failed claim published owner %d", got)
	}
	h.exec(`CREATE TRIGGER test_claim_failure BEFORE UPDATE OF owner_id ON chats WHEN NEW.owner_id = 95
		BEGIN SELECT raise(ABORT, 'claim operation failure'); END`)
	if err := h.setOwner(t.Context(), id, 95); dispatch.IsPermanent(err) {
		t.Fatalf("claim used a routing error wrapper: %v", err)
	} else if _, ok := errors.AsType[*store.OpError](err); !ok {
		t.Fatalf("claim error = %v, want a store OpError", err)
	}
	h.assertCounter(`yabot_chats_write_failures_total{outcome="op_error"}`, 1)
	h.assertLog(slog.LevelError, "chat write failed", "chat_id", id)
	h.exec("DROP TRIGGER test_claim_failure")
	if err := h.setOwner(t.Context(), id, 95); err != nil || h.snapshot(id).OwnerID != 95 {
		t.Fatalf("claim after backpressure and rejected write = %v", err)
	}
}

func TestRuntimeFullMailboxRejectsExternalUpdatesAndKeepsTheirWatermarkRetryable(t *testing.T) {
	t.Parallel()
	const id = -617
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
	release, held := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	done := make(chan error, 1)
	if err := h.store.Write(func(*sql.Tx) error {
		close(held)
		<-release
		return nil
	}, func(err error) { done <- err }); err != nil {
		t.Fatal(err)
	}
	runtimeReceive(t, held)
	receipts := make([]dispatch.Receipt, 0, 17)
	update := func(n int64) *models.Update {
		return runtimeUpdate(id, n, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, true)
	}
	for n := int64(2); n <= 18; n++ {
		receipts = append(receipts, h.update(update(n)))
	}
	rejected := update(19)
	if err := h.update(rejected).Wait(runtimeWait); !errors.Is(err, ErrBusy) || dispatch.IsPermanent(err) {
		t.Fatalf("receipt from full mailbox = %v, want retryable ErrBusy", err)
	}
	if err := h.setOwner(t.Context(), id, 19); !errors.Is(err, ErrBusy) {
		t.Fatalf("claim from full mailbox = %v, want ErrBusy", err)
	}
	h.assertCounter(`yabot_chats_overload_total{source="mailbox"}`, 2)
	if got := h.water(id).ID; got != 1 {
		t.Fatalf("held batch advanced SQLite watermark to %d", got)
	}
	unblock()
	if err := runtimeReceive(t, done); err != nil {
		t.Fatal(err)
	}
	for _, receipt := range receipts {
		if err := receipt.Wait(runtimeWait); err != nil {
			t.Fatal(err)
		}
	}
	h.apply(rejected)
	if got := h.water(id).ID; got != 19 {
		t.Fatalf("replayed update SQLite watermark = %d, want 19", got)
	}
}

func TestRuntimeDeferredMembershipAckSettlesOnDrop(t *testing.T) {
	t.Parallel()
	const id = -618
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
	if err := h.dispatcher.Submit(keyOf(id), func(_ context.Context, sh *dispatch.Shard) { sh.Hold(keyOf(id)) }); err != nil {
		t.Fatal(err)
	}
	dropped := h.update(runtimeUpdate(id, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
	if err := h.dispatcher.Submit(keyOf(id), func(_ context.Context, sh *dispatch.Shard) {
		sh.Drop(keyOf(id), errors.New("chat deleted"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := dropped.Wait(runtimeWait); err != nil {
		t.Fatalf("deferred membership receipt on drop = %v", err)
	}
	if got := h.water(id).ID; got != 1 {
		t.Fatalf("dropped update advanced SQLite watermark to %d", got)
	}
	h.apply(runtimeUpdate(id, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
	if got := h.water(id).ID; got != 2 {
		t.Fatalf("dropped hold blocked a new update: watermark %d", got)
	}
}

func TestRuntimeRecordlessEffectReplaysAfterTicketReturn(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, method, effect string
		accept               bool
		trigger              func(*runtimeHarness, int64)
	}{
		{"unknown chat", "getChat", "chats.StartProbe", true, func(h *runtimeHarness, id int64) {
			h.chats.ReportUnknown(id)
			h.tick()
		}},
		{"rejected chat", "leaveChat", "chats.LeaveChat", false, func(h *runtimeHarness, id int64) {
			h.apply(runtimeUpdate(id, 1, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const id = -619
			h := newRuntimeHarness(t, runtimeOptions{tickets: 1, configure: func(d *Deps) { d.AcceptNewChats = tc.accept }})
			returnTicket := holdOnlyRuntimeTicket(t, h, id)
			tc.trigger(h, id)
			h.barrier(id)
			h.assertNoRequest()
			h.assertCounter(`yabot_chats_overload_total{source="sender_ticket"}`, 1)
			h.assertLog(slog.LevelInfo, "chat effect waiting for sender admission", "chat_id", id, "effect", tc.effect)

			h.advance(memoryTTL)
			h.tick()
			h.barrier(id)
			h.assertNoRequest()
			h.assertCounter(`yabot_chats_overload_total{source="sender_ticket"}`, 2)
			h.assertCounter(`yabot_chats_memory_evictions_total{reason="ttl"}`, 0)

			returnTicket()
			h.advance(time.Second)
			h.tick()
			req := h.nextRequest(tc.method)
			if req.ChatID != id {
				t.Fatalf("replayed request for chat %d, want %d", req.ChatID, id)
			}
			req.Done(sender.DroppedResult(sender.Shutdown, sender.ErrClosed))
			h.settleEffects(id)
			h.assertProbeTicket(0)

			h.advance(memoryTTL)
			h.tick()
			h.barrier(id)
			h.assertCounter(`yabot_chats_memory_evictions_total{reason="ttl"}`, 1)
			h.assertNoRequest()
		})
	}
}

func TestRuntimeRejectedLeaveReplaysAfterSenderQueueRecovers(t *testing.T) {
	t.Parallel()
	const id int64 = -632
	h := newRuntimeHarness(t, runtimeOptions{tickets: 1, memoryCapacity: 1, configure: func(d *Deps) { d.AcceptNewChats = false }})
	h.sender.mu.Lock()
	h.sender.reject = "leaveChat"
	h.sender.mu.Unlock()

	update := runtimeUpdate(id, 1, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false)
	h.apply(update)
	h.barrier(id)
	h.assertNoRequest()
	h.assertProbeTicket(0)
	h.assertCounter(`yabot_chats_overload_total{source="sender_queue"}`, 1)
	h.assertCounter(`yabot_chats_effects_total{effect="leave_outcome",outcome="busy"}`, 0)

	other := runtimeUpdate(id-1, 2, h.now().Unix(), models.ChatMemberTypeMember, models.ChatMemberTypeLeft, false)
	if err := h.update(other).Wait(runtimeWait); !errors.Is(err, ErrBusy) {
		t.Fatalf("recordless memory pressure while leaveChat is pending: %v, want ErrBusy", err)
	}
	h.sender.mu.Lock()
	h.sender.reject = ""
	h.sender.mu.Unlock()
	h.advance(time.Second)
	h.tick()
	req := h.nextRequest("leaveChat")
	if req.ChatID != id || req.Class != sender.Background || req.NoRetry {
		t.Fatalf("unexpected replayed leaveChat request: %+v", req)
	}
	h.finishRequest(req, `true`)
	h.settleEffects(id)
	h.apply(update)
	h.barrier(id)
	h.assertNoRequest()
	h.assertProbeTicket(0)
	if _, exists := h.chats.State(id); exists {
		t.Fatal("rejected chat was added to the snapshot")
	}
}

func TestRuntimeDroppingRecordlessReplayReleasesMemory(t *testing.T) {
	t.Parallel()
	const id = -630
	h := newRuntimeHarness(t, runtimeOptions{tickets: 1, configure: func(d *Deps) { d.AcceptNewChats = false }})
	returnTicket := holdOnlyRuntimeTicket(t, h, id)
	h.apply(runtimeUpdate(id, 1, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false))
	h.advance(memoryTTL)
	if err := h.dispatcher.Submit(keyOf(id), func(_ context.Context, sh *dispatch.Shard) {
		sh.Drop(keyOf(id), errors.New("chat dropped"))
	}); err != nil {
		t.Fatal(err)
	}
	h.barrier(id)
	returnTicket()
	h.tick()
	h.barrier(id)
	h.assertCounter(`yabot_chats_memory_evictions_total{reason="ttl"}`, 1)
	h.assertNoRequest()
}

func holdOnlyRuntimeTicket(t *testing.T, h *runtimeHarness, id int64) func() {
	t.Helper()
	admitted := make(chan dispatch.Ticket, 1)
	if err := h.dispatcher.Submit(keyOf(id), func(_ context.Context, sh *dispatch.Shard) {
		ticket, _ := sh.Admit()
		admitted <- ticket
	}); err != nil {
		t.Fatal(err)
	}
	cancel := sync.OnceFunc(runtimeReceive(t, admitted).Cancel)
	t.Cleanup(cancel)
	return cancel
}

func TestRuntimeRecordlessReplayCapacityRejectsNewChats(t *testing.T) {
	t.Parallel()
	const capacity, first = 64, -900000
	const idle, overflow = first + capacity, first + capacity + 1
	h := newRuntimeHarness(t, runtimeOptions{tickets: 1, memoryCapacity: capacity, configure: func(d *Deps) {
		d.AcceptNewChats = false
	}})
	returnTicket := holdOnlyRuntimeTicket(t, h, first)
	joined := runtimeUpdate(first, 5, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false)
	h.apply(joined)
	if err := h.dispatcher.Submit(keyOf(first), func(_ context.Context, sh *dispatch.Shard) {
		sh.Release(keyOf(first))
	}); err != nil {
		t.Fatal(err)
	}
	h.apply(runtimeUpdate(idle, 6, h.now().Unix(), models.ChatMemberTypeMember, models.ChatMemberTypeLeft, false))
	if err := h.dispatcher.Submit(keyOf(first), func(ctx context.Context, sh *dispatch.Shard) {
		for id := int64(first + 1); id < idle-1; id++ {
			h.chats.runEvent(ctx, sh, keyOf(id), &eventWork{event: MemberUpdate{
				Order: dispatch.Order{Date: h.now().Unix(), ID: 5}, Type: models.ChatTypeGroup,
				Old: Membership{Status: models.ChatMemberTypeLeft}, New: Membership{Status: models.ChatMemberTypeMember},
			}})
		}
		// An item released to the ready queue must survive admission before it runs.
		sh.Release(keyOf(first))
		h.chats.runEvent(ctx, sh, keyOf(idle-1), &eventWork{event: MemberUpdate{
			Order: dispatch.Order{Date: h.now().Unix(), ID: 5}, Type: models.ChatTypeGroup,
			Old: Membership{Status: models.ChatMemberTypeLeft}, New: Membership{Status: models.ChatMemberTypeMember},
		}})
	}); err != nil {
		t.Fatal(err)
	}
	h.barrier(first)
	h.assertLog(slog.LevelWarn, "recordless chat memory is full", "chat_id", int64(idle))
	h.assertCounter(`yabot_chats_memory_evictions_total{reason="capacity"}`, 1)

	overflowUpdate := runtimeUpdate(overflow, 5, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false)
	if err := h.update(overflowUpdate).Wait(runtimeWait); !errors.Is(err, ErrBusy) || dispatch.IsPermanent(err) {
		t.Fatalf("chat rejection without a memory slot settled with %v, want retryable ErrBusy", err)
	}
	h.chats.ReportUnknown(overflow + 1)
	h.tick()
	h.barrier(overflow + 1)
	h.assertNoRequest()
	h.assertCounter(`yabot_chats_overload_total{source="recordless_memory"}`, 2)
	h.assertLog(slog.LevelWarn, "recordless chat memory is full of pinned entries", "chat_id", int64(overflow))
	h.assertLog(slog.LevelWarn, "recordless chat memory is full of pinned entries", "chat_id", int64(overflow+1))

	counts := make(chan [2]int, 1)
	if err := h.dispatcher.Submit(keyOf(first), func(_ context.Context, sh *dispatch.Shard) {
		var pending int
		for id := int64(first); id <= overflow+1; id++ {
			if sh.Busy(keyOf(id)) {
				pending++
			}
		}
		counts <- [2]int{len(h.chats.local(sh).memory), pending}
		h.chats.policy.AcceptNewChats = true
	}); err != nil {
		t.Fatal(err)
	}
	if got := runtimeReceive(t, counts); got != [2]int{capacity, capacity} {
		t.Fatalf("recordless memory entries and pending replays = %v, want %d each", got, capacity)
	}
	creation := h.update(overflowUpdate)
	if err := creation.Wait(runtimeWait); !errors.Is(err, ErrBusy) || dispatch.IsPermanent(err) {
		t.Fatalf("record creation without a memory slot settled with %v, want retryable ErrBusy", err)
	}
	h.assertCounter(`yabot_chats_overload_total{source="recordless_memory"}`, 3)

	h.advance(memoryTTL)
	h.tick()
	h.barrier(first)
	h.assertCounter(`yabot_chats_memory_evictions_total{reason="ttl"}`, 0)
	h.assertNoRequest()

	returnTicket()
	h.advance(time.Second)
	h.tick()
	resumed := h.nextRequest("leaveChat")
	h.apply(runtimeUpdate(overflow, 7, h.now().Unix(), models.ChatMemberTypeMember, models.ChatMemberTypeLeft, false))
	h.assertCounter(`yabot_chats_memory_evictions_total{reason="ttl"}`, 1)
	h.apply(overflowUpdate)
	h.assertCounter(`yabot_dispatch_stale_updates_total{route="my_chat_member"}`, 1)
	resumed.Done(sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}})
	h.settleEffects(first)
	h.assertProbeTicket(0)
}

func TestRuntimeRecordlessWriteHoldPinsMemoryUntilFailure(t *testing.T) {
	t.Parallel()
	const id = -631
	h := newRuntimeHarness(t, runtimeOptions{})
	h.exec(`CREATE TRIGGER test_reject_create BEFORE INSERT ON chats BEGIN SELECT raise(ABORT, 'write rejected'); END`)
	unblock := blockMaintenanceWriter(t, h)
	rc := h.update(runtimeUpdate(id, 6, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false))
	h.barrier(id)
	h.advance(memoryTTL)
	h.tick()
	h.barrier(id)
	h.assertCounter(`yabot_chats_memory_evictions_total{reason="ttl"}`, 0)
	unblock()
	if err := rc.Wait(runtimeWait); !dispatch.IsPermanent(err) {
		t.Fatalf("rejected write settled with %v, want permanent error", err)
	}
	h.advance(time.Second)
	h.tick()
	h.barrier(id)
	h.assertCounter(`yabot_chats_memory_evictions_total{reason="ttl"}`, 1)
	h.assertNoRequest()
}

func TestRuntimeRecordlessWriteReplayPreservesWatermark(t *testing.T) {
	t.Parallel()
	const id = -632
	h := newRuntimeHarness(t, runtimeOptions{tickets: 1})
	returnTicket := holdOnlyRuntimeTicket(t, h, id)
	h.apply(runtimeUpdate(id, 5, h.now().Unix(), models.ChatMemberTypeMember, models.ChatMemberTypeLeft, false))
	h.chats.ReportMigrated(id, id-1)
	h.tick()
	h.barrier(id)
	h.assertCounter(`yabot_chats_write_failures_total{outcome="retry"}`, 1)

	h.advance(memoryTTL)
	returnTicket()
	h.tick()
	h.settleEffects(id)
	if got := h.snapshot(id); got.State != Removed || got.MigratedTo != id-1 || got.Watermark.ID != 5 {
		t.Fatalf("replayed migration did not retain its previous watermark: %+v", got)
	}
	h.assertCounter(`yabot_chats_memory_evictions_total{reason="ttl"}`, 0)
}

func TestRuntimeCanceledQueuedOwnerClaimDoesNotWrite(t *testing.T) {
	t.Parallel()
	const id, claimant = -620, 95
	submitted := make(chan struct{})
	var observe atomic.Bool
	h := newRuntimeHarness(t, runtimeOptions{
		beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) },
		configure: func(d *Deps) {
			actual := d.Dispatcher
			d.Dispatcher = preloadDispatcher{Dispatcher: actual, submit: func(key dispatch.Key, task dispatch.Task) error {
				err := actual.Submit(key, task)
				if observe.CompareAndSwap(true, false) {
					close(submitted)
				}
				return err
			}}
		},
	})
	before := h.snapshot(id)
	unblock := blockMaintenanceWriter(t, h)
	update := runtimeUpdate(id, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, true)
	previous := h.update(update)
	h.barrier(id)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	observe.Store(true)
	result := make(chan error, 1)
	go func() { result <- h.setOwner(ctx, id, claimant) }()
	runtimeReceive(t, submitted)
	h.barrier(id)
	cancel()
	if err := runtimeReceive(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled queued claim returned %v", err)
	}
	unblock()
	if err := previous.Wait(runtimeWait); err != nil {
		t.Fatalf("preceding membership write returned %v", err)
	}
	h.settleEffects(id)
	want := before
	want.Watermark = dispatch.Order{Date: int64(update.MyChatMember.Date), ID: update.ID}
	if got := h.snapshot(id); got != want {
		t.Fatalf("snapshot after canceled claim = %+v, want %+v", got, want)
	}
	var storedOwner, storedWatermark int64
	if err := h.store.Read().QueryRowContext(t.Context(), `SELECT owner_id, member_event_update_id FROM chats WHERE chat_id = ?`, id).Scan(&storedOwner, &storedWatermark); err != nil {
		t.Fatal(err)
	}
	if storedOwner != want.OwnerID || storedWatermark != want.Watermark.ID {
		t.Fatalf("stored owner and watermark = %d, %d; want %d, %d", storedOwner, storedWatermark, want.OwnerID, want.Watermark.ID)
	}
}

func TestRuntimeSynchronousStorePanicReleasesHoldAndTicket(t *testing.T) {
	t.Parallel()
	const id = -621
	var attempts atomic.Int32
	h := newRuntimeHarness(t, runtimeOptions{
		tickets: 1,
		configure: func(d *Deps) {
			actual := d.Store
			d.Store = maintenanceStore{Store: actual, write: func(op store.Op, done func(error)) error {
				switch attempts.Add(1) {
				case 1:
					panic("store panicked before accepting operation")
				case 2:
					return store.ErrBusy
				default:
					return actual.Write(op, done)
				}
			}}
		},
	})
	update := runtimeUpdate(id, 1, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false)
	err := h.update(update).Wait(runtimeWait)
	panicErr, panicked := errors.AsType[*dispatch.PanicError](err)
	if !panicked || !dispatch.IsPermanent(err) {
		t.Fatalf("receipt after synchronous store panic = %v", err)
	}
	h.barrier(id)
	h.assertLog(slog.LevelError, "dispatch task panicked", "route", "my_chat_member", "stack", string(panicErr.Stack))
	h.assertProbeTicket(0)
	if err := h.update(update).Wait(runtimeWait); !errors.Is(err, store.ErrBusy) {
		t.Fatalf("receipt after released hold = %v, want write backpressure", err)
	}
	h.barrier(id)
	h.assertCounter(`yabot_dispatch_ticket_late_total{op="cancel"}`, 0)
	h.apply(update)
	if got := h.snapshot(id); got.State != Onboarding || got.Watermark.ID != update.ID || attempts.Load() != 3 {
		t.Fatalf("replayed membership did not commit after panic and backpressure: %+v, attempts %d", got, attempts.Load())
	}
}

func TestRuntimeDeferredMembershipPanicSettlesAndIsAttributedToTheRoute(t *testing.T) {
	t.Parallel()
	const id = -623
	h := newRuntimeHarness(t, runtimeOptions{configure: func(d *Deps) {
		d.Store = maintenanceStore{Store: d.Store, write: func(store.Op, func(error)) error {
			panic("store panicked on deferred member update")
		}}
	}})
	key := keyOf(id)
	if err := h.dispatcher.Submit(key, func(_ context.Context, sh *dispatch.Shard) { sh.Hold(key) }); err != nil {
		t.Fatal(err)
	}
	receipt := h.update(runtimeUpdate(id, 1, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false))
	if err := h.dispatcher.Submit(key, func(_ context.Context, sh *dispatch.Shard) { sh.Release(key) }); err != nil {
		t.Fatal(err)
	}
	if err := receipt.Wait(runtimeWait); !dispatch.IsPermanent(err) {
		t.Fatalf("deferred member panic receipt = %v", err)
	}
	h.barrier(id)
	h.assertCounter(`yabot_dispatch_panics_total{route="my_chat_member"}`, 1)
	h.assertLog(slog.LevelError, "dispatch task panicked", "route", "my_chat_member")
	h.assertProbeTicket(0)
}

func TestRuntimeImmediateOwnerClaimPanicSettlesBeforeDeadline(t *testing.T) {
	t.Parallel()
	const id = -624
	h := newRuntimeHarness(t, runtimeOptions{
		beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) },
		configure: func(d *Deps) {
			d.Store = maintenanceStore{Store: d.Store, write: func(store.Op, func(error)) error {
				panic("store panicked on owner update")
			}}
		},
	})
	ctx, cancel := context.WithTimeout(t.Context(), runtimeWait)
	defer cancel()
	if err := h.setOwner(ctx, id, 95); !dispatch.IsPermanent(err) {
		t.Fatalf("claim did not settle with a permanent panic: %v", err)
	}
	h.barrier(id)
	h.assertProbeTicket(0)
	if got := h.snapshot(id).OwnerID; got != 81 {
		t.Fatalf("failed claim published %d as owner", got)
	}
}

func TestRuntimeDroppingQueuedReportAllowsItToBePostedAgain(t *testing.T) {
	t.Parallel()
	const id = -622
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
	if err := h.dispatcher.Submit(keyOf(id), func(_ context.Context, sh *dispatch.Shard) { sh.Hold(keyOf(id)) }); err != nil {
		t.Fatal(err)
	}
	h.chats.ReportMigrated(id, id-1)
	h.tick()
	h.barrier(id)
	if err := h.dispatcher.Submit(keyOf(id), func(_ context.Context, sh *dispatch.Shard) {
		sh.Drop(keyOf(id), errors.New("chat dropped"))
	}); err != nil {
		t.Fatal(err)
	}
	h.barrier(id)
	h.chats.ReportMigrated(id, id-2)
	h.tick()
	h.settleEffects(id)
	got := h.snapshot(id)
	if got.State != Removed || got.MigratedTo != id-2 {
		t.Fatalf("report submitted after mailbox drop = %+v", got)
	}
}

func TestRuntimeReplayingReportDoesNotForgetTheNextQueuedReport(t *testing.T) {
	t.Parallel()
	const id = -625
	h := newRuntimeHarness(t, runtimeOptions{
		beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) },
		configure: func(d *Deps) {
			d.Store = &rejectFirstChatWrite{Store: d.Store, reject: store.ErrBusy}
		},
	})
	h.chats.ReportMigrated(id, id-1)
	h.tick()
	h.barrier(id)
	h.assertCounter(`yabot_chats_write_failures_total{outcome="retry"}`, 1)
	h.chats.ReportMigrated(id, id-2)
	h.tick()
	h.barrier(id)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	done := make(chan error, 1)
	if err := h.store.Write(func(*sql.Tx) error {
		close(entered)
		<-release
		return nil
	}, func(err error) { done <- err }); err != nil {
		t.Fatal(err)
	}
	runtimeReceive(t, entered)
	h.advance(time.Second)
	h.tick()
	h.barrier(id)
	h.chats.ReportMigrated(id, id-3)
	h.tick()
	h.barrier(id)
	unblock()
	if err := runtimeReceive(t, done); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		h.settleEffects(id)
	}
	if got := h.snapshot(id).MigratedTo; got != id-2 {
		t.Fatalf("migration target = %d, want queued report %d", got, id-2)
	}
}

func TestRuntimeCanceledShardWorkSettlesWithoutWriting(t *testing.T) {
	t.Parallel()
	const id = -626
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
	replied := make(chan error, 1)
	update, _ := MemberUpdateOf(runtimeUpdate(id, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
	if err := h.dispatcher.Submit(keyOf(id), func(ctx context.Context, sh *dispatch.Shard) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		h.chats.enqueueExternal(ctx, sh, keyOf(id), &eventWork{event: update, reply: func(err error) { replied <- err }})
	}); err != nil {
		t.Fatal(err)
	}
	if err := runtimeReceive(t, replied); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled shard work returned %v", err)
	}
	h.settleEffects(id)
	if got := h.snapshot(id); got.State != Active || h.water(id).ID != 1 {
		t.Fatalf("canceled shard work changed a committed chat: %+v", got)
	}
}

func TestRuntimeQueuedPeriodicProbeRechecksLastStartAtExecution(t *testing.T) {
	t.Parallel()
	const id = -627
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
	key := keyOf(id)
	if err := h.dispatcher.Submit(key, func(_ context.Context, sh *dispatch.Shard) { sh.Hold(key) }); err != nil {
		t.Fatal(err)
	}
	h.chats.ReportUnknown(id)
	h.tick()
	h.barrier(id)
	if n, err := h.janitor["chats_probe"].run(t.Context()); n != 1 || err != nil {
		t.Fatalf("periodic task while held = %d, %v", n, err)
	}
	if err := h.dispatcher.Submit(key, func(_ context.Context, sh *dispatch.Shard) { sh.Release(key) }); err != nil {
		t.Fatal(err)
	}
	req := h.nextRequest("getChat")
	h.barrier(id)
	h.assertCounter(`yabot_chats_probe_total{trigger="periodic",outcome="in_flight"}`, 0)
	h.assertCounter(`yabot_chats_probe_total{trigger="periodic",outcome="rate_limited"}`, 0)
	h.assertNoRequest()
	req.Done(sender.DroppedResult(sender.Busy, sender.ErrBusy))
	h.settleEffects(id)
	h.assertProbeTicket(0)
}
