package chats

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/janitor"
	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

func TestRuntimePeriodicJanitorProbesDueChatsInTurn(t *testing.T) {
	t.Parallel()
	const firstID, secondID, removedID = -100401, -100400, -100399
	h := newRuntimeHarness(t, runtimeOptions{shards: 2, beforeStart: func(h *runtimeHarness) {
		h.seedActive(firstID, 1)
		h.seedActive(secondID, 1)
		h.seedActive(removedID, 1)
		h.exec(`UPDATE chats SET state = 'removed' WHERE chat_id = ?`, removedID)
	}})
	job := h.janitor["chats_probe"]
	if job.every != 5*time.Second {
		t.Fatalf("periodic probe interval = %v, want 5s", job.every)
	}
	for _, id := range []int64{firstID, secondID} {
		if n, err := job.run(t.Context()); n != 1 || err != nil {
			t.Fatalf("periodic trigger count = %d, error = %v", n, err)
		}
		request := h.nextRequest("getChat")
		if request.ChatID != id || request.Class != sender.Background {
			t.Fatalf("periodic probe = %d, %v; want %d Background", request.ChatID, request.Class, id)
		}
		request.Done(sender.DroppedResult(sender.Busy, sender.ErrBusy))
		h.settleEffects(id)
	}
	h.advance(24*time.Hour - time.Nanosecond)
	if n, err := job.run(t.Context()); n != 0 || err != nil {
		t.Fatalf("periodic job selected an early or removed chat: n = %d, error = %v", n, err)
	}
	h.assertNoRequest()
	h.advance(time.Nanosecond)
	if n, err := job.run(t.Context()); n != 1 || err != nil {
		t.Fatalf("periodic job did not wrap to the first due chat: n = %d, error = %v", n, err)
	}
	if req := h.nextRequest("getChat"); req.ChatID != firstID {
		t.Fatalf("periodic cursor wrapped to %d, want %d", req.ChatID, firstID)
	}
}

func TestRuntimePeriodicJanitorRotatesPastADeferredProbe(t *testing.T) {
	t.Parallel()
	const first, second = -100405, -100404
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
		h.seedActive(first, 1)
		h.seedActive(second, 1)
	}})
	key := keyOf(first)
	if err := h.dispatcher.Submit(key, func(_ context.Context, sh *dispatch.Shard) { sh.Hold(key) }); err != nil {
		t.Fatal(err)
	}
	job := h.janitor["chats_probe"]
	for i := range 2 {
		if n, err := job.run(t.Context()); n != 1 || err != nil {
			t.Fatalf("periodic run %d: n = %d, error = %v", i+1, n, err)
		}
	}
	req := h.nextRequest("getChat")
	if req.ChatID != second {
		t.Fatalf("periodic probe for chat %d, want unheld chat %d", req.ChatID, second)
	}
	if n, err := job.run(t.Context()); n != 0 || err != nil {
		t.Fatalf("periodic run coalescing held candidate: n = %d, error = %v", n, err)
	}
	if err := h.dispatcher.Submit(key, func(_ context.Context, sh *dispatch.Shard) { sh.Release(key) }); err != nil {
		t.Fatal(err)
	}
	h.barrier(first)
	h.assertNoRequest()
	req.Done(sender.DroppedResult(sender.Shutdown, sender.ErrClosed))
	h.settleEffects(second)
	if n, err := job.run(t.Context()); n != 1 || err != nil {
		t.Fatalf("periodic run after the sender dropped a queued request: n = %d, error = %v", n, err)
	}
	if req := h.nextRequest("getChat"); req.ChatID != first {
		t.Fatalf("periodic probe for chat %d, want released chat %d", req.ChatID, first)
	}
	h.assertNoRequest()
}

func TestRuntimePeriodicProbeStartsAreSpacedAcrossShards(t *testing.T) {
	t.Parallel()
	h := newRuntimeHarness(t, runtimeOptions{shards: 2, deferChatsStart: true})
	var chats [2]int64
	shard := make(chan int, 1)
	for id := int64(-100500); id < -100436 && (chats[0] == 0 || chats[1] == 0); id++ {
		if err := h.dispatcher.Submit(keyOf(id), func(_ context.Context, sh *dispatch.Shard) { shard <- sh.Index() }); err != nil {
			t.Fatal(err)
		}
		chats[runtimeReceive(t, shard)] = id
	}
	for _, id := range chats {
		if id == 0 {
			t.Fatal("could not find a chat on each shard")
		}
		h.seedActive(id, 1)
	}
	if err := h.chats.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { runtimeStop(t, h.chats.Stop) })
	for _, id := range chats {
		key := keyOf(id)
		if err := h.dispatcher.Submit(key, func(_ context.Context, sh *dispatch.Shard) { sh.Hold(key) }); err != nil {
			t.Fatal(err)
		}
	}
	job := h.janitor["chats_probe"]
	run := func() {
		t.Helper()
		if n, err := job.run(t.Context()); n != 1 || err != nil {
			t.Fatalf("periodic run: n = %d, error = %v", n, err)
		}
	}
	run()
	run()
	h.barrier(chats[0])
	h.barrier(chats[1])
	h.assertNoRequest()

	ready := make(chan struct{}, len(chats))
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	for _, id := range chats {
		key := keyOf(id)
		if err := h.dispatcher.Submit(key, func(_ context.Context, sh *dispatch.Shard) {
			ready <- struct{}{}
			<-release
			sh.Release(key)
		}); err != nil {
			t.Fatal(err)
		}
	}
	for range chats {
		runtimeReceive(t, ready)
	}
	unblock()
	h.barrier(chats[0])
	h.barrier(chats[1])
	first := h.nextRequest("getChat")
	var waiting int64
	switch first.ChatID {
	case chats[0]:
		waiting = chats[1]
	case chats[1]:
		waiting = chats[0]
	default:
		t.Fatalf("unexpected periodic target %d", first.ChatID)
	}
	h.assertNoRequest()
	const limited = `yabot_chats_probe_total{trigger="periodic",outcome="rate_limited"}`
	h.assertCounter(limited, 1)
	queued := h.metrics.GetOrCreateGauge(`yabot_dispatch_tickets_inuse{shard="0"}`, nil).Get() +
		h.metrics.GetOrCreateGauge(`yabot_dispatch_tickets_inuse{shard="1"}`, nil).Get()
	if queued != 1 {
		t.Fatalf("periodic Probe holds %.0f tickets, want one", queued)
	}

	h.advance(2 * job.every)
	run()
	h.barrier(waiting)
	h.assertNoRequest()
	h.assertCounter(limited, 2)

	issued, finish, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	finishFirst := sync.OnceFunc(func() { close(finish) })
	t.Cleanup(finishFirst)
	do := first.Do
	first.Do = func(ctx context.Context, b *bot.Bot) error {
		err := do(ctx, b)
		close(issued)
		<-finish
		return err
	}
	go func() {
		h.finishRequest(first, `{"type":"supergroup","title":"Probe title"}`)
		close(finished)
	}()
	runtimeReceive(t, issued)
	h.advance(job.every - time.Nanosecond)
	run()
	h.barrier(waiting)
	h.assertNoRequest()
	h.assertCounter(limited, 3)
	h.advance(time.Nanosecond)
	h.tick()
	h.barrier(waiting)
	h.assertNoRequest()
	run()
	second := h.nextRequest("getChat")
	if second.ChatID != waiting {
		t.Fatalf("periodic Probe after the interval targeted %d, want %d", second.ChatID, waiting)
	}
	finishFirst()
	runtimeReceive(t, finished)
	h.nextRequest("getChatMember").Done(sender.DroppedResult(sender.Shutdown, sender.ErrClosed))
	h.settleEffects(first.ChatID)
	h.advance(24*time.Hour - job.every)
	run()
	h.barrier(first.ChatID)
	h.assertNoRequest()
	h.assertCounter(limited, 4)
	second.Done(sender.DroppedResult(sender.Shutdown, sender.ErrClosed))
}

func TestRuntimePeriodicProbeWithNoTicketLeavesTheGlobalPermitAvailable(t *testing.T) {
	t.Parallel()
	const first, second = -100407, -100406
	h := newRuntimeHarness(t, runtimeOptions{tickets: 1, beforeStart: func(h *runtimeHarness) {
		h.seedActive(first, 1)
		h.seedActive(second, 1)
	}})
	tickets := make(chan dispatch.Ticket, 1)
	if err := h.dispatcher.Submit(keyOf(first), func(_ context.Context, sh *dispatch.Shard) {
		ticket, ok := sh.Admit()
		if !ok {
			t.Error("could not reserve the available dispatcher ticket")
		}
		tickets <- ticket
	}); err != nil {
		t.Fatal(err)
	}
	ticket := runtimeReceive(t, tickets)
	release := sync.OnceFunc(ticket.Cancel)
	t.Cleanup(release)
	job := h.janitor["chats_probe"]
	if n, err := job.run(t.Context()); n != 1 || err != nil {
		t.Fatalf("first periodic run: n = %d, error = %v", n, err)
	}
	h.barrier(first)
	h.assertNoRequest()
	h.assertCounter(`yabot_chats_overload_total{source="sender_ticket"}`, 1)
	release()
	if n, err := job.run(t.Context()); n != 1 || err != nil {
		t.Fatalf("second periodic run: n = %d, error = %v", n, err)
	}
	if req := h.nextRequest("getChat"); req.ChatID != second {
		t.Fatalf("periodic Probe after partial overload targeted %d, want %d", req.ChatID, second)
	}
}

func TestRuntimeIdleJanitorProbesBeforeLeave(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		member    string
		leave     telegram.Class
		wantState ChatState
	}{
		{"became active", runtimeProbeAdmin, telegram.OK, Active},
		{"left", runtimeProbeMember, telegram.OK, Removed},
		{"transient leave failure", runtimeProbeMember, telegram.Retry, Onboarding},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const idle, recent, everActive = -100411, -100412, -100413
			h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
				oldTime := h.now().Add(-168 * time.Hour).Unix()
				seedIdleChat(h, idle, oldTime, false)
				seedIdleChat(h, recent, oldTime+1, false)
				seedIdleChat(h, everActive, oldTime, true)
			}})
			job := h.janitor["chats_idle"]
			if job.every != time.Hour {
				t.Fatalf("idle sweep interval = %v, want 1h", job.every)
			}
			if n, err := job.run(t.Context()); n != 1 || err != nil {
				t.Fatalf("idle candidates = %d, error = %v", n, err)
			}
			before := h.snapshot(idle)
			request := h.nextRequest("getChat")
			if request.ChatID != idle {
				t.Fatalf("idle probe chat = %d, want %d", request.ChatID, idle)
			}
			h.finishRequest(request, `{"type":"supergroup","title":"Idle chat"}`)
			h.finishRequest(h.nextRequest("getChatMember"), tc.member)
			h.finishRequest(h.nextRequest("getChatAdministrators"), runtimeProbeOwner)
			if tc.wantState != Active {
				leave := h.nextRequest("leaveChat")
				if leave.ChatID != idle || leave.Class != sender.Background || !leave.NoRetry {
					t.Fatalf("idle leave = %d, %v, NoRetry=%v", leave.ChatID, leave.Class, leave.NoRetry)
				}
				leave.Done(sender.Result{Outcome: telegram.Outcome{Class: tc.leave}})
			}
			h.settleEffects(idle)
			after := h.snapshot(idle)
			if after.State != tc.wantState || after.MemberEpoch != before.MemberEpoch {
				t.Fatalf("idle sweep state = %+v, want %s in epoch %d", after, tc.wantState, before.MemberEpoch)
			}
			if tc.wantState == Removed && (after.PurgeAfter != h.now().Unix() || after.RemovedReason != RemovedByIdle) {
				t.Fatalf("idle departure did not become eligible for cleanup: %+v", after)
			}
			h.assertNoRequest()
		})
	}
}

func TestRuntimeTickRetriesIdleProbeAtRateBoundary(t *testing.T) {
	t.Parallel()
	const chatID = -100414
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
		seedIdleChat(h, chatID, h.now().Add(-8*24*time.Hour).Unix(), false)
	}})
	h.chats.ReportUnknown(chatID)
	h.tick()
	h.nextRequest("getChat").Done(sender.DroppedResult(sender.Busy, sender.ErrBusy))
	h.settleEffects(chatID)
	if n, err := h.janitor["chats_idle"].run(t.Context()); n != 1 || err != nil {
		t.Fatalf("idle candidate not dispatched: n = %d, error = %v", n, err)
	}
	h.barrier(chatID)
	h.assertNoRequest()
	h.advance(ProbeInterval - time.Nanosecond)
	h.tick()
	h.barrier(chatID)
	h.assertNoRequest()
	h.advance(time.Nanosecond)
	h.tick()
	if req := h.nextRequest("getChat"); req.ChatID != chatID {
		t.Fatalf("idle retry chat = %d, want %d", req.ChatID, chatID)
	}
	h.tick()
	h.barrier(chatID)
	h.assertCounter(`yabot_chats_probe_total{trigger="idle",outcome="in_flight"}`, 0)
}

func TestRuntimeIdleSweepContinuesPastFullBatch(t *testing.T) {
	t.Parallel()
	const firstID = -100499
	h := newRuntimeHarness(t, runtimeOptions{tickets: 64, beforeStart: func(h *runtimeHarness) {
		h.exec(`WITH RECURSIVE ids(id) AS (VALUES (?) UNION ALL SELECT id+1 FROM ids WHERE id+1 < ?)
			INSERT INTO chats (chat_id, type, state, added_at, updated_at)
			SELECT id, 'supergroup', 'onboarding', ?, ? FROM ids`, firstID, firstID+51,
			h.now().Add(-8*24*time.Hour).Unix(), h.now().Add(-8*24*time.Hour).Unix())
	}})
	job := h.janitor["chats_idle"]
	if n, err := job.run(t.Context()); n != 50 || !errors.Is(err, janitor.ErrBacklog) {
		t.Fatalf("first idle page: n = %d, error = %v", n, err)
	}
	if n, err := job.run(t.Context()); n != 1 || err != nil {
		t.Fatalf("last idle page: n = %d, error = %v", n, err)
	}
	for i := range int64(51) {
		req := h.nextRequest("getChat")
		if req.ChatID != firstID+i {
			t.Fatalf("idle probe %d = %d, want %d", i, req.ChatID, firstID+i)
		}
		if i == 0 {
			req.Done(sender.DroppedResult(sender.Busy, sender.ErrBusy))
		}
	}
	h.assertNoRequest()
	h.settleEffects(firstID)
	h.advance(ProbeInterval)
	if n, err := job.run(t.Context()); n != 50 || !errors.Is(err, janitor.ErrBacklog) {
		t.Fatalf("next idle scan did not resume at the start: n = %d, error = %v", n, err)
	}
	if req := h.nextRequest("getChat"); req.ChatID != firstID {
		t.Fatalf("next idle scan skipped the first chat: %d", req.ChatID)
	}
}

func TestRuntimeIdleSweepCoalescesCandidateHeldInMailbox(t *testing.T) {
	t.Parallel()
	const chatID = -100415
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
		seedIdleChat(h, chatID, h.now().Add(-8*24*time.Hour).Unix(), false)
	}})
	unblock := blockMaintenanceWriter(t, h)
	receipt := h.update(runtimeUpdate(chatID, 1, h.now().Unix(), models.ChatMemberTypeMember, models.ChatMemberTypeMember, false))
	h.barrier(chatID)
	job := h.janitor["chats_idle"]
	if n, err := job.run(t.Context()); n != 1 || err != nil {
		t.Fatalf("first idle sweep: n = %d, error = %v", n, err)
	}
	h.barrier(chatID)
	if n, err := job.run(t.Context()); n != 0 || err != nil {
		t.Fatalf("held candidate was submitted twice: n = %d, error = %v", n, err)
	}
	h.barrier(chatID)
	h.assertNoRequest()
	unblock()
	if err := receipt.Wait(runtimeWait); err != nil {
		t.Fatal(err)
	}
	if req := h.nextRequest("getChat"); req.ChatID != chatID {
		t.Fatalf("idle probe = %d, want %d", req.ChatID, chatID)
	}
}

func TestRuntimeMaintenanceBusySubmissionsCanBeRetried(t *testing.T) {
	t.Parallel()
	for _, task := range []string{"chats_probe", "chats_idle"} {
		t.Run(task, func(t *testing.T) {
			const chatID = -100416
			h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
				seedIdleChat(h, chatID, h.now().Add(-8*24*time.Hour).Unix(), false)
			}})
			started, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			t.Cleanup(unblock)
			if err := h.dispatcher.Submit(keyOf(chatID), func(context.Context, *dispatch.Shard) {
				close(started)
				<-release
			}); err != nil {
				t.Fatal(err)
			}
			runtimeReceive(t, started)
			for {
				err := h.dispatcher.Submit(keyOf(chatID), func(context.Context, *dispatch.Shard) {})
				if errors.Is(err, dispatch.ErrBusy) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			job := h.janitor[task]
			for i := range uint64(2) {
				if n, err := job.run(t.Context()); n != 0 || err != nil {
					t.Fatalf("busy maintenance run: n = %d, error = %v", n, err)
				}
				h.assertCounter(`yabot_chats_maintenance_busy_total{task="`+task+`"}`, i+1)
			}
			h.assertLog(slog.LevelInfo, "chat maintenance inbox is full", "chat_id", int64(chatID), "task", task)
			unblock()
			h.barrier(chatID)
			if n, err := job.run(t.Context()); n != 1 || err != nil {
				t.Fatalf("maintenance retry: n = %d, error = %v", n, err)
			}
			if req := h.nextRequest("getChat"); req.ChatID != chatID {
				t.Fatalf("maintenance retried chat %d, want %d", req.ChatID, chatID)
			}
		})
	}
}

func TestRuntimeMaintenanceReturnsCancellationAndClosedErrors(t *testing.T) {
	t.Parallel()
	const chatID = -100417
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
		seedIdleChat(h, chatID, h.now().Add(-8*24*time.Hour).Unix(), false)
	}})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, task := range []string{"chats_probe", "chats_idle"} {
		if n, err := h.janitor[task].run(ctx); n != 0 || !errors.Is(err, context.Canceled) {
			t.Fatalf("%s returned n = %d, error = %v for canceled context", task, n, err)
		}
	}
	h.barrier(chatID)
	h.assertNoRequest()
	runtimeStop(t, h.dispatcher.Stop)
	for _, task := range []string{"chats_probe", "chats_idle"} {
		if n, err := h.janitor[task].run(t.Context()); n != 0 || !errors.Is(err, dispatch.ErrClosed) {
			t.Fatalf("%s returned n = %d, error = %v for closed dispatcher", task, n, err)
		}
	}
}

func TestRuntimeMaintenanceReturnsIdleQueryError(t *testing.T) {
	t.Parallel()
	h := newRuntimeHarness(t, runtimeOptions{})
	job := h.janitor["chats_idle"]
	h.exec(`INSERT INTO chats (chat_id, state, updated_at) VALUES (-100417, 'onboarding', 1.5)`)
	if n, err := job.run(t.Context()); n != 0 || err == nil || !strings.Contains(err.Error(), "scan idle chat") {
		t.Fatalf("idle sweep with a fractional timestamp: n = %d, error = %v", n, err)
	}
	if inUse := h.store.Read().Stats().InUse; inUse != 0 {
		t.Fatalf("failed idle scan left %d SQLite connections checked out", inUse)
	}
	h.exec("DROP TABLE chats")
	if n, err := job.run(t.Context()); n != 0 || err == nil || !strings.Contains(err.Error(), "query idle chats") {
		t.Fatalf("idle sweep without chats table: n = %d, error = %v", n, err)
	}
	h.assertNoRequest()
}

type maintenanceDispatcher struct {
	Dispatcher
	submit func(dispatch.Key, dispatch.Task) error
	runAll func(context.Context, dispatch.AsyncTask) error
}

func (d maintenanceDispatcher) Submit(k dispatch.Key, task dispatch.Task) error {
	if d.submit != nil {
		return d.submit(k, task)
	}
	return d.Dispatcher.Submit(k, task)
}

func (d maintenanceDispatcher) RunOnAll(ctx context.Context, task dispatch.AsyncTask) error {
	if d.runAll != nil {
		return d.runAll(ctx, task)
	}
	return d.Dispatcher.RunOnAll(ctx, task)
}

func TestRuntimeIdleSweepStopsSubmittingAfterCancellation(t *testing.T) {
	t.Parallel()
	const first, second = -100419, -100418
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var armed atomic.Bool
	h := newRuntimeHarness(t, runtimeOptions{
		beforeStart: func(h *runtimeHarness) {
			idleAt := h.now().Add(-8 * 24 * time.Hour).Unix()
			seedIdleChat(h, first, idleAt, false)
			seedIdleChat(h, second, idleAt, false)
		},
		configure: func(d *Deps) {
			actual := d.Dispatcher
			d.Dispatcher = maintenanceDispatcher{Dispatcher: actual, submit: func(k dispatch.Key, task dispatch.Task) error {
				err := actual.Submit(k, task)
				if armed.Load() && k.A == first {
					cancel()
				}
				return err
			}}
		},
	})
	armed.Store(true)
	if n, err := h.janitor["chats_idle"].run(ctx); n != 1 || !errors.Is(err, context.Canceled) {
		t.Fatalf("partially canceled idle sweep: n = %d, error = %v", n, err)
	}
	if req := h.nextRequest("getChat"); req.ChatID != first {
		t.Fatalf("idle probe for %d, want %d", req.ChatID, first)
	}
	h.barrier(second)
	h.assertNoRequest()
}

func seedIdleChat(h *runtimeHarness, chatID, at int64, everActive bool) {
	h.t.Helper()
	h.exec(`INSERT INTO chats (chat_id, type, title, state, added_at, updated_at, ever_active)
		VALUES (?, 'supergroup', 'Idle chat', 'onboarding', ?, ?, ?)`, chatID, at, at, everActive)
}

func TestRuntimeOwnerWritesCommitBeforeReplyAndListOnlyPresentChats(t *testing.T) {
	t.Parallel()
	const first, second, third, removed = -100425, -100423, -100421, -100420
	h := newRuntimeHarness(t, runtimeOptions{shards: 3, beforeStart: func(h *runtimeHarness) {
		for _, id := range []int64{second, removed, first, third} {
			h.seedActive(id, 1)
		}
		h.exec(`UPDATE chats SET state = 'removed' WHERE chat_id = ?`, removed)
		h.exec(`UPDATE chats SET state = 'degraded', guard_assigned = 1 WHERE chat_id = ?`, first)
	}})
	before := h.chats.Version()
	if err := h.setOwner(t.Context(), second, 72); err != nil {
		t.Fatal(err)
	}
	var owner sql.NullInt64
	if err := h.store.Read().QueryRowContext(t.Context(), `SELECT owner_id FROM chats WHERE chat_id=?`, second).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if !owner.Valid || owner.Int64 != 72 || h.snapshot(second).OwnerID != 72 || h.chats.Version() != before {
		t.Fatalf("claim did not atomically publish owner: SQL=%+v snapshot=%+v", owner, h.snapshot(second))
	}
	h.exec(`CREATE TRIGGER forbid_redundant_claim BEFORE UPDATE ON chats WHEN NEW.chat_id = -100423
		BEGIN SELECT raise(ABORT, 'redundant claim'); END`)
	if err := h.setOwner(t.Context(), second, 72); err != nil {
		t.Fatalf("repeated claim performed a write: %v", err)
	}
	if err := h.setOwner(t.Context(), removed, 72); !errors.Is(err, ErrForbidden) {
		t.Fatalf("removed group claim = %v, want ErrForbidden", err)
	}
	want := []ChatRef{
		{ID: first, Title: "Initial title", Type: models.ChatTypeSupergroup, State: Degraded, GuardAssigned: true},
		{ID: third, Title: "Initial title", Type: models.ChatTypeSupergroup, State: Active},
	}
	if got := h.chats.OwnedBy(81); !slices.Equal(got, want) {
		t.Fatalf("previous owner's chats = %+v, want %+v", got, want)
	}
	if got := h.chats.OwnedBy(72); len(got) != 1 || got[0].ID != second {
		t.Fatalf("new owner's chats = %+v, want %d", got, second)
	}
}

type maintenanceStore struct {
	Store
	write func(store.Op, func(error)) error
}

func (s maintenanceStore) Write(op store.Op, done func(error)) error { return s.write(op, done) }

func TestRuntimeEraseScrubsInFlightCommitAndRollback(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		failPersist bool
	}{
		{"successful write", false},
		{"operation failure", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const chatID, userID = -100431, 83
			writes := make(chan struct{}, 2)
			h := newRuntimeHarness(t, runtimeOptions{configure: func(d *Deps) {
				actual := d.Store
				d.Store = maintenanceStore{Store: actual, write: func(op store.Op, done func(error)) error {
					err := actual.Write(op, done)
					select {
					case writes <- struct{}{}:
					default:
					}
					return err
				}}
			}, beforeStart: func(h *runtimeHarness) {
				h.seedActive(chatID, 1)
				h.exec(`UPDATE chats SET owner_id = ? WHERE chat_id = ?`, userID, chatID)
				if tc.failPersist {
					h.exec(`CREATE TRIGGER block_member_write BEFORE UPDATE ON chats WHEN NEW.member_event_update_id = 2
						AND NEW.owner_id IS NOT NULL BEGIN SELECT raise(ABORT, 'forced membership failure'); END`)
				}
			}})
			unblock := blockMaintenanceWriter(t, h)
			update := runtimeUpdate(chatID, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, true)
			receipt := h.update(update)
			runtimeReceive(t, writes)
			deleted := make(chan error, 1)
			go func() { deleted <- h.chats.DeleteUser(t.Context(), userID) }()
			runtimeReceive(t, writes)
			if s := h.snapshot(chatID); s.OwnerID != 0 {
				t.Fatalf("eraser left the owner visible during the held write: %+v", s)
			}
			select {
			case err := <-deleted:
				t.Fatalf("eraser returned before the SQL commit: %v", err)
			default:
			}
			unblock()
			err := receipt.Wait(runtimeWait)
			if tc.failPersist != dispatch.IsPermanent(err) || !tc.failPersist && err != nil {
				t.Fatalf("membership receipt = %v, fail=%v", err, tc.failPersist)
			}
			if err := runtimeReceive(t, deleted); err != nil {
				t.Fatal(err)
			}
			if snap := h.snapshot(chatID); snap.OwnerID != 0 {
				t.Fatalf("membership write republished erased identity: %+v", snap)
			}
			wantWatermark := int64(2)
			if tc.failPersist {
				wantWatermark = 1
			}
			if got := h.water(chatID).ID; got != wantWatermark {
				t.Fatalf("stored watermark after erasure = %d, want %d", got, wantWatermark)
			}
			assertNoStoredChatIdentity(t, h, userID)
		})
	}
}

func blockMaintenanceWriter(t *testing.T, h *runtimeHarness) func() {
	t.Helper()
	blocked, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	if err := h.store.Write(func(*sql.Tx) error {
		close(blocked)
		<-release
		return nil
	}, func(error) {}); err != nil {
		t.Fatal(err)
	}
	runtimeReceive(t, blocked)
	return unblock
}

func TestRuntimeEraseCanRetryAfterStoreSubmissionPanics(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		panicWrite  int64
		rows        int
		wantPending int
	}{
		{"first batch submission", 1, 1, 1},
		{"writer callback submitting another batch", 2, 501, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const firstChat, userID = -200000, 79
			var writes atomic.Int64
			h := newRuntimeHarness(t, runtimeOptions{configure: func(d *Deps) {
				actual := d.Store
				d.Store = maintenanceStore{Store: actual, write: func(op store.Op, done func(error)) error {
					if writes.Add(1) == tc.panicWrite {
						panic("injected erasure submit panic")
					}
					return actual.Write(op, done)
				}}
			}, beforeStart: func(h *runtimeHarness) {
				h.exec(`WITH RECURSIVE ids(id) AS (VALUES (?) UNION ALL SELECT id+1 FROM ids WHERE id+1 < ?)
					INSERT INTO chats (chat_id, type, state, updated_at, owner_id)
					SELECT id, 'supergroup', 'active', ?, ? FROM ids`, firstChat, firstChat+tc.rows, h.now().Unix(), userID)
			}})
			ctx, cancel := context.WithTimeout(t.Context(), runtimeWait)
			defer cancel()
			if err := h.chats.DeleteUser(ctx, userID); err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("erasure panic = %v, want a settled error", err)
			}
			if n := storedChatIdentities(t, h, userID); n != tc.wantPending {
				t.Fatalf("rows still holding user identity = %d, want %d", n, tc.wantPending)
			}
			if err := h.chats.DeleteUser(ctx, userID); err != nil {
				t.Fatalf("retrying erasure: %v", err)
			}
			assertNoStoredChatIdentity(t, h, userID)
			if err := h.setOwner(ctx, firstChat, userID); err != nil {
				t.Fatalf("erasure guard leaked: %v", err)
			}
			if snap := h.snapshot(firstChat); snap.OwnerID != userID {
				t.Fatalf("later owner claim did not restore the owner: %+v", snap)
			}
		})
	}
}

func assertNoStoredChatIdentity(t *testing.T, h *runtimeHarness, userID int64) {
	t.Helper()
	if n := storedChatIdentities(t, h, userID); n != 0 {
		t.Fatalf("%d SQLite rows still contain erased identity", n)
	}
}

func storedChatIdentities(t *testing.T, h *runtimeHarness, userID int64) int {
	t.Helper()
	var n int
	err := h.store.Read().QueryRowContext(t.Context(), `SELECT count(*) FROM chats WHERE owner_id = ?`, userID).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}
