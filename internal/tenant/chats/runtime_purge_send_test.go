package chats

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

func TestRuntimePurgeCanceledInMailboxDoesNotEnqueueLeave(t *testing.T) {
	t.Parallel()
	const id = -100847
	entryRan := make(chan struct{}, 1)
	var watchEntry atomic.Bool
	h := newRuntimeHarness(t, runtimeOptions{
		beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) },
		configure: func(d *Deps) {
			actual := d.Dispatcher
			d.Dispatcher = maintenanceDispatcher{Dispatcher: actual, submit: func(k dispatch.Key, task dispatch.Task) error {
				if !watchEntry.Swap(false) {
					return actual.Submit(k, task)
				}
				return actual.Submit(k, func(ctx context.Context, sh *dispatch.Shard) {
					task(ctx, sh)
					entryRan <- struct{}{}
				})
			}}
		},
	})
	k := keyOf(id)
	if err := h.dispatcher.Submit(k, func(_ context.Context, sh *dispatch.Shard) { sh.Hold(k) }); err != nil {
		t.Fatal(err)
	}
	release := sync.OnceFunc(func() {
		if err := h.dispatcher.Submit(k, func(_ context.Context, sh *dispatch.Shard) { sh.Release(k) }); err != nil {
			t.Error(err)
		}
	})
	defer release()
	h.barrier(id)
	before := h.snapshot(id)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	watchEntry.Store(true)
	go func() { result <- h.chats.Purge(ctx, id) }()
	runtimeReceive(t, entryRan)
	h.assertNoRequest()
	cancel()
	if err := runtimeReceive(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("Purge returned %v, want context.Canceled", err)
	}
	release()
	h.settleEffects(id)
	h.assertNoRequest()
	h.assertProbeTicket(0)
	assertRuntimePurgeState(h, id, before, true)
}

func TestRuntimePurgeSkipsLeaveWhenRejoinedBeforeAdmission(t *testing.T) {
	t.Parallel()
	const id = -100845
	delayed := &runtimeDelayedStoreDone{callbacks: make(chan func(), 1)}
	entryRan := make(chan struct{}, 1)
	var watchEntry atomic.Bool
	h := newRuntimeHarness(t, runtimeOptions{
		beforeStart: func(h *runtimeHarness) {
			h.seedActive(id, 1)
			seedPurgeSettings(h, id)
		},
		configure: func(d *Deps) {
			delayed.Store = d.Store
			d.Store = delayed
			actual := d.Dispatcher
			d.Dispatcher = maintenanceDispatcher{Dispatcher: actual, submit: func(k dispatch.Key, task dispatch.Task) error {
				if !watchEntry.Swap(false) {
					return actual.Submit(k, task)
				}
				return actual.Submit(k, func(ctx context.Context, sh *dispatch.Shard) {
					task(ctx, sh)
					entryRan <- struct{}{}
				})
			}}
		},
	})
	defer delayed.resume(h)
	initial := h.snapshot(id)
	left := h.update(purgeMemberUpdate(h, id, 2, models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
	commitLeft := runtimeReceive(t, delayed.callbacks)
	defer commitLeft()
	rejoin := h.update(purgeMemberUpdate(h, id, 3, models.ChatMemberTypeLeft, models.ChatMemberTypeAdministrator, true))
	h.barrier(id)

	watchEntry.Store(true)
	result := make(chan error, 1)
	go func() { result <- h.chats.Purge(t.Context(), id) }()
	runtimeReceive(t, entryRan)
	if got := h.snapshot(id); got != initial || got.MemberEpoch != 1 {
		t.Fatalf("snapshot at Purge entry = %+v, want the original membership", got)
	}
	h.assertNoRequest()

	commitLeft()
	if err := left.Wait(runtimeWait); err != nil {
		t.Fatal(err)
	}
	commitRejoin := runtimeReceive(t, delayed.callbacks)
	defer commitRejoin()
	if got := h.snapshot(id); got.State != Removed || got.MemberEpoch != 1 {
		t.Fatalf("snapshot before rejoin publication = %+v, want removed with the original membership", got)
	}
	h.assertNoRequest()
	delayed.open.Store(true)
	commitRejoin()
	if err := rejoin.Wait(runtimeWait); err != nil {
		t.Fatal(err)
	}
	h.nextRequest("getChat").Done(purgeLeaveResult(telegram.Retry))
	leave := h.nextRequest("leaveChat")
	want, version := h.snapshot(id), h.chats.Version()
	if want.State != Active || want.MemberEpoch != 2 || want.Watermark.ID != 3 || want.PurgeAfter != 0 {
		t.Fatalf("snapshot after rejoin publication = %+v, want active with the new membership", want)
	}

	runPurgeLeaveRequest(t, leave, 0)
	if err := runtimeReceive(t, result); !errors.Is(err, ErrMembershipChanged) {
		t.Fatalf("Purge returned %v, want ErrMembershipChanged", err)
	}
	assertRuntimePurgeState(h, id, want, true)
	if got := h.chats.Version(); got != version {
		t.Fatalf("Purge changed view version from %d to %d", version, got)
	}
	assertPurgeSettings(h, id)
	h.assertProbeTicket(0)
	h.assertNoRequest()
}

func TestRuntimePurgeRechecksMembershipBeforeSenderCallsTelegram(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		initial      bool
		change       string
		wantEpoch    uint64
		wantConflict bool
		wantCalls    int
	}{
		{name: "rejoined while queued", initial: true, change: "rejoin", wantEpoch: 2, wantConflict: true},
		{name: "permission changed while queued", initial: true, change: "downgrade", wantEpoch: 1, wantCalls: 1},
		{name: "record appeared while queued", change: "join", wantEpoch: 1, wantConflict: true},
		{name: "record disappeared while queued", initial: true, change: "clear"},
		{name: "record stayed absent", wantCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const id = -100846
			h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
				if tc.initial {
					h.seedActive(id, 1)
				}
				seedPurgeSettings(h, id)
			}})
			leave, result := startRuntimePurge(h, t.Context(), id)
			if tc.change == "rejoin" || tc.change == "clear" {
				h.apply(purgeMemberUpdate(h, id, 2, models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
			}
			switch tc.change {
			case "rejoin", "join":
				h.apply(purgeMemberUpdate(h, id, 3, models.ChatMemberTypeLeft, models.ChatMemberTypeAdministrator, true))
				h.nextRequest("getChat").Done(purgeLeaveResult(telegram.Retry))
			case "downgrade":
				h.apply(purgeMemberUpdate(h, id, 2, models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, false))
				h.nextRequest("getChat").Done(purgeLeaveResult(telegram.Retry))
			case "clear":
				submitRuntimeClear(t, h, h.snapshot(id))
			}
			h.settleEffects(id)
			want, exists := h.chats.State(id)
			if want.MemberEpoch != tc.wantEpoch || exists != (tc.wantEpoch != 0) {
				t.Fatalf("membership before send = (%t, %d), want epoch %d", exists, want.MemberEpoch, tc.wantEpoch)
			}
			version := h.chats.Version()

			runPurgeLeaveRequest(t, leave, tc.wantCalls)
			var wantErr error
			if tc.wantConflict {
				wantErr = ErrMembershipChanged
			}
			if err := runtimeReceive(t, result); !errors.Is(err, wantErr) {
				t.Fatalf("Purge returned %v, want %v", err, wantErr)
			}
			if tc.wantCalls == 0 {
				h.assertCounter(`yabot_chats_effects_total{effect="leave_outcome",outcome="canceled"}`, 1)
				h.assertCounter(`yabot_chats_effects_total{effect="leave_outcome",outcome="unknown"}`, 0)
				h.assertLog(slog.LevelInfo, "chat purge leave canceled before execution", "chat_id", id)
			} else if exists {
				want.State, want.BotRights, want.RemovedReason = Removed, 0, RemovedByPurge
				want.RemovedAt, want.UpdatedAt, want.PurgeAfter = h.now().Unix(), h.now().Unix(), h.now().Unix()
				version++
			}
			assertRuntimePurgeState(h, id, want, exists)
			if got := h.chats.Version(); got != version {
				t.Fatalf("view version = %d, want %d", got, version)
			}
			assertPurgeSettings(h, id)
			h.assertProbeTicket(0)
			h.assertNoRequest()
		})
	}
}

func runPurgeLeaveRequest(t *testing.T, request sender.Request, wantCalls int) {
	t.Helper()
	calls := 0
	upstream := successfulRuntimeLeaveBot(t, &calls)
	err := request.Do(t.Context(), upstream)
	request.Done(sender.Result{Outcome: telegram.Classify(err), Err: err, Attempts: 1})
	if calls != wantCalls {
		t.Errorf("leaveChat made %d Telegram calls, want %d", calls, wantCalls)
	}
}

const purgeSavedSettings = `{"language":"en"}`

func seedPurgeSettings(h *runtimeHarness, id int64) {
	h.t.Helper()
	h.exec(`INSERT INTO chat_settings (chat_id, feature, version, rev, data, etag, updated_at)
		VALUES (?, 'general', 1, 1, ?, 'saved', ?)`, id, purgeSavedSettings, h.now().Unix())
}

func assertPurgeSettings(h *runtimeHarness, id int64) {
	h.t.Helper()
	var data string
	if err := h.store.Read().QueryRowContext(h.t.Context(), `SELECT data FROM chat_settings WHERE chat_id=?`, id).Scan(&data); err != nil || data != purgeSavedSettings {
		h.t.Fatalf("saved setting = %q (%v), want %q", data, err, purgeSavedSettings)
	}
}
