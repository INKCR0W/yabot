package chats

import (
	"context"
	"errors"
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

type runtimeMemberAck chan error

func (a runtimeMemberAck) Done(err error) { a <- err }

func submitRuntimeMemberWithAck(h *runtimeHarness, update *models.Update) runtimeMemberAck {
	h.t.Helper()
	ack := make(runtimeMemberAck, 1)
	key := keyOf(update.MyChatMember.Chat.ID)
	if err := h.dispatcher.Submit(key, func(ctx context.Context, sh *dispatch.Shard) {
		h.chats.handleMemberUpdate(ctx, sh, update, h.now(), ack)
	}); err != nil {
		h.t.Fatal(err)
	}
	return ack
}

func assertRuntimeAckPending(t *testing.T, ack runtimeMemberAck) {
	t.Helper()
	select {
	case err := <-ack:
		t.Fatalf("membership update acknowledged early: %v", err)
	default:
	}
}

func TestRuntimeEffectReplayKeepsMemberUpdatesBehindCommittedEffects(t *testing.T) {
	t.Parallel()
	const id = -2030
	h := newRuntimeHarness(t, runtimeOptions{tickets: 1, beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
	h.apply(runtimeUpdate(id, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, false))
	probe := h.nextRequest("getChat")
	h.barrier(id)
	h.assertCounter(`yabot_chats_overload_total{source="sender_ticket"}`, 1)

	update := runtimeUpdate(id, 3, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false)
	later := submitRuntimeMemberWithAck(h, update)
	h.apply(runtimeUpdate(id-1, 4, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeLeft, false))
	h.barrier(id)
	assertRuntimeAckPending(t, later)
	if got := h.snapshot(id); got.State != Degraded || got.Watermark.ID != 2 || h.water(id).ID != 2 {
		t.Fatalf("chat while the downgrade waits for capacity = %+v", got)
	}

	h.advance(time.Second)
	h.tick()
	h.barrier(id)
	h.assertCounter(`yabot_chats_overload_total{source="sender_ticket"}`, 2)
	assertRuntimeAckPending(t, later)
	h.assertNoRequest()

	probe.Done(sender.DroppedResult(sender.Busy, sender.ErrBusy))
	h.barrier(id)
	assertRuntimeAckPending(t, later)
	h.assertCounter(`yabot_chats_probe_total{trigger="transition",outcome="busy"}`, 0)

	h.advance(time.Second)
	h.tick()
	downgrade := h.nextRequest("sendMessage")
	if downgrade.ChatID != id {
		t.Fatalf("replayed downgrade targeted chat %d, want %d", downgrade.ChatID, id)
	}
	if err := runtimeReceive(t, later); !errors.Is(err, ErrBusy) || dispatch.IsPermanent(err) {
		t.Fatalf("member write competing with the downgrade ticket returned %v, want transient busy", err)
	}
	h.barrier(id)
	if got := h.snapshot(id); got.State != Degraded || h.water(id).ID != 2 {
		t.Fatalf("write without capacity changed the chat: %+v", got)
	}
	h.assertCounter(`yabot_chats_probe_total{trigger="transition",outcome="busy"}`, 1)
	downgrade.Done(sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}})
	h.barrier(id)
	h.apply(update)
	if got := h.snapshot(id); got.State != Removed || h.water(id).ID != 3 {
		t.Fatalf("retired effects still blocked the later member update: %+v", got)
	}
	h.assertProbeTicket(0)
	h.assertNoRequest()
}

type runtimeDelayedStoreDone struct {
	Store
	callbacks chan func()
	open      atomic.Bool
}

func (s *runtimeDelayedStoreDone) Write(op store.Op, done func(error)) error {
	return s.Store.Write(op, func(err error) {
		if s.open.Load() {
			done(err)
			return
		}
		s.callbacks <- sync.OnceFunc(func() { done(err) })
	})
}

func (s *runtimeDelayedStoreDone) resume(h *runtimeHarness) {
	s.open.Store(true)
	h.storeBarrier()
	for {
		select {
		case complete := <-s.callbacks:
			complete()
		default:
			return
		}
	}
}

func TestRuntimeOldCommitCannotReleaseReplacementHold(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		newWrite      bool
		replacedTitle string
	}{
		{name: "another write superseded the pending completion", newWrite: true},
		{name: "the current record was replaced", replacedTitle: "Replacement"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const id = -2031
			key := keyOf(id)
			delayed := &runtimeDelayedStoreDone{callbacks: make(chan func(), 8)}
			h := newRuntimeHarness(t, runtimeOptions{tickets: 2,
				beforeStart: func(h *runtimeHarness) { h.seedActive(id, 10) },
				configure: func(deps *Deps) {
					delayed.Store = deps.Store
					deps.Store = delayed
				},
			})
			defer delayed.resume(h)
			original := h.snapshot(id)
			old := h.update(runtimeUpdate(id, 11, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, false))
			oldDone := runtimeReceive(t, delayed.callbacks)
			defer oldDone()
			if h.water(id).ID != 11 || h.snapshot(id) != original {
				t.Fatal("old commit was expected to wait for publication")
			}

			if tc.replacedTitle != "" {
				h.exec(`UPDATE chats SET state = 'removed', title = ?, member_event_update_id = 12 WHERE chat_id = ?`,
					tc.replacedTitle, id)
			}
			if err := h.dispatcher.Submit(key, func(_ context.Context, sh *dispatch.Shard) {
				sh.Drop(key, errors.New("chat replaced"))
				if tc.replacedTitle != "" {
					local := h.chats.local(sh)
					replacement := local.records[id].state
					replacement.Record.State = Removed
					replacement.Record.Title = tc.replacedTitle
					replacement.Record.Watermark.ID = 12
					replacement.MemberEpoch++
					h.chats.install(sh, local, &chatEntity{}, replacement, h.now())
					sh.Hold(key)
				}
			}); err != nil {
				t.Fatal(err)
			}
			h.barrier(id)

			var finishReplacement func()
			if tc.newWrite {
				replacement := h.update(runtimeUpdate(id, 12, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
				replacementDone := runtimeReceive(t, delayed.callbacks)
				defer replacementDone()
				finishReplacement = func() {
					replacementDone()
					if err := replacement.Wait(runtimeWait); err != nil {
						t.Fatalf("replacement write lost its completion: %v", err)
					}
				}
			} else {
				finishReplacement = func() {
					if err := h.dispatcher.Submit(key, func(_ context.Context, sh *dispatch.Shard) { sh.Release(key) }); err != nil {
						t.Fatal(err)
					}
				}
			}
			beforeLate := h.snapshot(id)
			version := h.chats.Version()
			follow := submitRuntimeMemberWithAck(h, runtimeUpdate(id, 10, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeLeft, false))
			h.barrier(id)
			assertRuntimeAckPending(t, follow)

			oldDone()
			if err := old.Wait(runtimeWait); !errors.Is(err, dispatch.ErrClosed) {
				t.Fatalf("superseded write settled with %v, want dispatch.ErrClosed", err)
			}
			h.barrier(id)
			assertRuntimeAckPending(t, follow)
			if got := h.snapshot(id); got != beforeLate || h.chats.Version() != version || h.water(id).ID != 12 {
				t.Fatalf("late completion changed the replacement: %+v", got)
			}
			h.assertNoRequest()

			finishReplacement()
			if err := runtimeReceive(t, follow); err != nil {
				t.Fatalf("replacement settlement did not release deferred membership: %v", err)
			}
			h.barrier(id)
			if got := h.snapshot(id); got.State != Removed || got.Watermark.ID != 12 {
				t.Fatalf("replacement was not preserved: %+v", got)
			}
			h.assertProbeTicket(0)
		})
	}
}
