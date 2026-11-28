package chats

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

type privacyRaceGate struct {
	armed   atomic.Bool
	reached chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func newPrivacyRaceGate() *privacyRaceGate {
	return &privacyRaceGate{reached: make(chan struct{}), resume: make(chan struct{})}
}

func (g *privacyRaceGate) wait() bool {
	if !g.armed.Swap(false) {
		return false
	}
	close(g.reached)
	<-g.resume
	return true
}

func (g *privacyRaceGate) release() {
	g.once.Do(func() { close(g.resume) })
}

func claimToken(t *testing.T, h *runtimeHarness, userID, chatID int64) ClaimToken {
	t.Helper()
	token, err := h.chats.ClaimToken(userID, chatID)
	if err != nil {
		t.Fatalf("claim token for chat %d: %v", chatID, err)
	}
	return token
}

func assertStoredOwner(t *testing.T, h *runtimeHarness, chatID, want int64) {
	t.Helper()
	var got sql.NullInt64
	err := h.store.Read().QueryRowContext(t.Context(), "SELECT owner_id FROM chats WHERE chat_id = ?", chatID).Scan(&got)
	if err != nil {
		t.Fatal(err)
	}
	if got.Valid != (want != 0) || got.Int64 != want {
		t.Fatalf("SQLite owner for chat %d = %+v, want %d", chatID, got, want)
	}
}

func runtimeShardOf(t *testing.T, h *runtimeHarness, chatID int64) int {
	t.Helper()
	index := make(chan int, 1)
	if err := h.dispatcher.Submit(keyOf(chatID), func(_ context.Context, sh *dispatch.Shard) {
		index <- sh.Index()
	}); err != nil {
		t.Fatal(err)
	}
	return runtimeReceive(t, index)
}

func runtimeChatOnShard(t *testing.T, h *runtimeHarness, first int64, shard int) int64 {
	t.Helper()
	for id := first; id < first+128; id++ {
		if runtimeShardOf(t, h, id) == shard {
			return id
		}
	}
	t.Fatalf("could not find a chat on shard %d", shard)
	return 0
}

func TestRuntimeClaimsAndProbeCannotWriteDuringEraseOnAnotherShard(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name               string
		completeBeforeDone bool
	}{
		{"probe completes during erasure", true},
		{"probe completes after erasure", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const chatID, ownerBefore, erasedUser = int64(-100600), 81, 555
			cleaned := make(chan error, 1)
			gate := newPrivacyRaceGate()
			var erasing atomic.Bool
			var blockedShard int
			h := newRuntimeHarness(t, runtimeOptions{
				shards: 2, deferChatsStart: true,
				configure: func(d *Deps) {
					actual := d.Dispatcher
					d.Dispatcher = maintenanceDispatcher{Dispatcher: actual, runAll: func(ctx context.Context, task dispatch.AsyncTask) error {
						if !erasing.Load() {
							return actual.RunOnAll(ctx, task)
						}
						return actual.RunOnAll(ctx, func(shCtx context.Context, sh *dispatch.Shard, done func(error)) {
							if sh.Index() == blockedShard {
								gate.wait()
								task(shCtx, sh, done)
								return
							}
							task(shCtx, sh, func(err error) {
								done(err)
								cleaned <- err
							})
						})
					}}
				},
			})
			t.Cleanup(gate.release)
			probeShard := runtimeShardOf(t, h, chatID)
			blockedShard = 1 - probeShard
			otherChat := runtimeChatOnShard(t, h, chatID+1, blockedShard)
			cleanedChat := runtimeChatOnShard(t, h, chatID+1, probeShard)
			h.seedActive(chatID, 1)
			h.seedActive(otherChat, 2)
			h.seedActive(cleanedChat, 3)
			h.exec("UPDATE chats SET owner_id = ? WHERE chat_id IN (?, ?)", erasedUser, otherChat, cleanedChat)
			if err := h.chats.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { runtimeStop(t, h.chats.Stop) })

			stale := claimToken(t, h, erasedUser, chatID)
			gate.armed.Store(true)
			erasing.Store(true)
			erased := make(chan error, 1)
			go func() { erased <- h.chats.DeleteUser(t.Context(), erasedUser) }()
			runtimeReceive(t, gate.reached)
			if err := runtimeReceive(t, cleaned); err != nil {
				t.Fatalf("first erasure shard: %v", err)
			}
			if got := h.snapshot(cleanedChat).OwnerID; got != 0 {
				t.Fatalf("finished shard has not scrubbed owner %d", got)
			}
			if got := h.snapshot(otherChat).OwnerID; got != erasedUser {
				t.Fatalf("blocked shard was already scrubbed: %d", got)
			}
			assertStoredOwner(t, h, cleanedChat, erasedUser)
			assertStoredOwner(t, h, otherChat, erasedUser)
			if _, err := h.chats.ClaimToken(erasedUser, chatID); !errors.Is(err, ErrBusy) {
				t.Fatalf("claim token during erasure = %v, want ErrBusy", err)
			}
			if err := h.chats.SetOwner(t.Context(), chatID, erasedUser, stale); !errors.Is(err, ErrBusy) {
				t.Fatalf("pre-erasure token during erasure = %v, want ErrBusy", err)
			}
			h.assertClaimRejections(chatID, erasedUser, "erasure", 2)
			h.chats.ReportUnknown(chatID)
			select {
			case h.ticks[probeShard] <- h.now():
			case <-time.After(runtimeWait):
				t.Fatal("timed out delivering the available shard's tick")
			}
			runtimeReceive(t, h.ticked[probeShard])
			h.finishRequest(h.nextRequest("getChat"), `{"type":"supergroup","title":"Updated title"}`)
			h.finishRequest(h.nextRequest("getChatMember"), runtimeProbeAdmin)
			admins := h.nextRequest("getChatAdministrators")
			complete := func() {
				h.finishRequest(admins, runtimeProbeOwner)
				h.settleEffects(chatID)
				if got := h.snapshot(chatID); got.Title != "Updated title" || got.OwnerID != ownerBefore {
					t.Fatalf("overlapping Probe changed owner or lost updates: %+v", got)
				}
				assertStoredOwner(t, h, chatID, ownerBefore)
			}
			if tc.completeBeforeDone {
				complete()
			}
			gate.release()
			if err := runtimeReceive(t, erased); err != nil {
				t.Fatal(err)
			}
			if !tc.completeBeforeDone {
				complete()
			}
			if err := h.chats.SetOwner(t.Context(), chatID, erasedUser, stale); !errors.Is(err, ErrBusy) {
				t.Fatalf("pre-erasure token after erasure = %v, want ErrBusy", err)
			}
			h.assertClaimRejections(chatID, erasedUser, "erasure", 3)
			assertNoStoredChatIdentity(t, h, erasedUser)
			if got := h.snapshot(otherChat).OwnerID; got != 0 {
				t.Fatalf("other shard still publishes erased owner %d", got)
			}

			h.advance(ProbeInterval)
			h.finishRequest(h.startReportedProbe(chatID), `{"type":"supergroup","title":"Updated title"}`)
			h.finishRequest(h.nextRequest("getChatMember"), runtimeProbeAdmin)
			h.finishRequest(h.nextRequest("getChatAdministrators"), runtimeProbeOwner)
			h.settleEffects(chatID)
			assertStoredOwner(t, h, chatID, erasedUser)
			if got := h.snapshot(chatID).OwnerID; got != erasedUser {
				t.Fatalf("freshly observed owner = %d, want %d", got, erasedUser)
			}
		})
	}
}

func TestRuntimeProbeAcrossErasureAppliesFactsWithoutChangingOwner(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, admins string
		before       ChatState
		eraseUser    int64
		wantOwner    int64
	}{
		{"recover missing record", runtimeProbeOwner, NoRecord, 555, 0},
		{"recover removed record", runtimeProbeOwner, Removed, 555, 0},
		{"preserve existing owner", runtimeProbeOwner, Active, 555, 81},
		{"still use an unrelated creator", runtimeProbeOwner, Active, 91, 555},
		{"successful query without creator clears owner", "[]", Active, 555, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const chatID = -100452
			h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
				if tc.before == NoRecord {
					return
				}
				h.seedActive(chatID, 1)
				if tc.before == Removed {
					h.exec("UPDATE chats SET state = 'removed' WHERE chat_id = ?", chatID)
				}
			}})
			h.finishRequest(h.startReportedProbe(chatID), `{"type":"supergroup","title":"Refreshed title"}`)
			h.finishRequest(h.nextRequest("getChatMember"), runtimeProbeAdmin)
			admins := h.nextRequest("getChatAdministrators")
			if err := h.chats.DeleteUser(t.Context(), tc.eraseUser); err != nil {
				t.Fatal(err)
			}
			h.finishRequest(admins, tc.admins)
			h.settleEffects(chatID)
			if got := h.snapshot(chatID); got.State != Active || got.OwnerID != tc.wantOwner || got.Title != "Refreshed title" {
				t.Fatalf("Probe completion = %+v, want Active with updated title and owner %d", got, tc.wantOwner)
			}
			assertStoredOwner(t, h, chatID, tc.wantOwner)
		})
	}
}

func TestRuntimeDeferredProbeChecksErasureWhenItRuns(t *testing.T) {
	t.Parallel()
	const id, erasedUser = -100448, 555
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
	h.finishRequest(h.startReportedProbe(id), `{"type":"supergroup","title":"Updated title"}`)
	h.finishRequest(h.nextRequest("getChatMember"), runtimeProbeAdmin)
	admins := h.nextRequest("getChatAdministrators")
	key := keyOf(id)
	if err := h.dispatcher.Submit(key, func(_ context.Context, sh *dispatch.Shard) { sh.Hold(key) }); err != nil {
		t.Fatal(err)
	}
	h.barrier(id)
	h.finishRequest(admins, runtimeProbeOwner)
	h.barrier(id)
	if err := h.chats.DeleteUser(t.Context(), erasedUser); err != nil {
		t.Fatal(err)
	}
	if err := h.dispatcher.Submit(key, func(_ context.Context, sh *dispatch.Shard) { sh.Release(key) }); err != nil {
		t.Fatal(err)
	}
	h.settleEffects(id)
	if got := h.snapshot(id); got.Title != "Updated title" || got.OwnerID != 81 {
		t.Fatalf("queued completion after erasure = %+v", got)
	}
	assertStoredOwner(t, h, id, 81)
}

func TestRuntimeOldClaimCannotOverwriteOwnerAfterTransferAndReentry(t *testing.T) {
	t.Parallel()
	const chatID, oldOwner, newOwner = -100453, 81, 555
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) { h.seedActive(chatID, 1) }})
	before := h.snapshot(chatID)
	verified := claimToken(t, h, oldOwner, chatID)

	h.apply(runtimeUpdate(chatID, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
	h.apply(runtimeUpdate(chatID, 3, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeAdministrator, true))
	h.finishRequest(h.nextRequest("getChat"), `{"type":"supergroup","title":"Initial title"}`)
	h.finishRequest(h.nextRequest("getChatMember"), runtimeProbeAdmin)
	h.finishRequest(h.nextRequest("getChatAdministrators"), runtimeProbeOwner)
	h.nextRequest("sendMessage").Done(sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}})
	h.settleEffects(chatID)
	if got := h.snapshot(chatID); got.OwnerID != newOwner || got.MemberEpoch == before.MemberEpoch {
		t.Fatalf("owner transfer or new membership missing: %+v", got)
	}
	if err := h.chats.SetOwner(t.Context(), chatID, oldOwner, verified); !errors.Is(err, ErrBusy) {
		t.Fatalf("claim from the prior membership returned %v, want ErrBusy", err)
	}
	h.assertClaimRejections(chatID, oldOwner, "membership_changed", 1)
	if got := h.snapshot(chatID).OwnerID; got != newOwner {
		t.Fatalf("stale claim replaced owner %d with %d", newOwner, got)
	}
	assertStoredOwner(t, h, chatID, newOwner)
}

func TestRuntimeOwnerIdempotencyChecksClaimTokenFirst(t *testing.T) {
	t.Parallel()
	const chatID, userID = -100454, 81
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) { h.seedActive(chatID, 1) }})
	stale := claimToken(t, h, userID, chatID)
	if err := h.chats.DeleteUser(t.Context(), userID); err != nil {
		t.Fatal(err)
	}
	fresh := claimToken(t, h, userID, chatID)
	if err := h.chats.SetOwner(t.Context(), chatID, userID, fresh); err != nil {
		t.Fatal(err)
	}
	mismatched := claimToken(t, h, userID+1, chatID)
	for _, tc := range []struct {
		name, reason string
		token        ClaimToken
		want         error
	}{
		{"expired", "erasure", stale, ErrBusy},
		{"different user", "user_mismatch", mismatched, ErrBusy},
		{"current", "", fresh, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := h.chats.SetOwner(t.Context(), chatID, userID, tc.token); !errors.Is(err, tc.want) {
				t.Fatalf("same-value claim returned %v, want %v", err, tc.want)
			}
			if tc.reason != "" {
				h.assertClaimRejections(chatID, userID, tc.reason, 1)
			}
			assertStoredOwner(t, h, chatID, userID)
		})
	}
	h.assertCounter(`yabot_chats_overload_total{source="owner_inbox"}`, 0)
}

func TestRuntimeClaimTokenCannotBeReusedForAnotherChat(t *testing.T) {
	t.Parallel()
	const first, userID = int64(-100605), 81
	h := newRuntimeHarness(t, runtimeOptions{shards: 2, deferChatsStart: true})
	second := runtimeChatOnShard(t, h, first+1, 1-runtimeShardOf(t, h, first))
	h.seedActive(first, 1)
	h.seedActive(second, 1)
	if err := h.chats.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { runtimeStop(t, h.chats.Stop) })
	if h.snapshot(first).MemberEpoch != h.snapshot(second).MemberEpoch {
		t.Fatal("expected equal initial epochs from distinct shards")
	}
	token := claimToken(t, h, userID, first)
	if err := h.chats.SetOwner(t.Context(), second, userID, token); !errors.Is(err, ErrBusy) {
		t.Fatalf("different chat accepted token with matching owner and epoch: %v", err)
	}
	h.assertClaimRejections(second, userID, "chat_mismatch", 1)
	assertStoredOwner(t, h, second, userID)
}

func (h *runtimeHarness) assertClaimRejections(chatID, userID int64, reason string, want uint64) {
	h.t.Helper()
	h.assertCounter(fmt.Sprintf(`yabot_chats_claim_rejections_total{reason=%q}`, reason), want)
	if n := h.logCount(slog.LevelInfo, "chat owner claim rejected", "chat_id", chatID, "user_id", userID, "reason", reason); uint64(n) != want {
		h.t.Fatalf("claim rejection logs for %s = %d, want %d", reason, n, want)
	}
}
