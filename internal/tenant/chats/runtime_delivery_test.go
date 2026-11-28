package chats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
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
	"github.com/INKCR0W/yabot/internal/tenant/settings"
)

type deliveredMessage struct {
	ChatID    int64                       `json:"chat_id"`
	Text      string                      `json:"text"`
	Mode      models.ParseMode            `json:"parse_mode"`
	Silent    bool                        `json:"disable_notification"`
	Ephemeral json.RawMessage             `json:"ephemeral_message_parameters"`
	Markup    models.InlineKeyboardMarkup `json:"reply_markup"`
}

type deliveryPanicError struct{}

func (deliveryPanicError) Error() string { return "injected sender result error" }

func (deliveryPanicError) Is(error) bool { panic("injected sender completion panic") }

const runtimeRetryLeaveEnvelope = `{"ok":false,"error_code":500,"description":"upstream unavailable"}`

func TestRuntimeGroupOnboardingDelivery(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		result  sender.Result
		english bool
		outcome string
		level   slog.Level
	}{
		{name: "success", result: sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}}, outcome: "ok"},
		{name: "permission failure", result: sender.Result{Outcome: telegram.Outcome{Class: telegram.Permission}}, outcome: "permission"},
		{name: "shutdown", result: sender.DroppedResult(sender.Shutdown, sender.ErrClosed), english: true, outcome: "shutdown"},
		{name: "busy", result: sender.DroppedResult(sender.Busy, sender.ErrBusy), outcome: "busy"},
		{name: "expiry keeps its own reason", result: sender.DroppedResult(sender.Expired, sender.ErrClosed), outcome: "expired"},
		{name: "completion panic", result: sender.DroppedResult(sender.Busy, deliveryPanicError{}), outcome: "fatal", level: slog.LevelError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const chatID int64 = -100781
			h := newRuntimeHarness(t, runtimeOptions{tickets: 1})
			if tc.english {
				_, err := h.settings.Patch(t.Context(), "general", chatID, settings.Actor{UserID: 70, Public: true},
					map[string]json.RawMessage{"language": json.RawMessage(`"en"`)}, `"0"`)
				if err != nil {
					t.Fatal(err)
				}
			}
			joinDeliveryChat(h, chatID, 1, models.ChatTypeSupergroup, false)
			finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, false)
			request := h.nextRequest("sendMessage")
			if !request.NoRetry {
				t.Fatal("onboarding message can retry")
			}
			message := captureDelivery(h, request, tc.result)
			assertDelivery(t, message, chatID, tc.english)
			h.settleEffects(chatID)
			h.assertNoRequest()
			h.assertCounter(fmt.Sprintf(`yabot_chats_effects_total{effect="onboarding",outcome=%q}`, tc.outcome), 1)
			if tc.outcome == "ok" {
				h.assertNoLog(slog.LevelInfo, "chat message was not sent", "chat_id", chatID, "operation", "onboarding")
			} else {
				logMessage := "chat message was not sent"
				if tc.level == slog.LevelError {
					logMessage = "chat message failed"
				}
				h.assertLog(tc.level, logMessage, "chat_id", chatID, "operation", "onboarding", "outcome", tc.outcome)
			}
			ctx, cancel := context.WithTimeout(t.Context(), runtimeWait)
			defer cancel()
			if err := h.setOwner(ctx, chatID, 72); err != nil {
				t.Fatalf("set owner after message completion: %v", err)
			}
			if got := h.snapshot(chatID).OwnerID; got != 72 {
				t.Fatalf("owner after message completion = %d, want 72", got)
			}
			h.barrier(chatID)
			if got := h.metrics.GetOrCreateGauge(`yabot_dispatch_tickets_inuse{shard="0"}`, nil).Get(); got != 0 {
				t.Fatalf("in-use tickets after completion = %v, want 0", got)
			}
			if got := h.metrics.GetOrCreateCounter(`yabot_dispatch_ticket_late_total{op="complete"}`).Get(); got != 0 {
				t.Fatalf("duplicate ticket completions = %v, want 0", got)
			}
		})
	}
}

func TestRuntimeOnboardingRecordsClosedSender(t *testing.T) {
	t.Parallel()
	const chatID int64 = -100786
	h := newRuntimeHarness(t, runtimeOptions{})
	joinDeliveryChat(h, chatID, 1, models.ChatTypeSupergroup, true)
	h.finishRequest(h.nextRequest("getChat"), deliveryProbeChat(chatID, models.ChatTypeSupergroup))
	h.finishRequest(h.nextRequest("getChatMember"), runtimeProbeAdmin)
	administrators := h.nextRequest("getChatAdministrators")
	h.sender.mu.Lock()
	h.sender.closed = true
	h.sender.mu.Unlock()
	h.finishRequest(administrators, runtimeProbeOwner)
	h.settleEffects(chatID)
	h.barrier(chatID)
	h.assertProbeTicket(0)
	h.assertNoRequest()
	h.assertCounter(`yabot_chats_effects_total{effect="onboarding",outcome="shutdown"}`, 1)
	h.assertLog(slog.LevelInfo, "chat message was not sent", "chat_id", chatID, "operation", "onboarding", "outcome", "shutdown")
}

func TestRuntimePendingOnboardingPreventsAnotherAttempt(t *testing.T) {
	t.Parallel()
	const chatID int64 = -100787
	h := newRuntimeHarness(t, runtimeOptions{})
	joinDeliveryChat(h, chatID, 1, models.ChatTypeSupergroup, true)
	finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, true)
	pending := h.nextRequest("sendMessage")
	previous := h.snapshot(chatID)
	h.advance(24 * time.Hour)
	joinDeliveryChat(h, chatID, 2, models.ChatTypeSupergroup, true)
	if current := h.snapshot(chatID); current.MemberEpoch == previous.MemberEpoch {
		t.Fatal("re-entry did not create a new membership epoch")
	}
	finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, true)
	h.settleEffects(chatID)
	h.assertNoRequest()
	h.assertProbeTicket(1)
	pending.Done(sender.Result{Outcome: telegram.Outcome{Class: telegram.Retry}})
	h.settleEffects(chatID)
	h.assertNoRequest()
	h.assertCounter(`yabot_chats_effects_total{effect="onboarding",outcome="retry"}`, 1)
	h.assertProbeTicket(0)
}

func TestRuntimeOnboardingSettlesLatestBlockedMembership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		outcome      telegram.Class
		laterState   ChatState
		laterChannel bool
		finishLatest bool
		wantGuide    bool
	}{
		{name: "permission failure sends blocked guide", outcome: telegram.Permission, wantGuide: true},
		{name: "success keeps cooldown", outcome: telegram.OK},
		{name: "departure cancels blocked guide", outcome: telegram.Permission, laterState: Removed},
		{name: "new epoch without a completed probe cancels blocked guide", outcome: telegram.Permission, laterState: Degraded},
		{name: "channel never receives blocked guide", outcome: telegram.Permission, laterChannel: true},
		{name: "newest blocked guide replaces previous one", outcome: telegram.Permission, laterState: Degraded, finishLatest: true, wantGuide: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const chatID int64 = -100791
			h := newRuntimeHarness(t, runtimeOptions{})
			joinDeliveryChat(h, chatID, 1, models.ChatTypeSupergroup, false)
			finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, false)
			firstGuide := h.nextRequest("sendMessage")
			firstEpoch := h.snapshot(chatID).MemberEpoch

			h.apply(runtimeUpdate(chatID, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
			joinDeliveryChat(h, chatID, 3, models.ChatTypeSupergroup, false)
			blockedEpoch := h.snapshot(chatID).MemberEpoch
			if blockedEpoch == firstEpoch {
				t.Fatal("re-entry did not create a new membership epoch")
			}
			finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, false)
			h.settleEffects(chatID)
			h.assertNoRequest()

			var latestProbe sender.Request
			switch tc.laterState {
			case Removed:
				h.apply(runtimeUpdate(chatID, 4, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
			case Degraded:
				update := runtimeUpdate(chatID, 4, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeAdministrator, false)
				update.MyChatMember.From.LanguageCode = "en-US"
				h.apply(update)
				if h.snapshot(chatID).MemberEpoch == blockedEpoch {
					t.Fatal("inferred re-entry did not supersede the blocked membership")
				}
				if tc.finishLatest {
					finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, false)
				} else {
					latestProbe = h.nextRequest("getChat")
				}
			}
			if tc.laterChannel {
				update := runtimeUpdate(chatID, 4, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, false)
				update.MyChatMember.Chat.Type = models.ChatTypeChannel
				h.apply(update)
				if got := h.snapshot(chatID); got.Type != models.ChatTypeChannel || got.MemberEpoch != blockedEpoch {
					t.Fatalf("channel update did not preserve the blocked membership: %+v", got)
				}
			}
			h.settleEffects(chatID)
			h.assertNoRequest()
			firstGuide.Done(sender.Result{Outcome: telegram.Outcome{Class: tc.outcome}})
			h.settleEffects(chatID)
			if tc.wantGuide {
				guide := h.nextRequest("sendMessage")
				message := captureDelivery(h, guide, sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}})
				assertDelivery(t, message, chatID, tc.finishLatest)
				h.settleEffects(chatID)
			}
			h.assertNoRequest()
			if latestProbe.Done != nil {
				latestProbe.Done(sender.DroppedResult(sender.Shutdown, sender.ErrClosed))
				h.nextRequest("sendMessage").Done(sender.DroppedResult(sender.Shutdown, sender.ErrClosed))
				h.settleEffects(chatID)
			}
			h.assertProbeTicket(0)
		})
	}
}

func TestRuntimeDeferredOnboardingResolvesLanguageWhenSent(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, before, after string
		changeAfterEnqueue  bool
		wantEnglish         bool
	}{
		{name: "group setting overrides actor", before: "zh-Hans", after: "en", wantEnglish: true},
		{name: "auto falls back to original actor", before: "en", after: "auto"},
		{name: "setting changes in sender queue", before: "zh-Hans", after: "en", changeAfterEnqueue: true, wantEnglish: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const id = -100797
			h := newRuntimeHarness(t, runtimeOptions{})
			patch := func(ifMatch, language string) string {
				t.Helper()
				result, err := h.settings.Patch(t.Context(), "general", id, settings.Actor{UserID: 70, Public: true},
					map[string]json.RawMessage{"language": json.RawMessage(fmt.Sprintf("%q", language))}, ifMatch)
				if err != nil {
					t.Fatal(err)
				}
				return result.ETag
			}
			etag := patch(`"0"`, tc.before)
			joinDeliveryChat(h, id, 1, models.ChatTypeSupergroup, false)
			finishDeliveryProbe(h, id, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, false)
			first := h.nextRequest("sendMessage")

			joinDeliveryChat(h, id, 2, models.ChatTypeSupergroup, false)
			finishDeliveryProbe(h, id, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, false)
			h.settleEffects(id)
			h.assertNoRequest()
			if !tc.changeAfterEnqueue {
				patch(etag, tc.after)
			}
			h.finishRequestEnvelope(first, `{"ok":false,"error_code":400,"description":"CHAT_ADMIN_REQUIRED"}`)
			deferred := h.nextRequest("sendMessage")
			if tc.changeAfterEnqueue {
				patch(etag, tc.after)
			}
			message := captureDelivery(h, deferred, sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}})
			assertDelivery(t, message, id, tc.wantEnglish)
			h.settleEffects(id)
			h.assertNoRequest()
			h.assertProbeTicket(0)
		})
	}
}

func TestRuntimeQueuedDowngradeUsesCurrentLanguage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, before, after, actor string
		text, button               string
	}{
		{"group setting overrides actor", "zh-Hans", "en", "zh-CN", "YaBot lost the Invite Users permission.", "Grant Invite Users"},
		{"auto falls back to actor", "en", "auto", "zh-CN", "YaBot 已失去邀请用户权限。", "授予邀请用户权限"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const id int64 = -100799
			h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
			patch := func(etag, language string) string {
				t.Helper()
				result, err := h.settings.Patch(t.Context(), "general", id, settings.Actor{UserID: 70, Public: true},
					map[string]json.RawMessage{"language": json.RawMessage(fmt.Sprintf("%q", language))}, etag)
				if err != nil {
					t.Fatal(err)
				}
				return result.ETag
			}
			etag := patch(`"0"`, tc.before)
			update := runtimeUpdate(id, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeMember, false)
			update.MyChatMember.From.LanguageCode = tc.actor
			h.apply(update)
			probe := h.nextRequest("getChat")
			downgrade := h.nextRequest("sendMessage")

			patch(etag, tc.after)
			message := captureDelivery(h, downgrade, sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}})
			assertDeliveryLinks(t, message, "https://t.me/ExampleGuardBot?startgroup&admin=invite_users")
			if !strings.Contains(message.Text, tc.text) || message.Markup.InlineKeyboard[0][0].Text != tc.button {
				t.Fatalf("downgrade was not rendered in the current language: %+v", message)
			}
			probe.Done(sender.DroppedResult(sender.Shutdown, sender.ErrClosed))
			h.settleEffects(id)
			h.assertNoRequest()
			h.assertProbeTicket(0)
		})
	}
}

func TestRuntimeDeliveryAndRejectedLeaveUseSDKMigration(t *testing.T) {
	t.Parallel()
	const id = -100798
	for _, effect := range []string{"onboarding", "downgrade", "leave_outcome"} {
		for _, tc := range []struct{ outcome, envelope string }{
			{"ok", ""},
			{"migrated", runtimeMigrationEnvelope},
			{"chat_gone", runtimeChatGoneEnvelope},
			{"forbidden", runtimeForbiddenEnvelope},
			{"retry", `{"ok":false,"error_code":500,"description":"upstream unavailable"}`},
		} {
			t.Run(effect+"/"+tc.outcome, func(t *testing.T) {
				t.Parallel()
				h := newRuntimeHarness(t, runtimeOptions{
					beforeStart: func(h *runtimeHarness) {
						if effect == "downgrade" {
							h.seedActive(id, 1)
						}
					},
					configure: func(d *Deps) { d.AcceptNewChats = effect != "leave_outcome" },
				})
				var request sender.Request
				switch effect {
				case "onboarding":
					joinDeliveryChat(h, id, 2, models.ChatTypeGroup, false)
					finishDeliveryProbe(h, id, models.ChatTypeGroup, models.ChatMemberTypeAdministrator, false)
					request = h.nextRequest("sendMessage")
				case "downgrade":
					h.apply(runtimeUpdate(id, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, false))
					probe := h.nextRequest("getChat")
					request = h.nextRequest("sendMessage")
					h.finishRequest(probe, deliveryProbeChat(id, models.ChatTypeGroup))
					finishDeliveryMembership(h, models.ChatMemberTypeAdministrator, false)
				case "leave_outcome":
					joinDeliveryChat(h, id, 2, models.ChatTypeGroup, false)
					request = h.nextRequest("leaveChat")
				}
				h.settleEffects(id)
				before, existed := h.chats.State(id)
				if tc.outcome == "ok" {
					if effect == "leave_outcome" {
						h.finishRequest(request, `true`)
					} else {
						h.finishRequest(request, `{"message_id":1,"date":1,"chat":{"id":-100798,"type":"group"}}`)
					}
				} else {
					h.finishRequestEnvelope(request, tc.envelope)
				}
				h.settleEffects(id)
				h.assertNoRequest()
				h.assertProbeTicket(0)
				wantCount := uint64(1)
				if effect == "leave_outcome" && tc.outcome == "ok" {
					wantCount = 0
				}
				h.assertCounter(fmt.Sprintf(`yabot_chats_effects_total{effect=%q,outcome=%q}`, effect, tc.outcome), wantCount)

				after, exists := h.chats.State(id)
				if tc.outcome != "migrated" {
					if exists != existed || after != before {
						t.Fatalf("%s unexpectedly changed chat state from %+v to %+v", tc.outcome, before, after)
					}
					return
				}
				if !exists || after.State != Removed || after.RemovedReason != RemovedMigrated ||
					after.MigratedTo != runtimeMigrationTarget || after.MemberEpoch != before.MemberEpoch ||
					after.PurgeAfter != h.now().Add(720*time.Hour).Unix() {
					t.Fatalf("migration from %s settled as %+v", effect, after)
				}
				if _, exists := h.chats.State(runtimeMigrationTarget); exists {
					t.Fatal("migration created a record for the destination")
				}
			})
		}
	}
}

func TestRuntimeIdleLeaveUsesSDKOutcome(t *testing.T) {
	t.Parallel()
	const chatID int64 = -100789
	for _, tc := range []struct {
		name, envelope string
		outcome        string
		level          slog.Level
		removed        RemovedReason
	}{
		{name: "left", envelope: `{"ok":true,"result":true}`, removed: RemovedByIdle},
		{name: "migrated", envelope: runtimeMigrationEnvelope, removed: RemovedMigrated},
		{name: "chat gone", envelope: runtimeChatGoneEnvelope, removed: RemovedByIdle},
		{name: "forbidden", envelope: runtimeForbiddenEnvelope, removed: RemovedByIdle},
		{name: "false result", envelope: `{"ok":true,"result":false}`, outcome: "fatal", level: slog.LevelError},
		{name: "retryable API failure", envelope: runtimeRetryLeaveEnvelope, outcome: "retry"},
		{name: "permission failure", envelope: `{"ok":false,"error_code":400,"description":"CHAT_ADMIN_REQUIRED"}`, outcome: "permission"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
				seedIdleChat(h, chatID, h.now().Add(-8*24*time.Hour).Unix(), false)
			}})
			old := h.snapshot(chatID)
			if n, err := h.janitor["chats_idle"].run(t.Context()); n != 1 || err != nil {
				t.Fatalf("idle maintenance accepted %d chats with error %v", n, err)
			}
			finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeMember, false)
			leave := h.nextRequest("leaveChat")
			if leave.ChatID != chatID || leave.Class != sender.Background || !leave.NoRetry {
				t.Fatalf("idle leave for chat %d scheduled as %v, NoRetry=%t", leave.ChatID, leave.Class, leave.NoRetry)
			}
			payload := h.finishRequestEnvelope(leave, tc.envelope)
			if got := string(payload["chat_id"]); got != "-100789" {
				t.Fatalf("leaveChat payload targeted %s, want %d", got, chatID)
			}
			h.settleEffects(chatID)
			h.assertProbeTicket(0)
			h.assertNoRequest()
			after := h.snapshot(chatID)
			if after.MemberEpoch != old.MemberEpoch {
				t.Fatalf("leaveChat changed member epoch from %d to %d", old.MemberEpoch, after.MemberEpoch)
			}
			if tc.removed != "" {
				var migratedTo int64
				if tc.removed == RemovedMigrated {
					migratedTo = runtimeMigrationTarget
				}
				if after.State != Removed || after.PurgeAfter != h.now().Unix() ||
					after.RemovedReason != tc.removed || after.MigratedTo != migratedTo {
					t.Fatalf("successful idle leave did not mark the chat for cleanup: %+v", after)
				}
				return
			}
			if after.State != Onboarding || after.PurgeAfter != old.PurgeAfter {
				t.Fatalf("failed idle leave changed membership or scheduled cleanup: %+v", after)
			}
			h.assertCounter(fmt.Sprintf(`yabot_chats_effects_total{effect="leave_outcome",outcome=%q}`, tc.outcome), 1)
			h.assertChatOperation(chatID, LeaveOutcome, tc.outcome, tc.level)
		})
	}
}

func TestRuntimeIdleLeaveAfterProbeChangesIdleState(t *testing.T) {
	t.Parallel()
	const chatID int64 = -100794
	for _, tc := range []struct {
		from, to         ChatState
		before, observed models.ChatMemberType
	}{
		{Onboarding, Degraded, models.ChatMemberTypeMember, models.ChatMemberTypeAdministrator},
		{Degraded, Onboarding, models.ChatMemberTypeAdministrator, models.ChatMemberTypeMember},
	} {
		t.Run(string(tc.from)+"_to_"+string(tc.to), func(t *testing.T) {
			t.Parallel()
			h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
				at := h.now().Add(-8 * 24 * time.Hour).Unix()
				h.exec(`INSERT INTO chats (chat_id, type, title, state, added_at, updated_at)
					VALUES (?, 'supergroup', 'Idle chat', ?, ?, ?)`, chatID, tc.from, at, at)
			}})
			h.apply(runtimeUpdate(chatID, 1, h.now().Unix(), models.ChatMemberTypeMember, tc.before, false))
			before := h.snapshot(chatID)
			if n, err := h.janitor["chats_idle"].run(t.Context()); n != 1 || err != nil {
				t.Fatalf("idle maintenance accepted %d chats with error %v", n, err)
			}
			finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, tc.observed, false)
			leave := h.nextRequest("leaveChat")
			if after := h.snapshot(chatID); after.State != tc.to || after.UpdatedAt != h.now().Unix() ||
				after.MemberEpoch != before.MemberEpoch || after.EverActive {
				t.Fatalf("unexpected chat after idle Probe: %+v", after)
			}
			payload := h.finishRequest(leave, "true")
			if got := string(payload["chat_id"]); got != fmt.Sprint(chatID) {
				t.Fatalf("leaveChat payload targeted %s, want %d", got, chatID)
			}
			h.settleEffects(chatID)
			if after := h.snapshot(chatID); after.State != Removed || after.RemovedReason != RemovedByIdle ||
				after.PurgeAfter != h.now().Unix() || after.MemberEpoch != before.MemberEpoch {
				t.Fatalf("idle leave did not schedule immediate cleanup: %+v", after)
			}
			h.assertProbeTicket(0)
			h.assertNoRequest()
		})
	}
}

func TestRuntimeIdleLeaveRetryExpeditesConfirmedRemoval(t *testing.T) {
	t.Parallel()
	const chatID int64 = -100795
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
		seedIdleChat(h, chatID, h.now().Add(-8*24*time.Hour).Unix(), false)
	}})
	if n, err := h.janitor["chats_idle"].run(t.Context()); n != 1 || err != nil {
		t.Fatalf("idle maintenance accepted %d chats with error %v", n, err)
	}
	finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeMember, false)
	leave := h.nextRequest("leaveChat")
	deliver := leave.Done
	var result sender.Result
	leave.Done = func(r sender.Result) { result = r }
	h.finishRequestStatus(leave, http.StatusInternalServerError, runtimeRetryLeaveEnvelope)
	if result.Outcome.Class != telegram.Retry {
		t.Fatalf("HTTP 500 leaveChat returned %v, want Retry", result.Outcome)
	}

	h.apply(runtimeUpdate(chatID, 2, h.now().Unix(), models.ChatMemberTypeMember, models.ChatMemberTypeLeft, false))
	removed, version := h.snapshot(chatID), h.chats.Version()
	if removed.State != Removed || removed.RemovedReason != RemovedLeft ||
		removed.PurgeAfter != h.now().Add(30*24*time.Hour).Unix() {
		t.Fatalf("left update did not commit the normal retention deadline: %+v", removed)
	}
	h.advance(time.Minute)
	deliver(result)
	h.settleEffects(chatID)

	want := removed
	want.PurgeAfter = h.now().Unix()
	if after := h.snapshot(chatID); after != want || h.chats.Version() != version {
		t.Fatalf("late idle result changed more than the cleanup deadline or failed to expedite it: got %+v, want %+v", after, want)
	}
	var removedAt, purgeAfter int64
	var reason RemovedReason
	err := h.store.Read().QueryRowContext(t.Context(), `SELECT removed_at, removed_reason, purge_after
		FROM chats WHERE chat_id = ?`, chatID).Scan(&removedAt, &reason, &purgeAfter)
	if err != nil {
		t.Fatal(err)
	}
	if removedAt != want.RemovedAt || reason != want.RemovedReason || purgeAfter != want.PurgeAfter {
		t.Fatalf("stored removal = %d, %q, %d, want %d, %q, %d",
			removedAt, reason, purgeAfter, want.RemovedAt, want.RemovedReason, want.PurgeAfter)
	}
	h.assertProbeTicket(0)
	h.assertNoRequest()
}

func TestRuntimeIdleLeaveCompletionKeepsNewMembership(t *testing.T) {
	t.Parallel()
	const chatID int64 = -100790
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
		seedIdleChat(h, chatID, h.now().Add(-8*24*time.Hour).Unix(), false)
	}})
	before := h.snapshot(chatID)
	if n, err := h.janitor["chats_idle"].run(t.Context()); n != 1 || err != nil {
		t.Fatalf("idle maintenance accepted %d chats with error %v", n, err)
	}
	finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeMember, false)
	leave := h.nextRequest("leaveChat")
	deliver := leave.Done
	var result sender.Result
	leave.Done = func(r sender.Result) { result = r }
	h.finishRequestEnvelope(leave, runtimeMigrationEnvelope)
	if result.Outcome.Class != telegram.Migrated {
		t.Fatalf("leaveChat returned %v, want Migrated", result.Outcome)
	}
	h.apply(runtimeUpdate(chatID, 2, h.now().Unix(), models.ChatMemberTypeMember, models.ChatMemberTypeLeft, false))
	joinDeliveryChat(h, chatID, 3, models.ChatTypeSupergroup, true)
	newProbe := h.nextRequest("getChat")
	joined := h.snapshot(chatID)
	if joined.MemberEpoch == before.MemberEpoch || joined.State != Active || joined.PurgeAfter != 0 {
		t.Fatalf("new membership was not committed: %+v", joined)
	}
	deliver(result)
	h.settleEffects(chatID)
	if got := h.snapshot(chatID); got != joined {
		t.Fatalf("older leave migration changed new membership: %+v", got)
	}
	h.assertCounter(`yabot_chats_events_total{outcome="stale_completion"}`, 1)
	h.assertProbeTicket(1)
	newProbe.Done(sender.DroppedResult(sender.Shutdown, sender.ErrClosed))
	h.nextRequest("sendMessage").Done(sender.DroppedResult(sender.Shutdown, sender.ErrClosed))
	h.settleEffects(chatID)
	h.assertProbeTicket(0)
}

func TestRuntimeIdleLeaveSentDuringPromotionKeepsNormalRetention(t *testing.T) {
	t.Parallel()
	const chatID int64 = -100798
	const retention = 30 * 24 * time.Hour
	for _, tc := range []struct {
		name             string
		leftBeforeResult bool
	}{
		{name: "result before left"},
		{name: "left before result", leftBeforeResult: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
				seedIdleChat(h, chatID, h.now().Add(-8*24*time.Hour).Unix(), false)
			}})
			if n, err := h.janitor["chats_idle"].run(t.Context()); n != 1 || err != nil {
				t.Fatalf("idle maintenance accepted %d chats with error %v", n, err)
			}
			finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeMember, false)
			leave := h.nextRequest("leaveChat")
			before := h.snapshot(chatID)
			unblock := blockMaintenanceWriter(t, h)
			promotion := h.update(runtimeUpdate(chatID, 2, h.now().Unix(), models.ChatMemberTypeMember, models.ChatMemberTypeAdministrator, true))
			h.barrier(chatID)
			h.assertProbeTicket(2)
			if got := h.snapshot(chatID); got != before || h.water(chatID) != before.Watermark {
				t.Fatalf("promotion became visible before commit: %+v", got)
			}

			deliver := leave.Done
			var result sender.Result
			leave.Done = func(r sender.Result) { result = r }
			payload := h.finishRequest(leave, "true")
			if got := string(payload["chat_id"]); got != fmt.Sprint(chatID) || result.Outcome.Class != telegram.OK {
				t.Fatalf("leaveChat targeted %s and returned %v, want %d and OK", got, result.Outcome, chatID)
			}
			unblock()
			if err := promotion.Wait(runtimeWait); err != nil {
				t.Fatalf("commit promotion: %v", err)
			}
			h.nextRequest("getChat").Done(sender.DroppedResult(sender.Shutdown, sender.ErrClosed))
			h.settleEffects(chatID)
			promoted := h.snapshot(chatID)
			if promoted.State != Active || !promoted.EverActive || promoted.MemberEpoch != before.MemberEpoch || promoted.Watermark.ID != 2 {
				t.Fatalf("unexpected membership after promotion: %+v", promoted)
			}

			left := runtimeUpdate(chatID, 3, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false)
			if tc.leftBeforeResult {
				h.apply(left)
			}
			deliver(result)
			h.settleEffects(chatID)
			if !tc.leftBeforeResult {
				if got := h.snapshot(chatID); got != promoted {
					t.Fatalf("idle leave result superseded promotion: %+v", got)
				}
				h.apply(left)
			}
			removed := h.snapshot(chatID)
			if removed.State != Removed || removed.RemovedReason != RemovedLeft || !removed.EverActive ||
				removed.MemberEpoch != before.MemberEpoch || removed.PurgeAfter != h.now().Add(retention).Unix() {
				t.Fatalf("left update did not retain the promoted chat for 30 days: %+v", removed)
			}
			var purgeAfter int64
			if err := h.store.Read().QueryRowContext(t.Context(), `SELECT purge_after FROM chats WHERE chat_id = ?`, chatID).Scan(&purgeAfter); err != nil {
				t.Fatal(err)
			}
			if purgeAfter != removed.PurgeAfter {
				t.Fatalf("stored cleanup deadline = %d, want %d", purgeAfter, removed.PurgeAfter)
			}
			h.assertProbeTicket(0)
			h.assertNoRequest()
		})
	}
}

func TestRuntimeIdleLeaveRechecksFactsObservedByLaterProbes(t *testing.T) {
	t.Parallel()
	const chatID = -100799
	for _, tc := range []struct {
		name string
		sent bool
		want ChatState
	}{
		{"receipt after promotion", true, Active},
		{"receipt after promotion and downgrade", true, Degraded},
		{"send after promotion", false, Active},
		{"send after promotion and downgrade", false, Degraded},
		{"send after Probe confirms departure", false, Removed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
				seedIdleChat(h, chatID, h.now().Add(-8*24*time.Hour).Unix(), false)
			}})
			if n, err := h.janitor["chats_idle"].run(t.Context()); n != 1 || err != nil {
				t.Fatalf("idle maintenance accepted %d chats with error %v", n, err)
			}
			finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeMember, false)
			leave := h.nextRequest("leaveChat")
			idle := h.snapshot(chatID)
			deliver := leave.Done
			var result sender.Result
			if tc.sent {
				leave.Done = func(r sender.Result) { result = r }
				h.finishRequest(leave, "true")
				if result.Outcome.Class != telegram.OK {
					t.Fatalf("leaveChat returned %v, want OK", result.Outcome)
				}
			}

			report := func() {
				h.advance(ProbeInterval)
				h.chats.ReportPermissionLost(chatID)
				h.tick()
			}
			report()
			if tc.want == Removed {
				h.finishRequest(h.nextRequest("getChat"), deliveryProbeChat(chatID, models.ChatTypeSupergroup))
				h.finishRequest(h.nextRequest("getChatMember"), fmt.Sprintf(`{"status":"left","user":{"id":%d}}`, runtimeBotID))
			} else {
				finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, true)
				h.settleEffects(chatID)
				if got := h.snapshot(chatID); got.State != Active || !got.EverActive {
					t.Fatalf("reported Probe did not commit promotion: %+v", got)
				}
				if tc.want == Degraded {
					report()
					finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, false)
					h.nextRequest("sendMessage").Done(sender.DroppedResult(sender.Shutdown, sender.ErrClosed))
				}
			}
			h.settleEffects(chatID)
			observed, version := h.snapshot(chatID), h.chats.Version()
			if observed.State != tc.want || observed.EverActive != (tc.want != Removed) ||
				observed.MemberEpoch != idle.MemberEpoch || observed.Watermark != idle.Watermark {
				t.Fatalf("unexpected membership after reported Probe: %+v", observed)
			}
			if tc.sent {
				deliver(result)
			} else {
				finishCanceledIdleLeave(t, leave)
			}
			h.settleEffects(chatID)
			want := observed
			if want.State == Removed {
				want.PurgeAfter = h.now().Unix()
			}
			if got := h.snapshot(chatID); got != want || h.chats.Version() != version {
				t.Fatalf("old leave changed the verified chat: got %+v, want %+v", got, want)
			}
			if want.State != Removed {
				h.apply(runtimeUpdate(chatID, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
				removed := h.snapshot(chatID)
				if removed.State != Removed || removed.RemovedReason != RemovedLeft || !removed.EverActive ||
					removed.PurgeAfter != h.now().Add(30*24*time.Hour).Unix() {
					t.Fatalf("left update did not apply normal retention: %+v", removed)
				}
				var purgeAfter int64
				if err := h.store.Read().QueryRowContext(t.Context(), `SELECT purge_after FROM chats WHERE chat_id = ?`, chatID).Scan(&purgeAfter); err != nil || purgeAfter != removed.PurgeAfter {
					t.Fatalf("stored cleanup deadline = %d (%v), want %d", purgeAfter, err, removed.PurgeAfter)
				}
			}
			h.assertProbeTicket(0)
			h.assertNoRequest()
		})
	}
}

func TestRuntimeQueuedIdleLeaveCancelsAfterMembershipUpdate(t *testing.T) {
	t.Parallel()
	const chatID int64 = -100792
	const idleAfter = 168 * time.Hour
	for _, tc := range []struct {
		name           string
		initialState   ChatState
		old, next      models.ChatMemberType
		oldInvite      bool
		invite         bool
		rejoin         bool
		wantState      ChatState
		wantEverActive bool
		queuedMethods  []string
	}{
		{
			name: "gained invite permission", old: models.ChatMemberTypeMember, next: models.ChatMemberTypeAdministrator,
			invite: true, wantState: Active, wantEverActive: true, queuedMethods: []string{"getChat"},
		},
		{
			name: "left and rejoined before execution", old: models.ChatMemberTypeLeft, next: models.ChatMemberTypeMember,
			rejoin: true, wantState: Onboarding, queuedMethods: []string{"getChat", "sendMessage"},
		},
		{
			name: "onboarding to degraded", old: models.ChatMemberTypeMember, next: models.ChatMemberTypeAdministrator,
			wantState: Degraded, queuedMethods: []string{"getChat"},
		},
		{
			name: "degraded to onboarding", initialState: Degraded,
			old: models.ChatMemberTypeAdministrator, next: models.ChatMemberTypeMember,
			wantState: Onboarding, wantEverActive: true, queuedMethods: []string{"getChat"},
		},
		{
			name: "watermark advanced without changing membership", old: models.ChatMemberTypeMember, next: models.ChatMemberTypeMember,
			wantState: Onboarding,
		},
		{
			name: "learned of previous invite permission", old: models.ChatMemberTypeAdministrator, next: models.ChatMemberTypeMember,
			oldInvite: true, wantState: Onboarding, wantEverActive: true, queuedMethods: []string{"sendMessage"},
		},
		{
			name: "already left", old: models.ChatMemberTypeMember, next: models.ChatMemberTypeLeft,
			wantState: Removed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
				seedIdleChat(h, chatID, h.now().Add(-idleAfter).Unix(), false)
				if tc.initialState == Degraded {
					h.exec(`UPDATE chats SET state = ? WHERE chat_id = ?`, Degraded, chatID)
				}
			}})
			if n, err := h.janitor["chats_idle"].run(t.Context()); n != 1 || err != nil {
				t.Fatalf("idle maintenance accepted %d chats with error %v", n, err)
			}
			probeMember := models.ChatMemberTypeMember
			if tc.initialState == Degraded {
				probeMember = models.ChatMemberTypeAdministrator
			}
			finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, probeMember, false)
			leave := h.nextRequest("leaveChat")
			queued := h.snapshot(chatID)
			wantInitial := Onboarding
			if tc.initialState != NoRecord {
				wantInitial = tc.initialState
			}
			if queued.State != wantInitial || queued.EverActive {
				t.Fatalf("unexpected membership when idle leave was queued: %+v", queued)
			}
			updateID := int64(2)
			if tc.rejoin {
				h.apply(runtimeUpdate(chatID, updateID, h.now().Unix(), models.ChatMemberTypeMember, models.ChatMemberTypeLeft, false))
				updateID++
			}
			update := runtimeUpdate(chatID, updateID, h.now().Unix(), tc.old, tc.next, tc.invite)
			update.MyChatMember.OldChatMember = runtimeMember(tc.old, tc.oldInvite)
			h.apply(update)
			for _, method := range tc.queuedMethods {
				h.nextRequest(method).Done(sender.DroppedResult(sender.Shutdown, sender.ErrClosed))
			}
			h.settleEffects(chatID)
			before, version := h.snapshot(chatID), h.chats.Version()
			if before.State != tc.wantState || before.EverActive != tc.wantEverActive || (queued.MemberEpoch != before.MemberEpoch) != tc.rejoin ||
				before.Watermark.ID != updateID || h.water(chatID) != before.Watermark {
				t.Fatalf("unexpected eligibility before executing queued leave: %+v", before)
			}
			if tc.old == models.ChatMemberTypeMember && tc.next == models.ChatMemberTypeMember {
				want := queued
				want.Watermark = before.Watermark
				if before != want {
					t.Fatalf("same-membership update changed more than the watermark: got %+v, want %+v", before, want)
				}
			}

			finishCanceledIdleLeave(t, leave)
			h.settleEffects(chatID)
			want := before
			if before.State == Removed {
				want.PurgeAfter = h.now().Unix()
			}
			if got := h.snapshot(chatID); got != want || h.chats.Version() != version {
				t.Errorf("chat after canceled leave = %+v, want %+v with view version %d", got, want, version)
			}
			h.assertProbeTicket(0)
			h.assertNoRequest()
			h.assertCounter(`yabot_chats_effects_total{effect="leave_outcome",outcome="canceled"}`, 1)
			h.assertLog(slog.LevelInfo, "idle chat leave canceled before execution", "chat_id", chatID)
		})
	}
}

func finishCanceledIdleLeave(t *testing.T, leave sender.Request) {
	t.Helper()
	calls := 0
	transport := runtimeTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if err := r.Body.Close(); err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"ok":true,"result":true}`)),
			Header:     make(http.Header),
		}, nil
	})
	upstream, err := bot.New("12345:test", bot.WithSkipGetMe(), bot.WithServerURL("https://telegram.test"),
		bot.WithHTTPClient(0, &http.Client{Transport: transport}))
	if err != nil {
		t.Fatal(err)
	}
	err = leave.Do(t.Context(), upstream)
	leave.Done(sender.Result{Outcome: telegram.Classify(err), Err: err, Attempts: 1})
	if calls != 0 {
		t.Errorf("queued leave made %d Telegram calls, want zero", calls)
	}
}

func TestRuntimeIdleLeaveObservesOwnerErasure(t *testing.T) {
	t.Parallel()
	const chatID, previousOwner, probedOwner = -100799, 81, 71
	for _, tc := range []struct {
		name            string
		eraseUser       int64
		probePending    bool
		sendBeforeErase bool
		erasureRejected bool
		wantCalls       int
		wantOwner       int64
		wantState       ChatState
	}{
		{name: "queued leave", eraseUser: probedOwner, wantState: Onboarding},
		{name: "queued leave when erasure submission fails", eraseUser: probedOwner, erasureRejected: true, wantState: Onboarding},
		{name: "in-flight leave result", eraseUser: probedOwner, sendBeforeErase: true, wantCalls: 1, wantState: Onboarding},
		{name: "pending probe carries a different owner", eraseUser: previousOwner, probePending: true, wantOwner: probedOwner, wantState: Onboarding},
		{name: "pending owner erased despite rejected submission", eraseUser: probedOwner, probePending: true, erasureRejected: true, wantState: Onboarding},
		{name: "unrelated user", eraseUser: 99, wantCalls: 1, wantOwner: probedOwner, wantState: Removed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			delayed := &runtimeDelayedStoreDone{callbacks: make(chan func(), 1)}
			delayed.open.Store(!tc.probePending)
			var rejectErasure atomic.Bool
			h := newRuntimeHarness(t, runtimeOptions{
				beforeStart: func(h *runtimeHarness) {
					at := h.now().Add(-8 * 24 * time.Hour).Unix()
					h.exec(`INSERT INTO chats (chat_id, type, title, state, added_at, updated_at, owner_id)
						VALUES (?, 'supergroup', 'Idle chat', 'onboarding', ?, ?, ?)`, chatID, at, at, previousOwner)
				},
				configure: func(d *Deps) {
					delayed.Store = d.Store
					d.Store = maintenanceStore{Store: delayed, write: func(op store.Op, done func(error)) error {
						if rejectErasure.Swap(false) {
							return store.ErrBusy
						}
						return delayed.Write(op, done)
					}}
				},
			})
			defer delayed.resume(h)
			version, initial := h.chats.Version(), h.snapshot(chatID)
			if n, err := h.janitor["chats_idle"].run(t.Context()); n != 1 || err != nil {
				t.Fatalf("idle maintenance accepted %d chats with error %v", n, err)
			}
			finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeMember, false)

			calls := 0
			upstream := successfulRuntimeLeaveBot(t, &calls)
			var leave sender.Request
			var leaveErr error
			var completeProbe func()
			if tc.probePending {
				completeProbe = runtimeReceive(t, delayed.callbacks)
				t.Cleanup(completeProbe)
				delayed.open.Store(true)
				assertStoredOwner(t, h, chatID, probedOwner)
				if got := h.snapshot(chatID); got != initial {
					t.Fatalf("probe published before its completion ran: %+v", got)
				}
			} else {
				leave = h.nextRequest("leaveChat")
				assertStoredOwner(t, h, chatID, probedOwner)
			}
			if tc.sendBeforeErase {
				if leaveErr = leave.Do(t.Context(), upstream); leaveErr != nil {
					t.Fatalf("leave before erasure: %v", leaveErr)
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), runtimeWait)
			defer cancel()
			rejectErasure.Store(tc.erasureRejected)
			err := h.chats.DeleteUser(ctx, tc.eraseUser)
			if tc.erasureRejected && !errors.Is(err, store.ErrBusy) || !tc.erasureRejected && err != nil {
				t.Fatalf("erase owner: %v", err)
			}
			if tc.probePending {
				completeProbe()
				leave = h.nextRequest("leaveChat")
			}
			if !tc.sendBeforeErase {
				leaveErr = leave.Do(t.Context(), upstream)
			}
			leave.Done(sender.Result{Outcome: telegram.Classify(leaveErr), Err: leaveErr, Attempts: 1})
			h.settleEffects(chatID)
			if calls != tc.wantCalls {
				t.Errorf("leaveChat made %d Telegram calls, want %d", calls, tc.wantCalls)
			}
			if tc.wantState != Removed {
				after := h.snapshot(chatID)
				if after.PurgeAfter != 0 || after.MemberEpoch != initial.MemberEpoch || after.Watermark != initial.Watermark || h.chats.Version() != version {
					t.Errorf("erasure or stale leave changed membership, watermark or view version: %+v", after)
				}
				h.assertCounter(`yabot_chats_events_total{outcome="stale_completion"}`, 1)
			}
			if got := h.snapshot(chatID); got.State != tc.wantState || got.OwnerID != tc.wantOwner {
				t.Errorf("snapshot after leave = %+v, want state %s and owner %d", got, tc.wantState, tc.wantOwner)
			}
			var state ChatState
			err = h.store.Read().QueryRowContext(ctx, `SELECT state FROM chats WHERE chat_id = ?`, chatID).Scan(&state)
			if err != nil || state != tc.wantState {
				t.Errorf("stored state = %q with error %v, want %q", state, err, tc.wantState)
			}
			if tc.erasureRejected {
				assertStoredOwner(t, h, chatID, probedOwner)
				if err := h.chats.DeleteUser(ctx, tc.eraseUser); err != nil {
					t.Fatalf("retry erasure: %v", err)
				}
			}
			assertStoredOwner(t, h, chatID, tc.wantOwner)
			h.assertProbeTicket(0)
			h.assertNoRequest()
		})
	}
}

func TestRuntimeIdleLeaveObservesStoredOwnerErasure(t *testing.T) {
	t.Parallel()
	const queuedID, sentID, waitingID, userID = -100800, -100802, -100804, 71
	h := newRuntimeHarness(t, runtimeOptions{
		tickets: 2,
		beforeStart: func(h *runtimeHarness) {
			at := h.now().Add(-8 * 24 * time.Hour).Unix()
			h.exec(`WITH ids(id) AS (VALUES (?), (?), (?))
				INSERT INTO chats (chat_id, type, title, state, added_at, updated_at, owner_id)
				SELECT id, 'supergroup', 'Idle chat', 'onboarding', ?, ?, ? FROM ids`, queuedID, sentID, waitingID, at, at, userID)
		},
		configure: func(d *Deps) { d.Store = &rejectFirstChatWrite{Store: d.Store, reject: store.ErrBusy} },
	})
	ctx, cancel := context.WithTimeout(t.Context(), runtimeWait)
	defer cancel()
	if err := h.chats.DeleteUser(ctx, userID); !errors.Is(err, store.ErrBusy) {
		t.Fatalf("first erasure returned %v, want store.ErrBusy", err)
	}
	for _, id := range []int64{queuedID, sentID, waitingID} {
		if got := h.snapshot(id).OwnerID; got != 0 {
			t.Fatalf("snapshot owner for %d after first erasure = %d, want 0", id, got)
		}
	}
	submitIdle := func(id int64) {
		if err := h.dispatcher.Submit(keyOf(id), func(_ context.Context, sh *dispatch.Shard) {
			h.chats.enqueueInternal(sh, keyOf(id), ProbeTriggered{Trigger: ProbeIdle})
		}); err != nil {
			t.Fatalf("submit idle check for %d: %v", id, err)
		}
	}
	finishIdleProbe := func(id int64) sender.Request {
		h.finishRequest(h.nextRequest("getChat"), `{"type":"supergroup","title":"Idle chat"}`)
		h.finishRequest(h.nextRequest("getChatMember"), runtimeProbeMember)
		h.finishRequest(h.nextRequest("getChatAdministrators"), "[]")
		leave := h.nextRequest("leaveChat")
		if leave.ChatID != id {
			t.Fatalf("idle leave targeted %d, want %d", leave.ChatID, id)
		}
		return leave
	}
	submitIdle(queuedID)
	queued := finishIdleProbe(queuedID)
	submitIdle(sentID)
	sent := finishIdleProbe(sentID)
	calls := 0
	upstream := successfulRuntimeLeaveBot(t, &calls)
	if err := sent.Do(ctx, upstream); err != nil {
		t.Fatalf("leave before stored erasure: %v", err)
	}
	submitIdle(waitingID)
	h.barrier(waitingID)
	h.assertNoRequest()
	h.assertCounter(`yabot_chats_overload_total{source="sender_ticket"}`, 1)
	if got := storedChatIdentities(t, h, userID); got != 3 {
		t.Fatalf("stored owners before retry = %d, want 3", got)
	}
	if err := h.chats.DeleteUser(ctx, userID); err != nil {
		t.Fatalf("retry erasure: %v", err)
	}
	assertNoStoredChatIdentity(t, h, userID)
	err := queued.Do(ctx, upstream)
	queued.Done(sender.Result{Outcome: telegram.Classify(err), Err: err, Attempts: 1})
	sent.Done(sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}, Attempts: 1})
	h.settleEffects(sentID)
	if calls != 1 {
		t.Errorf("queued leave reached Telegram after stored erasure, total calls = %d", calls)
	}
	for _, id := range []int64{queuedID, sentID} {
		if got := h.snapshot(id); got.State != Onboarding || got.PurgeAfter != 0 {
			t.Errorf("old idle leave changed chat %d: %+v", id, got)
		}
	}
	h.advance(time.Second)
	h.tick()
	fresh := finishIdleProbe(waitingID)
	err = fresh.Do(ctx, upstream)
	fresh.Done(sender.Result{Outcome: telegram.Classify(err), Err: err, Attempts: 1})
	h.settleEffects(waitingID)
	if got := h.snapshot(waitingID); calls != 2 || err != nil || got.State != Removed {
		t.Fatalf("fresh idle Probe did not authorize a leave: calls = %d, error = %v, snapshot = %+v", calls, err, got)
	}
	h.assertProbeTicket(0)
	h.assertNoRequest()
}

func successfulRuntimeLeaveBot(t *testing.T, calls *int) *bot.Bot {
	t.Helper()
	transport := runtimeTransport(func(r *http.Request) (*http.Response, error) {
		(*calls)++
		if err := r.Body.Close(); err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(`{"ok":true,"result":true}`))}, nil
	})
	upstream, err := bot.New("12345:test", bot.WithSkipGetMe(), bot.WithServerURL("https://telegram.test"),
		bot.WithHTTPClient(0, &http.Client{Transport: transport}))
	if err != nil {
		t.Fatal(err)
	}
	return upstream
}

func TestRuntimeGeneralLanguageSchemaRendersRegisteredTranslations(t *testing.T) {
	t.Parallel()
	h := newRuntimeHarness(t, runtimeOptions{})
	if group, ok := h.settings.PermGroup("general"); !ok || group != "general" {
		t.Fatalf("permission group = %q, %t", group, ok)
	}
	for _, tc := range []struct {
		lang, title, field string
		labels             []string
	}{
		{"en", "General", "Chat language", []string{"Automatic", "Simplified Chinese", "English"}},
		{"zh-Hans", "通用", "群语言", []string{"自动", "简体中文", "英语"}},
	} {
		schema := h.settings.Render(-100780, tc.lang)
		if len(schema.Specs) != 1 {
			t.Fatalf("schema specs = %+v", schema.Specs)
		}
		spec := schema.Specs[0]
		if spec.Feature != "general" || spec.Title != tc.title || string(spec.Values["language"]) != `"auto"` ||
			!slices.Equal(spec.Groups, []settings.RenderedGroup{{ID: "general", Title: tc.title}}) || len(spec.Fields) != 1 {
			t.Fatalf("general schema for %s = %+v", tc.lang, spec)
		}
		field := spec.Fields[0]
		options := []settings.RenderedOption{
			{Value: "auto", Label: tc.labels[0]}, {Value: "zh-Hans", Label: tc.labels[1]}, {Value: "en", Label: tc.labels[2]},
		}
		if field.Key != "language" || field.Group != "general" || field.Type != settings.FieldEnum || field.Label != tc.field ||
			string(field.Default) != `"auto"` || !slices.Equal(field.Options, options) {
			t.Fatalf("language field for %s = %+v", tc.lang, field)
		}
	}
}

func TestRuntimeOnboardingUsesKnownGuardAndCurrentInviteRights(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		join   models.ChatMemberType
		invite bool
		phrase string
	}{
		{"unknown assignment", models.ChatMemberTypeMember, false, "确认负责处理入群申请的机器人"},
		{"confirmed unassigned", models.ChatMemberTypeAdministrator, true, "尚未被设为本群"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const chatID int64 = -100785
			h := newRuntimeHarness(t, runtimeOptions{})
			h.apply(runtimeUpdate(chatID, 1, h.now().Unix(), models.ChatMemberTypeLeft, tc.join, tc.invite))
			h.finishRequest(h.nextRequest("getChat"), `{"id":-100785,"type":"supergroup","title":"Initial title"}`)
			finishDeliveryMembership(h, tc.join, tc.invite)
			guide := captureDelivery(h, h.nextRequest("sendMessage"), sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}})
			if !strings.Contains(guide.Text, tc.phrase) || guide.Ephemeral != nil || !guide.Silent {
				t.Fatalf("message for %s = %+v", tc.join, guide)
			}
			settingsURL := "https://t.me/ExampleGuardBot?startapp=c-100785"
			if tc.invite {
				if !strings.Contains(guide.Text, "已获得邀请用户权限") {
					t.Fatalf("message does not confirm Invite Users: %q", guide.Text)
				}
				assertDeliveryLinks(t, guide, settingsURL)
			} else {
				assertDeliveryLinks(t, guide, settingsURL, "https://t.me/ExampleGuardBot?startgroup&admin=invite_users")
			}
			h.settleEffects(chatID)
			h.assertNoRequest()
		})
	}
}

func TestRuntimeOnboardingTriggerSources(t *testing.T) {
	t.Parallel()
	const chatID int64 = -100791
	for _, tc := range []struct {
		name      string
		before    ChatState
		report    bool
		old       models.ChatMemberType
		wantGuide bool
	}{
		{name: "missing row with old member", old: models.ChatMemberTypeMember, wantGuide: true},
		{name: "removed row with old administrator", before: Removed, old: models.ChatMemberTypeAdministrator, wantGuide: true},
		{name: "Probe discovers a chat", report: true, wantGuide: true},
		{name: "Probe recovers a removed chat", before: Removed, report: true, wantGuide: true},
		{name: "inferred same-state re-entry", before: Active, old: models.ChatMemberTypeLeft, wantGuide: true},
		{name: "permission upgrade", before: Degraded, old: models.ChatMemberTypeAdministrator},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
				if tc.before != NoRecord {
					h.seedActive(chatID, 1)
					if tc.before == Degraded {
						h.exec(`UPDATE chats SET state = 'degraded', bot_rights = 0 WHERE chat_id = ?`, chatID)
					}
				}
			}})
			if tc.before == Removed {
				h.apply(runtimeUpdate(chatID, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
			}
			if tc.report {
				h.chats.ReportUnknown(chatID)
				h.tick()
			} else {
				h.apply(runtimeUpdate(chatID, 3, h.now().Unix(), tc.old, models.ChatMemberTypeAdministrator, true))
			}
			finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, true)
			if tc.wantGuide {
				message := captureDelivery(h, h.nextRequest("sendMessage"), sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}})
				assertDeliveryLinks(t, message, fmt.Sprintf("https://t.me/ExampleGuardBot?startapp=c%d", chatID))
				intro := "YaBot 已加入群聊"
				if tc.report {
					intro = "YaBot has joined this chat"
				}
				if !strings.Contains(message.Text, intro) || !message.Silent || message.Ephemeral != nil {
					t.Fatalf("onboarding from %s: %+v", tc.name, message)
				}
			}
			h.settleEffects(chatID)
			h.assertNoRequest()
		})
	}
}

func TestRuntimeOnboardingDiscardsGuardAndJoinRequestAfterReentry(t *testing.T) {
	t.Parallel()
	const chatID int64 = -100792
	h := newRuntimeHarness(t, runtimeOptions{})
	joinDeliveryChat(h, chatID, 1, models.ChatTypeSupergroup, false)
	finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, false)
	first := captureDelivery(h, h.nextRequest("sendMessage"), sender.Result{Outcome: telegram.Outcome{Class: telegram.Permission}})
	assertDelivery(t, first, chatID, false)
	h.settleEffects(chatID)
	joinDeliveryChat(h, chatID, 2, models.ChatTypeSupergroup, false)
	h.nextRequest("getChat").Done(sender.Result{Outcome: telegram.Outcome{Class: telegram.Retry}})
	second := captureDelivery(h, h.nextRequest("sendMessage"), sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}})
	if !strings.Contains(second.Text, "确认负责处理入群申请的机器人") || strings.Contains(second.Text, "直接加入本群") {
		t.Fatalf("re-entry message reused stale guard or join approval facts: %s", second.Text)
	}
	if snapshot := h.snapshot(chatID); snapshot.GuardAssigned || snapshot.GuardKnown || snapshot.JoinByRequest {
		t.Fatalf("re-entry kept stale guard or join approval facts: %+v", snapshot)
	}
	h.settleEffects(chatID)
	h.assertNoRequest()
}

func TestRuntimeOnboardingCooldownCarriesAcrossClearAndReentry(t *testing.T) {
	t.Parallel()
	const chatID int64 = -100782
	h := newRuntimeHarness(t, runtimeOptions{})
	for i, tc := range []struct {
		advance time.Duration
		message bool
	}{
		{0, true},
		{24*time.Hour - time.Nanosecond, false},
		{time.Nanosecond, true},
	} {
		h.advance(tc.advance)
		joinDeliveryChat(h, chatID, int64(i+1)*2, models.ChatTypeSupergroup, false)
		finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, false)
		if tc.message {
			message := captureDelivery(h, h.nextRequest("sendMessage"), sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}})
			assertDelivery(t, message, chatID, false)
		}
		h.settleEffects(chatID)
		h.assertNoRequest()
		if i == 0 {
			h.apply(runtimeUpdate(chatID, 3, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
			removed := h.snapshot(chatID)
			if err := h.dispatcher.Submit(keyOf(chatID), func(_ context.Context, sh *dispatch.Shard) {
				h.chats.enqueueInternal(sh, keyOf(chatID), ClearFinished{RemovedAt: removed.RemovedAt, MemberEpoch: removed.MemberEpoch})
			}); err != nil {
				t.Fatal(err)
			}
			h.settleEffects(chatID)
			if _, exists := h.chats.State(chatID); exists {
				t.Fatal("chat was not cleared")
			}
		}
	}
}

func TestRuntimeOnboardingCooldownStartsAtSuccessfulSend(t *testing.T) {
	t.Parallel()
	const chatID int64 = -100799
	const queueDelay = 2 * time.Minute
	const shardDelay = time.Minute
	h := newRuntimeHarness(t, runtimeOptions{})
	joinDeliveryChat(h, chatID, 1, models.ChatTypeSupergroup, false)
	finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, false)
	firstGuide := h.nextRequest("sendMessage")
	entered, unblock := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	t.Cleanup(release)
	if err := h.dispatcher.Submit(keyOf(chatID), func(ctx context.Context, _ *dispatch.Shard) {
		close(entered)
		select {
		case <-unblock:
		case <-ctx.Done():
		}
	}); err != nil {
		t.Fatal(err)
	}
	runtimeReceive(t, entered)
	h.advance(queueDelay)
	assertDelivery(t, captureDelivery(h, firstGuide, sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}}), chatID, false)
	h.advance(shardDelay)
	release()
	h.settleEffects(chatID)

	for i, tc := range []struct {
		advance time.Duration
		message bool
	}{
		{24*time.Hour - queueDelay - shardDelay, false},
		{queueDelay - time.Nanosecond, false},
		{time.Nanosecond, true},
	} {
		h.advance(tc.advance)
		joinDeliveryChat(h, chatID, int64(i+2), models.ChatTypeSupergroup, false)
		finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, false)
		if tc.message {
			guide := h.nextRequest("sendMessage")
			assertDelivery(t, captureDelivery(h, guide, sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}}), chatID, false)
		}
		h.settleEffects(chatID)
		h.assertNoRequest()
	}
	h.assertCounter(`yabot_chats_effects_total{effect="onboarding",outcome="ok"}`, 2)
	h.assertProbeTicket(0)
}

func TestRuntimeOnboardingPermissionFailureDoesNotStartCooldown(t *testing.T) {
	t.Parallel()
	const chatID int64 = -100788
	h := newRuntimeHarness(t, runtimeOptions{})
	for i, class := range []telegram.Class{telegram.Permission, telegram.Retry} {
		joinDeliveryChat(h, chatID, int64(i+1), models.ChatTypeSupergroup, false)
		finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, false)
		h.nextRequest("sendMessage").Done(sender.Result{Outcome: telegram.Outcome{Class: class}})
		h.settleEffects(chatID)
	}
	joinDeliveryChat(h, chatID, 3, models.ChatTypeSupergroup, false)
	finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, false)
	h.settleEffects(chatID)
	h.assertNoRequest()
	h.assertCounter(`yabot_chats_effects_total{effect="onboarding",outcome="permission"}`, 1)
	h.assertCounter(`yabot_chats_effects_total{effect="onboarding",outcome="retry"}`, 1)
}

func TestRuntimeOnboardingCooldownCapacityPreservesExistingEntries(t *testing.T) {
	t.Parallel()
	const lastChatID, nextChatID = -onboardingCooldownLimit, -onboardingCooldownLimit - 1
	h := newRuntimeHarness(t, runtimeOptions{})
	if err := h.dispatcher.Submit(keyOf(nextChatID), func(_ context.Context, sh *dispatch.Shard) {
		cooldowns := &h.chats.local(sh).onboarding
		now := h.now()
		cooldowns.reserve(-1)
		cooldowns.finish(-1, false, now)
		for id := int64(-2); id >= lastChatID; id-- {
			cooldowns.reserve(id)
			cooldowns.finish(id, false, now.Add(-time.Minute))
		}
	}); err != nil {
		t.Fatal(err)
	}
	h.barrier(nextChatID)
	for i, id := range []int64{nextChatID, -1} {
		joinDeliveryChat(h, id, int64(i+1), models.ChatTypeSupergroup, true)
		finishDeliveryProbe(h, id, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, true)
		h.settleEffects(id)
		h.assertNoRequest()
	}
	h.assertCounter(`yabot_chats_overload_total{source="onboarding_cooldown"}`, 1)
	h.assertLog(slog.LevelWarn, "chat onboarding cooldown is full", "chat_id", nextChatID)
	h.advance(24*time.Hour - time.Minute)
	joinDeliveryChat(h, nextChatID, 3, models.ChatTypeSupergroup, true)
	finishDeliveryProbe(h, nextChatID, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, true)
	h.nextRequest("sendMessage").Done(sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}})
	h.settleEffects(nextChatID)
	h.assertNoRequest()
}

func TestRuntimeChannelRecordsLifecycleWithoutSendingMessages(t *testing.T) {
	t.Parallel()
	const chatID int64 = -100783
	h := newRuntimeHarness(t, runtimeOptions{})
	joinDeliveryChat(h, chatID, 1, models.ChatTypeChannel, true)
	finishDeliveryProbe(h, chatID, models.ChatTypeChannel, models.ChatMemberTypeAdministrator, true)
	h.settleEffects(chatID)
	if got := h.snapshot(chatID); got.State != Active || got.Type != models.ChatTypeChannel || got.OwnerID != 71 {
		t.Fatalf("channel after onboarding = %+v", got)
	}
	h.assertNoRequest()
	downgrade := runtimeUpdate(chatID, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeMember, false)
	downgrade.MyChatMember.Chat.Type = models.ChatTypeChannel
	h.apply(downgrade)
	finishDeliveryProbe(h, chatID, models.ChatTypeChannel, models.ChatMemberTypeMember, false)
	h.settleEffects(chatID)
	if got := h.snapshot(chatID); got.State != Onboarding || got.Type != models.ChatTypeChannel {
		t.Fatalf("channel after downgrade = %+v", got)
	}
	h.assertNoRequest()
}

func TestRuntimeGroupDowngradeSendsInvitePermissionLink(t *testing.T) {
	t.Parallel()
	const chatID int64 = -100784
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) { h.seedActive(chatID, 1) }})
	for i, tc := range []struct {
		status models.ChatMemberType
		want   ChatState
	}{
		{models.ChatMemberTypeMember, Onboarding},
		{models.ChatMemberTypeAdministrator, Degraded},
	} {
		if i != 0 {
			h.advance(24 * time.Hour)
			h.apply(runtimeUpdate(chatID, 3, h.now().Unix(), models.ChatMemberTypeMember, models.ChatMemberTypeAdministrator, true))
			finishDeliveryProbe(h, chatID, models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, true)
			h.settleEffects(chatID)
		}
		h.apply(runtimeUpdate(chatID, int64(2+2*i), h.now().Unix(), models.ChatMemberTypeAdministrator, tc.status, false))
		probe := h.nextRequest("getChat")
		request := h.nextRequest("sendMessage")
		if !request.NoRetry {
			t.Fatal("downgrade notice can be retried by the sender")
		}
		guide := captureDelivery(h, request, sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}})
		if guide.Ephemeral != nil || guide.Mode != models.ParseModeHTML || !guide.Silent ||
			!strings.Contains(guide.Text, "已失去邀请用户权限") || !strings.Contains(guide.Text, "选择当前群") {
			t.Fatalf("downgrade guide = %+v", guide)
		}
		assertDeliveryLinks(t, guide, "https://t.me/ExampleGuardBot?startgroup&admin=invite_users")
		h.finishRequest(probe, deliveryProbeChat(chatID, models.ChatTypeSupergroup))
		finishDeliveryMembership(h, tc.status, false)
		h.settleEffects(chatID)
		if got := h.snapshot(chatID).State; got != tc.want {
			t.Fatalf("chat state = %s, want %s", got, tc.want)
		}
		h.assertNoRequest()
	}
}

func joinDeliveryChat(h *runtimeHarness, id, updateID int64, kind models.ChatType, invite bool) {
	h.t.Helper()
	update := runtimeUpdate(id, updateID, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeAdministrator, invite)
	update.MyChatMember.Chat.Type = kind
	h.apply(update)
}

func deliveryProbeChat(id int64, kind models.ChatType) string {
	return fmt.Sprintf(`{"id":%d,"type":%q,"title":"Initial title","join_by_request":true,"guard_bot":{"id":%d,"is_bot":true}}`, id, kind, runtimeBotID)
}

func finishDeliveryProbe(h *runtimeHarness, id int64, kind models.ChatType, membership models.ChatMemberType, invite bool) {
	h.t.Helper()
	h.finishRequest(h.nextRequest("getChat"), deliveryProbeChat(id, kind))
	finishDeliveryMembership(h, membership, invite)
}

func finishDeliveryMembership(h *runtimeHarness, membership models.ChatMemberType, invite bool) {
	h.t.Helper()
	h.finishRequest(h.nextRequest("getChatMember"), fmt.Sprintf(`{"status":%q,"user":{"id":%d,"is_bot":true},"can_invite_users":%t}`,
		membership, runtimeBotID, invite))
	h.finishRequest(h.nextRequest("getChatAdministrators"), `[{"status":"creator","user":{"id":71}}]`)
}

func captureDelivery(h *runtimeHarness, request sender.Request, result sender.Result) deliveredMessage {
	h.t.Helper()
	if request.Class != sender.Background {
		h.t.Fatalf("sendMessage class = %v, want %v", request.Class, sender.Background)
	}
	finish := request.Done
	request.Done = func(sender.Result) { finish(result) }
	fields := h.finishRequest(request, `{"message_id":1,"date":1,"chat":{"id":-100,"type":"supergroup"}}`)
	raw, err := json.Marshal(fields)
	if err != nil {
		h.t.Fatal(err)
	}
	var message deliveredMessage
	if err := json.Unmarshal(raw, &message); err != nil {
		h.t.Fatal(err)
	}
	return message
}

func assertDelivery(t *testing.T, message deliveredMessage, chatID int64, english bool) {
	t.Helper()
	if message.ChatID != chatID || message.Mode != models.ParseModeHTML {
		t.Fatalf("message chat and parse mode = %d, %s", message.ChatID, message.Mode)
	}
	if !message.Silent || message.Ephemeral != nil {
		t.Fatalf("delivery was not a silent group message: %+v", message)
	}
	phrases := []string{"<b>YaBot 已加入群聊。", "已被设为本群", "直接加入本群", "请授予 YaBot", "选择当前群"}
	if english {
		phrases = []string{"<b>YaBot has joined this chat.", "is assigned as", "requires approval to join directly", "Grant YaBot", "Select this chat"}
	}
	for _, phrase := range phrases {
		if !strings.Contains(message.Text, phrase) {
			t.Fatalf("onboarding text %q does not contain %q", message.Text, phrase)
		}
	}
	assertDeliveryLinks(t, message, fmt.Sprintf("https://t.me/ExampleGuardBot?startapp=c%d", chatID),
		"https://t.me/ExampleGuardBot?startgroup&admin=invite_users")
}

func assertDeliveryLinks(t *testing.T, message deliveredMessage, urls ...string) {
	t.Helper()
	if len(message.Markup.InlineKeyboard) != len(urls) {
		t.Fatalf("message keyboard = %+v, want %d rows", message.Markup, len(urls))
	}
	for i, expected := range urls {
		row := message.Markup.InlineKeyboard[i]
		if len(row) != 1 || row[0].Text == "" || row[0].URL != expected || row[0].WebApp != nil || row[0].LoginURL != nil {
			t.Fatalf("keyboard row = %+v, want URL button to %q", row, expected)
		}
	}
}
