package chats_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
	"github.com/INKCR0W/yabot/internal/tenant/chats"
)

func matrixStates() []chats.ChatState {
	return []chats.ChatState{chats.NoRecord, chats.Onboarding, chats.Active, chats.Degraded, chats.Removed}
}

func matrixPresent(state chats.ChatState) bool {
	return state == chats.Onboarding || state == chats.Active || state == chats.Degraded
}

func matrixState(status chats.ChatState, now time.Time) chats.State {
	watermark := dispatch.Order{Date: now.Unix() - 1, ID: 50}
	state := chats.State{Record: chats.Record{ChatID: -123}, Version: 12}
	if status == chats.NoRecord {
		state.MemoryWatermark = watermark
		return state
	}
	state.MemberEpoch = 25
	state.OwnerRevision = 27
	state.Record.Type = models.ChatTypeSupergroup
	state.Record.Title = "original title"
	state.Record.State = status
	state.Record.OwnerID = 17
	state.Record.AddedBy = 18
	state.Record.UpdatedAt = now.Add(-10 * 24 * time.Hour).Unix()
	state.Record.AddedAt = state.Record.UpdatedAt - 1
	state.Record.Watermark = watermark
	switch status {
	case chats.Active:
		state.Record.BotRights = chats.InviteUsers
		state.Record.EverActive = true
	case chats.Degraded:
		state.Record.BotRights = chats.ManageChat
	case chats.Removed:
		state.Record.RemovedReason = chats.RemovedKicked
		state.Record.RemovedAt = state.Record.UpdatedAt
		state.Record.PurgeAfter = now.Add(20 * 24 * time.Hour).Unix()
	}
	return state
}

func matrixPolicy() chats.Policy {
	return chats.Policy{AcceptNewChats: true, RemovedRetention: 30 * 24 * time.Hour, IdleLeaveAfter: 7 * 24 * time.Hour}
}

func matrixEffects[T chats.Effect](decision chats.Decision) []T {
	var effects []T
	for _, effect := range decision.Effects {
		if value, ok := effect.(T); ok {
			effects = append(effects, value)
		}
	}
	return effects
}

func matrixPersist(t *testing.T, decision chats.Decision, want bool) {
	t.Helper()
	persists := matrixEffects[chats.Persist](decision)
	if !want {
		if len(persists) != 0 {
			t.Fatalf("unexpected persistence: %+v", persists)
		}
		return
	}
	if len(persists) != 1 || persists[0].Record != decision.Next.Record || decision.Effects[0] != persists[0] {
		t.Fatalf("first effect must persist the proposed record: effects=%+v record=%+v", decision.Effects, decision.Next.Record)
	}
}

func TestMemberUpdateStateMatrix(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	policy := matrixPolicy()
	policy.RemovedRetention = 168 * time.Hour
	cases := []struct {
		name   string
		member models.ChatMember
		state  chats.ChatState
		rights chats.BotRights
		reason chats.RemovedReason
	}{
		{"admin_with_invite", models.ChatMember{Type: models.ChatMemberTypeAdministrator, Administrator: &models.ChatMemberAdministrator{
			CanManageChat: true, CanDeleteMessages: true, CanRestrictMembers: true, CanInviteUsers: true,
			CanPinMessages: true, CanPromoteMembers: true, CanChangeInfo: true,
		}}, chats.Active, chats.ManageChat | chats.DeleteMessages | chats.RestrictMembers | chats.InviteUsers | chats.PinMessages | chats.PromoteMembers | chats.ChangeInfo, ""},
		{"admin_without_invite", models.ChatMember{Type: models.ChatMemberTypeAdministrator, Administrator: &models.ChatMemberAdministrator{CanDeleteMessages: true}}, chats.Degraded, chats.DeleteMessages, ""},
		{"member", models.ChatMember{Type: models.ChatMemberTypeMember, Member: &models.ChatMemberMember{}}, chats.Onboarding, 0, ""},
		{"restricted_in_chat", models.ChatMember{Type: models.ChatMemberTypeRestricted, Restricted: &models.ChatMemberRestricted{IsMember: true, CanInviteUsers: true, CanPinMessages: true, CanChangeInfo: true}}, chats.Onboarding, chats.InviteUsers | chats.PinMessages | chats.ChangeInfo, ""},
		{"restricted_out_of_chat", models.ChatMember{Type: models.ChatMemberTypeRestricted, Restricted: &models.ChatMemberRestricted{CanInviteUsers: true}}, chats.Removed, 0, chats.RemovedLeft},
		{"left", models.ChatMember{Type: models.ChatMemberTypeLeft, Left: &models.ChatMemberLeft{}}, chats.Removed, 0, chats.RemovedLeft},
		{"kicked", models.ChatMember{Type: models.ChatMemberTypeBanned, Banned: &models.ChatMemberBanned{}}, chats.Removed, 0, chats.RemovedKicked},
	}
	for _, from := range matrixStates() {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("from_%q/%s", from, tc.name), func(t *testing.T) {
				before := matrixState(from, now)
				old := models.ChatMember{Type: models.ChatMemberTypeLeft}
				switch from {
				case chats.Onboarding:
					old = models.ChatMember{Type: models.ChatMemberTypeMember}
				case chats.Active, chats.Degraded:
					old = models.ChatMember{Type: models.ChatMemberTypeAdministrator, Administrator: &models.ChatMemberAdministrator{CanInviteUsers: from == chats.Active}}
				}
				update := models.Update{ID: 51, MyChatMember: &models.ChatMemberUpdated{
					Chat: models.Chat{ID: before.Record.ChatID, Type: models.ChatTypeGroup, Title: "current title"},
					From: models.User{ID: 97, LanguageCode: "zh-Hans"}, Date: int(now.Unix()), OldChatMember: old, NewChatMember: tc.member,
				}}
				event, ok := chats.MemberUpdateOf(&update)
				if !ok {
					t.Fatal("group membership update was rejected")
				}
				decision := chats.Step(before, event, now, policy)
				got := decision.Next.Record
				wantOrder := dispatch.Order{Date: now.Unix(), ID: update.ID}
				if decision.Stale {
					t.Fatal("new update was classified as stale")
				}
				if from == chats.NoRecord && tc.state == chats.Removed {
					matrixPersist(t, decision, false)
					if got != before.Record || decision.Next.MemoryWatermark != wantOrder || decision.Next.Version != before.Version+1 || decision.NewMemberEpoch ||
						len(decision.Effects) != 1 || decision.Effects[0] != (chats.UpdateWatermark{Order: wantOrder}) {
						t.Fatalf("departure for an unknown chat must only advance the memory watermark: %+v", decision)
					}
					return
				}
				matrixPersist(t, decision, true)
				if got.State != tc.state || got.Watermark != wantOrder || got.BotRights != tc.rights ||
					got.Type != update.MyChatMember.Chat.Type || got.Title != update.MyChatMember.Chat.Title {
					t.Fatalf("incorrect state, Telegram metadata, rights, or watermark: %+v", got)
				}
				wantEverActive := from == chats.Active || from == chats.Degraded || tc.state == chats.Active
				if got.EverActive != wantEverActive {
					t.Fatalf("ever_active = %v, want %v", got.EverActive, wantEverActive)
				}
				joined := !matrixPresent(from) && matrixPresent(tc.state)
				if decision.NewMemberEpoch != joined || decision.Next.MemberEpoch != before.MemberEpoch {
					t.Fatalf("epoch proposal = %v, epoch = %d, want proposal %v and current epoch %d", decision.NewMemberEpoch, decision.Next.MemberEpoch, joined, before.MemberEpoch)
				}
				if joined {
					if got.OwnerID != 0 || got.AddedBy != 97 || got.AddedAt != now.Unix() ||
						got.RemovedAt != 0 || got.RemovedReason != "" || got.MigratedTo != 0 || got.PurgeAfter != 0 {
						t.Fatalf("joining failed to set membership provenance and clear previous removal and owner: %+v", got)
					}
				} else if got.OwnerID != before.Record.OwnerID || got.AddedBy != before.Record.AddedBy || got.AddedAt != before.Record.AddedAt {
					t.Fatalf("existing member provenance changed: %+v", got)
				}
				if tc.state == chats.Removed {
					wantRemovedAt, wantReason, wantPurgeAfter := now.Unix(), tc.reason, now.Add(policy.RemovedRetention).Unix()
					if from == chats.Removed {
						wantRemovedAt, wantReason, wantPurgeAfter = before.Record.RemovedAt, before.Record.RemovedReason, before.Record.PurgeAfter
					}
					if got.RemovedAt != wantRemovedAt || got.RemovedReason != wantReason || got.PurgeAfter != wantPurgeAfter {
						t.Fatalf("incorrect removal provenance or retention: %+v", got)
					}
				}
				wantUpdatedAt, wantVersion := before.Record.UpdatedAt, before.Version+1
				if from != tc.state {
					wantUpdatedAt = now.Unix()
				}
				if got.UpdatedAt != wantUpdatedAt || decision.Next.Version != wantVersion {
					t.Fatalf("updated_at=%d version=%d, want %d and %d", got.UpdatedAt, decision.Next.Version, wantUpdatedAt, wantVersion)
				}
				probe := matrixEffects[chats.StartProbe](decision)
				if matrixPresent(tc.state) && from != tc.state {
					wantProbe := chats.StartProbe{Trigger: chats.ProbeTransition}
					if joined {
						wantProbe.Actions = chats.FollowUp{Onboarding: true, LanguageCode: "zh-Hans"}
					}
					if len(probe) != 1 || probe[0] != wantProbe {
						t.Fatalf("probe=%+v, want %+v", probe, wantProbe)
					}
				} else if len(probe) != 0 {
					t.Fatalf("unexpected probe: %+v", probe)
				}
				wantDowngrade := from == chats.Active && (tc.state == chats.Degraded || tc.state == chats.Onboarding)
				if n := len(matrixEffects[chats.DowngradeMessage](decision)); (n == 1) != wantDowngrade || n > 1 {
					t.Fatalf("unexpected downgrade messages: %+v", decision.Effects)
				}
				lifecycleForbid[chats.OnboardingMessage](t, decision)
			})
		}
	}
}

func TestProbeCompletionStateMatrix(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	policy := matrixPolicy()
	okResult := sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}}
	active := chats.Membership{Status: models.ChatMemberTypeAdministrator, Rights: chats.InviteUsers}
	degraded := chats.Membership{Status: models.ChatMemberTypeAdministrator, Rights: chats.ManageChat}
	member := chats.Membership{Status: models.ChatMemberTypeMember}
	type successCase struct {
		name   string
		member chats.Membership
		state  chats.ChatState
		reason chats.RemovedReason
		owner  int64
	}
	results := []successCase{
		{"admin_with_invite", active, chats.Active, "", 87},
		{"admin_without_invite", degraded, chats.Degraded, "", 87},
		{"member", member, chats.Onboarding, "", 87},
		{"no_creator", member, chats.Onboarding, "", 0},
		{"restricted_in_chat", chats.Membership{Status: models.ChatMemberTypeRestricted, IsMember: true}, chats.Onboarding, "", 87},
		{"restricted_out_of_chat", chats.Membership{Status: models.ChatMemberTypeRestricted}, chats.Removed, chats.RemovedLeft, 0},
		{"left", chats.Membership{Status: models.ChatMemberTypeLeft}, chats.Removed, chats.RemovedLeft, 0},
		{"kicked", chats.Membership{Status: models.ChatMemberTypeBanned}, chats.Removed, chats.RemovedKicked, 0},
	}
	type failureCase struct {
		name         string
		result       sender.Result
		removed      chats.RemovedReason
		expectsError bool
	}
	failures := []failureCase{
		{"forbidden", sender.Result{Outcome: telegram.Outcome{Class: telegram.Forbidden}}, chats.RemovedForbidden, false},
		{"chat_gone", sender.Result{Outcome: telegram.Outcome{Class: telegram.Done, Reason: telegram.ChatGone}}, chats.RemovedChatGone, false},
		{"migrated", sender.Result{Outcome: telegram.Outcome{Class: telegram.Migrated, MigrateTo: -456}}, chats.RemovedMigrated, false},
		{"retry", sender.Result{Outcome: telegram.Outcome{Class: telegram.Retry, RetryAfter: time.Second}}, "", false},
		{"dropped", sender.DroppedResult(sender.Busy, errors.New("queue full")), "", false},
		{"permission", sender.Result{Outcome: telegram.Outcome{Class: telegram.Permission}}, "", false},
		{"fatal", sender.Result{Outcome: telegram.Outcome{Class: telegram.Fatal}}, "", true},
		{"unknown", sender.Result{Outcome: telegram.Outcome{Class: telegram.Unknown}, Err: errors.New("unknown result")}, "", true},
		{"conflict", sender.Result{Outcome: telegram.Outcome{Class: telegram.Conflict}}, "", true},
		{"unauthorized", sender.Result{Outcome: telegram.Outcome{Class: telegram.Unauthorized}}, "", true},
		{"other_done_reason", sender.Result{Outcome: telegram.Outcome{Class: telegram.Done, Reason: telegram.RequestMissing}}, "", true},
	}
	for _, from := range matrixStates() {
		before := matrixState(from, now)
		inFlight, request := chats.BeginProbe(before, chats.StartProbe{Trigger: chats.ProbeUnknown}, now.Add(-time.Minute))
		chat := chats.ProbeChat{Result: okResult, Type: inFlight.Record.Type, Title: inFlight.Record.Title}
		if from == chats.NoRecord {
			chat.Type, chat.Title = models.ChatTypeSupergroup, "discovered title"
		}
		for _, tc := range results {
			t.Run(fmt.Sprintf("from_%q/ok/%s", from, tc.name), func(t *testing.T) {
				event := chats.ProbeFinished{Request: request, Chat: chat, Member: chats.ProbeMember{Result: okResult, Membership: tc.member}}
				if matrixPresent(tc.state) {
					event.Admins = chats.ProbeAdmins{Result: okResult, CreatorID: tc.owner}
				}
				decision := chats.Step(inFlight, event, now, policy)
				if tc.state == chats.Removed {
					matrixProbeRemoval(t, decision, before, tc.reason, 0, now, policy)
				} else {
					matrixPersist(t, decision, true)
					got := decision.Next.Record
					joined := !matrixPresent(from)
					if got.State != tc.state || got.OwnerID != tc.owner || got.BotRights != tc.member.Rights ||
						decision.NewMemberEpoch != joined || got.EverActive != (from == chats.Active || tc.state == chats.Active) {
						t.Fatalf("incorrect probe transition: %+v", decision)
					}
					if joined && (got.AddedBy != 0 || got.RemovedAt != 0 || got.RemovedReason != "" || got.PurgeAfter != 0 || got.MigratedTo != 0) {
						t.Fatalf("probe recovery retained membership provenance: %+v", got)
					}
					if got.Title != chat.Title || got.Type != chat.Type {
						t.Fatalf("getChat metadata missing: %+v", got)
					}
				}
				matrixProbeOutcome(t, decision, before, okResult, false)
				wantDowngrade := from == chats.Active && (tc.state == chats.Degraded || tc.state == chats.Onboarding)
				if n := len(matrixEffects[chats.DowngradeMessage](decision)); (n == 1) != wantDowngrade || n > 1 {
					t.Fatalf("unexpected downgrade messages: %+v", decision.Effects)
				}
			})
		}
		for _, tc := range failures {
			for _, stage := range []string{"get_chat", "get_member", "get_administrators"} {
				t.Run(fmt.Sprintf("from_%q/%s/%s", from, stage, tc.name), func(t *testing.T) {
					event := chats.ProbeFinished{Request: request, Chat: chat}
					switch stage {
					case "get_chat":
						event.Chat = chats.ProbeChat{Result: tc.result}
					case "get_member":
						event.Member.Result = tc.result
					case "get_administrators":
						event.Member = chats.ProbeMember{Result: okResult, Membership: active}
						event.Admins.Result = tc.result
					}
					decision := chats.Step(inFlight, event, now, policy)
					if tc.removed == "" {
						matrixPersist(t, decision, false)
						if decision.Next.Record != before.Record || decision.NewMemberEpoch {
							t.Fatalf("failed probe altered the record: %+v", decision)
						}
					} else {
						matrixProbeRemoval(t, decision, before, tc.removed, tc.result.Outcome.MigrateTo, now, policy)
					}
					matrixProbeOutcome(t, decision, before, tc.result, tc.expectsError)
				})
			}
		}
	}
}

func matrixProbeRemoval(t *testing.T, decision chats.Decision, before chats.State, reason chats.RemovedReason, migrated int64, now time.Time, policy chats.Policy) {
	t.Helper()
	if !matrixPresent(before.Record.State) && reason != chats.RemovedMigrated {
		matrixPersist(t, decision, false)
		if decision.Next.Record != before.Record {
			t.Fatalf("probe changed the existing removal or created a departure record: %+v", decision)
		}
		return
	}
	matrixPersist(t, decision, true)
	got := decision.Next.Record
	wantRemovedAt, wantPurgeAfter := now.Unix(), now.Add(policy.RemovedRetention).Unix()
	if before.Record.State == chats.Removed {
		wantRemovedAt, wantPurgeAfter = before.Record.RemovedAt, before.Record.PurgeAfter
	}
	if got.State != chats.Removed || got.OwnerID != before.Record.OwnerID || got.BotRights != 0 ||
		got.RemovedReason != reason || got.MigratedTo != migrated || got.RemovedAt != wantRemovedAt ||
		got.PurgeAfter != wantPurgeAfter || decision.NewMemberEpoch {
		t.Fatalf("incorrect removal from Probe: %+v", decision)
	}
}

func matrixProbeOutcome(t *testing.T, decision chats.Decision, before chats.State, result sender.Result, reportError bool) {
	t.Helper()
	want := chats.Metric{Kind: chats.ProbeOutcome, Trigger: chats.ProbeUnknown, Outcome: result.Outcome, Dropped: result.Dropped, Error: reportError}
	metrics := matrixEffects[chats.Metric](decision)
	if len(metrics) != 1 || metrics[0] != want {
		t.Fatalf("probe metrics=%+v, want %+v", metrics, want)
	}
	if decision.Next.Probe.InFlight.Trigger != "" || decision.Next.Version != before.Version ||
		len(matrixEffects[chats.StartProbe](decision)) != 0 || len(matrixEffects[chats.OnboardingMessage](decision)) != 0 {
		t.Fatalf("unexpected follow-up or in-flight marker: %+v", decision)
	}
	watermark := before.Record.Watermark
	if before.Record.State == chats.NoRecord {
		watermark = before.MemoryWatermark
	}
	if decision.Next.Record.State == chats.NoRecord {
		if decision.Next.MemoryWatermark != watermark {
			t.Fatalf("absent record lost memory watermark: %+v", decision.Next)
		}
	} else if decision.Next.Record.Watermark != watermark {
		t.Fatalf("probe changed update watermark: %+v", decision.Next)
	}
}

func TestReporterAndClaimStateMatrix(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	policy := matrixPolicy()
	for _, from := range matrixStates() {
		before := matrixState(from, now)
		t.Run(fmt.Sprintf("from_%q", from), func(t *testing.T) {
			for _, reporter := range []chats.ProbeTrigger{chats.ProbeUnknown, chats.ProbePermissionLost, chats.ProbeOwnerStale} {
				t.Run(string(reporter), func(t *testing.T) {
					decision := chats.Step(before, chats.ProbeTriggered{Trigger: reporter}, now, policy)
					if decision.Next != before || len(decision.Effects) != 1 || decision.Effects[0] != (chats.StartProbe{Trigger: reporter}) {
						t.Fatalf("reporter must request a probe without changing the record or claiming an in-flight slot: %+v", decision)
					}
				})
			}
			t.Run("migrated", func(t *testing.T) {
				const destination = -456
				decision := chats.Step(before, chats.MigrationReported{To: destination}, now, policy)
				matrixPersist(t, decision, true)
				wantRemovedAt, wantRetention := now.Unix(), now.Add(policy.RemovedRetention).Unix()
				wantWatermark := before.Record.Watermark
				if from == chats.NoRecord {
					wantWatermark = before.MemoryWatermark
				}
				if from == chats.Removed {
					wantRemovedAt, wantRetention = before.Record.RemovedAt, before.Record.PurgeAfter
				}
				got := decision.Next.Record
				if got.State != chats.Removed || got.RemovedReason != chats.RemovedMigrated || got.MigratedTo != destination ||
					got.RemovedAt != wantRemovedAt || got.PurgeAfter != wantRetention || got.Watermark != wantWatermark ||
					got.ChatID != before.Record.ChatID || decision.Next.MemoryWatermark != (dispatch.Order{}) ||
					decision.NewMemberEpoch || decision.Next.MemberEpoch != before.MemberEpoch || len(decision.Effects) != 1 {
					t.Fatalf("incorrect migration for old chat: %+v", decision)
				}
			})
			t.Run("claim", func(t *testing.T) {
				const owner = 99
				decision := chats.Step(before, chats.OwnerSet{UserID: owner}, now, policy)
				want := before
				status := chats.ReplyForbidden
				if matrixPresent(from) {
					want.Version++
					want.Record.OwnerID = owner
					want.OwnerRevision++
					status = chats.ReplyOK
				}
				matrixPersist(t, decision, matrixPresent(from))
				replies := matrixEffects[chats.Reply](decision)
				if decision.Next != want || len(replies) != 1 || replies[0].Operation != chats.ReplySetOwner || replies[0].Status != status {
					t.Fatalf("wrong claim decision: %+v, want state=%+v status=%s", decision, want, status)
				}
			})
		})
	}
}

func TestNonReporterProbeTriggerStateMatrix(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	policy := matrixPolicy()
	for _, from := range matrixStates() {
		before := matrixState(from, now)
		for _, tc := range []struct {
			trigger chats.ProbeTrigger
			starts  bool
			idle    bool
		}{
			{chats.ProbeTransition, true, false},
			{chats.ProbePeriodic, matrixPresent(from), false},
			{chats.ProbeIdle, from == chats.Onboarding || from == chats.Degraded, true},
			{"unknown", false, false},
		} {
			t.Run(fmt.Sprintf("from_%q/%s", from, tc.trigger), func(t *testing.T) {
				decision := chats.Step(before, chats.ProbeTriggered{Trigger: tc.trigger}, now, policy)
				if decision.Next != before || decision.NewMemberEpoch {
					t.Fatalf("trigger changed state before the probe started: %+v", decision)
				}
				if !tc.starts {
					if len(decision.Effects) != 0 {
						t.Fatalf("probe triggered for ineligible chat: %+v", decision)
					}
					return
				}
				want := chats.StartProbe{Trigger: tc.trigger, Actions: chats.FollowUp{Idle: tc.idle}}
				if len(decision.Effects) != 1 || decision.Effects[0] != want {
					t.Fatalf("probe effects=%+v, want %+v", decision.Effects, want)
				}
			})
		}
	}
}

func TestMemberUpdateAdapterFiltersRouteAndMalformedMembership(t *testing.T) {
	for _, chatType := range []models.ChatType{models.ChatTypeGroup, models.ChatTypeSupergroup, models.ChatTypeChannel, models.ChatTypePrivate, "unknown"} {
		t.Run(string(chatType), func(t *testing.T) {
			payload := &models.ChatMemberUpdated{Chat: models.Chat{Type: chatType}}
			_, ok := chats.MemberUpdateOf(&models.Update{MyChatMember: payload})
			want := chatType == models.ChatTypeGroup || chatType == models.ChatTypeSupergroup || chatType == models.ChatTypeChannel
			if ok != want {
				t.Fatalf("route matches %v, want %v", ok, want)
			}
		})
	}
	for _, update := range []*models.Update{nil, {}, {MyChatMember: &models.ChatMemberUpdated{Chat: models.Chat{Type: models.ChatTypeGroup}}, Message: &models.Message{}}} {
		if _, ok := chats.MemberUpdateOf(update); ok {
			t.Fatalf("non-standalone my_chat_member payload matched the route: %+v", update)
		}
	}
	now := time.Unix(1_800_000_000, 0)
	for _, identity := range []models.ChatMemberType{models.ChatMemberTypeAdministrator, models.ChatMemberTypeRestricted, models.ChatMemberTypeOwner, "unknown"} {
		t.Run(string(identity), func(t *testing.T) {
			before := matrixState(chats.Active, now)
			newOrder := dispatch.Order{Date: now.Unix(), ID: 51}
			decision := chats.Step(before, chats.MemberUpdate{Order: newOrder, New: chats.MembershipOf(models.ChatMember{Type: identity})}, now, matrixPolicy())
			matrixPersist(t, decision, true)
			before.Record.Watermark = newOrder
			before.Version++
			metrics := matrixEffects[chats.Metric](decision)
			if decision.Next != before || len(metrics) != 1 || metrics[0].Kind != chats.InvalidMember || !metrics[0].Error {
				t.Fatalf("invalid membership should advance the watermark and version and report the error: %+v", decision)
			}
		})
	}
}

func TestSuccessfulGetChatMetadataSurvivesLaterProbeFailure(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	before := matrixState(chats.Active, now)
	inFlight, request := chats.BeginProbe(before, chats.StartProbe{Trigger: chats.ProbeUnknown}, now.Add(-time.Minute))
	okResult := sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}}
	event := chats.ProbeFinished{
		Request: request,
		Chat:    chats.ProbeChat{Result: okResult, Type: models.ChatTypeChannel, Title: "observed title", GuardAssigned: true, JoinByRequest: true},
		Member:  chats.ProbeMember{Result: okResult, Membership: chats.Membership{Status: models.ChatMemberTypeAdministrator}},
		Admins:  chats.ProbeAdmins{Result: sender.Result{Outcome: telegram.Outcome{Class: telegram.Permission}}},
	}
	decision := chats.Step(inFlight, event, now, matrixPolicy())
	matrixPersist(t, decision, true)
	wantRecord := before.Record
	wantRecord.Type, wantRecord.Title, wantRecord.GuardAssigned = event.Chat.Type, event.Chat.Title, true
	if decision.Next.Record != wantRecord || !decision.Next.GuardKnown || !decision.Next.JoinByRequest {
		t.Fatalf("successful getChat must update metadata, retaining rights, state and owner upon later failure: %+v", decision)
	}
}

func TestLeaveCompletionStateMatrix(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	policy := matrixPolicy()
	cases := []struct {
		name    string
		outcome telegram.Outcome
		drop    sender.DropReason
		left    bool
	}{
		{"ok", telegram.Outcome{Class: telegram.OK}, 0, true},
		{"done", telegram.Outcome{Class: telegram.Done, Reason: telegram.ChatGone}, 0, true},
		{"done_other_reason", telegram.Outcome{Class: telegram.Done, Reason: telegram.RequestMissing}, 0, true},
		{"forbidden", telegram.Outcome{Class: telegram.Forbidden}, 0, true},
		{"migrated", telegram.Outcome{Class: telegram.Migrated, MigrateTo: -456}, 0, true},
		{"retry", telegram.Outcome{Class: telegram.Retry}, 0, false},
		{"dropped", telegram.Outcome{Class: telegram.Retry}, sender.Shutdown, false},
		{"unauthorized", telegram.Outcome{Class: telegram.Unauthorized}, 0, false},
		{"conflict", telegram.Outcome{Class: telegram.Conflict}, 0, false},
		{"fatal", telegram.Outcome{Class: telegram.Fatal}, 0, false},
		{"permission", telegram.Outcome{Class: telegram.Permission}, 0, false},
		{"unknown", telegram.Outcome{Class: telegram.Unknown}, 0, false},
	}
	for _, from := range matrixStates() {
		before := matrixState(from, now)
		for _, source := range []chats.LeaveSource{chats.LeavePurge, chats.LeaveIdle, chats.LeaveRejected} {
			for _, tc := range cases {
				t.Run(fmt.Sprintf("from_%q/%s/%s", from, source, tc.name), func(t *testing.T) {
					result := sender.Result{Outcome: tc.outcome, Dropped: tc.drop}
					if !tc.left {
						result.Err = errors.New("leave did not complete")
					}
					membership := before.MembershipSnapshot()
					if source == chats.LeaveRejected {
						membership = chats.MembershipSnapshot{}
					}
					event := chats.LeaveFinished{Source: source, Membership: membership, Result: result}
					decision := chats.Step(before, event, now, policy)
					want := before
					purge := source == chats.LeavePurge
					expedite := source != chats.LeaveRejected && from != chats.NoRecord && (tc.left || purge && from == chats.Removed)
					matrixPersist(t, decision, expedite)
					if expedite {
						want.Version++
						want.Record.PurgeAfter = now.Unix()
						if from != chats.Removed {
							want.Record.State = chats.Removed
							want.Record.BotRights = 0
							want.Record.RemovedAt, want.Record.UpdatedAt = now.Unix(), now.Unix()
							want.Record.RemovedReason = chats.RemovedByIdle
							if purge {
								want.Record.RemovedReason = chats.RemovedByPurge
							}
							if tc.outcome.Class == telegram.Migrated {
								want.Record.RemovedReason, want.Record.MigratedTo = chats.RemovedMigrated, tc.outcome.MigrateTo
							}
						}
					}
					if decision.Next != want || decision.NewMemberEpoch || len(matrixEffects[chats.LeaveChat](decision)) != 0 ||
						decision.Stale != (from == chats.NoRecord && source == chats.LeaveIdle) {
						t.Fatalf("leave completion got %+v, want state %+v and no additional leave", decision, want)
					}
					replies := matrixEffects[chats.Reply](decision)
					if purge {
						want := chats.Reply{Operation: chats.ReplyPurge, Status: chats.ReplyOK}
						if matrixPresent(from) && !tc.left {
							want.Status, want.Result = chats.ReplyNotLeft, result
						}
						if len(replies) != 1 || replies[0] != want {
							t.Fatalf("purge reply=%+v, want %+v", replies, want)
						}
					} else if len(replies) != 0 {
						t.Fatalf("%s leave replied to a caller: %+v", source, replies)
					}
					if !tc.left {
						metrics := matrixEffects[chats.Metric](decision)
						if len(metrics) != 1 || metrics[0].Kind != chats.LeaveOutcome || metrics[0].Outcome != tc.outcome || metrics[0].Dropped != tc.drop {
							t.Fatalf("leave failure metric lost outcome: %+v", metrics)
						}
					}
				})
			}
		}
	}
}

func TestClearCompletionStateMatrix(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	for _, from := range matrixStates() {
		before := matrixState(from, now)
		for _, tc := range []struct {
			name       string
			removedOff int64
			epochOff   uint64
		}{
			{name: "matching"},
			{name: "wrong_removed_at", removedOff: 1},
			{name: "wrong_epoch", epochOff: 1},
		} {
			t.Run(fmt.Sprintf("from_%q/%s", from, tc.name), func(t *testing.T) {
				event := chats.ClearFinished{RemovedAt: before.Record.RemovedAt + tc.removedOff, MemberEpoch: before.MemberEpoch + tc.epochOff}
				decision := chats.Step(before, event, now, matrixPolicy())
				if from != chats.Removed || tc.removedOff != 0 || tc.epochOff != 0 {
					if decision.Next != before || len(decision.Effects) != 0 || !decision.Stale {
						t.Fatalf("clear completion targeted a different removal: %+v", decision)
					}
					return
				}
				if decision.Next.Record != (chats.Record{ChatID: before.Record.ChatID}) || decision.Next.MemoryWatermark != (dispatch.Order{}) ||
					decision.Next.MembershipSnapshot().Exists || decision.Next.Version != before.Version+1 || decision.NewMemberEpoch || decision.Stale ||
					len(decision.Effects) != 1 || decision.Effects[0] != (chats.DeleteRecord{ChatID: before.Record.ChatID}) {
					t.Fatalf("matching clear completion did not delete the record and watermark: %+v", decision)
				}
			})
		}
	}
}
