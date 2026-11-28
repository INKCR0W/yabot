package chats_test

import (
	"testing"
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/i18n"
	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
	"github.com/INKCR0W/yabot/internal/tenant/chats"
)

func TestMembershipOfMapsStoredPermissionBits(t *testing.T) {
	for _, tc := range []struct {
		name  string
		admin models.ChatMemberAdministrator
		want  chats.BotRights
	}{
		{"manage chat", models.ChatMemberAdministrator{CanManageChat: true}, 1 << 0},
		{"delete messages", models.ChatMemberAdministrator{CanDeleteMessages: true}, 1 << 1},
		{"restrict members", models.ChatMemberAdministrator{CanRestrictMembers: true}, 1 << 2},
		{"invite users", models.ChatMemberAdministrator{CanInviteUsers: true}, 1 << 3},
		{"pin messages", models.ChatMemberAdministrator{CanPinMessages: true}, 1 << 4},
		{"promote members", models.ChatMemberAdministrator{CanPromoteMembers: true}, 1 << 5},
		{"change info", models.ChatMemberAdministrator{CanChangeInfo: true}, 1 << 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := chats.MembershipOf(models.ChatMember{Type: models.ChatMemberTypeAdministrator, Administrator: &tc.admin})
			if want := (chats.Membership{Status: models.ChatMemberTypeAdministrator, Rights: tc.want}); got != want {
				t.Fatalf("membership = %+v, want %+v", got, want)
			}
		})
	}
}

func TestMembershipOfRestrictedAndMissingPayloads(t *testing.T) {
	for _, tc := range []struct {
		name   string
		member models.ChatMember
		want   chats.Membership
	}{
		{"restricted inside", models.ChatMember{Type: models.ChatMemberTypeRestricted, Restricted: &models.ChatMemberRestricted{
			IsMember: true, CanInviteUsers: true, CanPinMessages: true, CanChangeInfo: true,
		}}, chats.Membership{Status: models.ChatMemberTypeRestricted, IsMember: true, Rights: chats.InviteUsers | chats.PinMessages | chats.ChangeInfo}},
		{"restricted outside", models.ChatMember{Type: models.ChatMemberTypeRestricted, Restricted: &models.ChatMemberRestricted{
			CanInviteUsers: true, CanPinMessages: true, CanChangeInfo: true,
		}}, chats.Membership{Status: models.ChatMemberTypeRestricted}},
		{"missing administrator", models.ChatMember{Type: models.ChatMemberTypeAdministrator}, chats.Membership{}},
		{"missing restricted", models.ChatMember{Type: models.ChatMemberTypeRestricted}, chats.Membership{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := chats.MembershipOf(tc.member); got != tc.want {
				t.Fatalf("membership = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestReentryDiscardsAttachedIdleCheck(t *testing.T) {
	for _, trigger := range []chats.ProbeTrigger{chats.ProbeIdle, chats.ProbeUnknown} {
		t.Run(string(trigger), func(t *testing.T) {
			now, policy := lifecycleNow(), lifecyclePolicy()
			state := lifecycleState(chats.Onboarding)
			seq := state.MemberEpoch
			state = lifecycleCommit(chats.Step(state, chats.ProbeTriggered{Trigger: trigger}, now, policy), now, &seq)
			if trigger == chats.ProbeUnknown {
				state = chats.Step(state, chats.ProbeTriggered{Trigger: chats.ProbeIdle}, now, policy).Next
			}
			request := state.Probe.InFlight
			join := lifecycleUpdate(1, models.ChatMemberTypeLeft, models.ChatMemberTypeMember, 0)
			state = lifecycleCommit(chats.Step(state, join, now, policy), now, &seq)
			completion := chats.Step(state, lifecycleProbe(request, models.ChatMemberTypeMember, 0), now, policy)
			if state.MemberEpoch == request.MemberEpoch || completion.Next.Record != state.Record {
				t.Fatalf("old Probe changed the rejoined chat: %+v", completion)
			}
			lifecycleForbid[chats.LeaveChat](t, completion)
			lifecycleForbid[chats.OnboardingMessage](t, completion)
			pending := lifecycleRequire[chats.StartProbe](t, completion)
			if pending.Trigger != chats.ProbeTransition || !pending.Actions.Onboarding || pending.Actions.Idle {
				t.Fatalf("unexpected follow-up Probe: %+v", pending)
			}
			retry := chats.Step(completion.Next, chats.ProbeTriggered{Trigger: chats.ProbeIdle}, now.Add(time.Second), policy)
			lifecycleForbid[chats.LeaveChat](t, retry)
			lifecycleForbid[chats.StartProbe](t, retry)
			if retry.Next.Probe.IdlePending {
				t.Fatalf("new membership was considered idle: %+v", retry)
			}
			state, request = chats.BeginProbe(completion.Next, pending, now.Add(time.Second))
			fresh := chats.Step(state, lifecycleProbe(request, models.ChatMemberTypeMember, 0), now.Add(time.Second), policy)
			lifecycleForbid[chats.LeaveChat](t, fresh)
			lifecycleRequire[chats.OnboardingMessage](t, fresh)
		})
	}
}

func TestRecordlessDepartureDiscardsOlderRecoveryProbe(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	state := lifecycleState(chats.NoRecord)
	seq := uint64(0)
	start := chats.Step(state, chats.ProbeTriggered{Trigger: chats.ProbeUnknown}, now, policy)
	state = lifecycleCommit(start, now, &seq)
	old := lifecycleProbe(state.Probe.InFlight, models.ChatMemberTypeMember, 0)

	left := lifecycleUpdate(1, models.ChatMemberTypeMember, models.ChatMemberTypeLeft, 0)
	departure := chats.Step(state, left, now.Add(time.Second), policy)
	lifecycleRequire[chats.UpdateWatermark](t, departure)
	lifecycleForbid[chats.Persist](t, departure)
	if departure.Next.MemoryWatermark != left.Order || departure.Next.Version != state.Version+1 {
		t.Fatalf("departure lost recordless facts: %+v", departure)
	}
	completion := chats.Step(departure.Next, old, now.Add(2*time.Second), policy)
	lifecycleForbid[chats.Persist](t, completion)
	if completion.Next.Record != state.Record || completion.NewMemberEpoch {
		t.Fatalf("older Probe created a chat after departure: %+v", completion)
	}

	join := lifecycleUpdate(2, models.ChatMemberTypeLeft, models.ChatMemberTypeMember, 0)
	rejoined := lifecycleCommit(chats.Step(completion.Next, join, now.Add(3*time.Second), policy), now.Add(3*time.Second), &seq)
	if rejoined.Probe.InFlight.Version != departure.Next.Version+1 || rejoined.Record.Watermark != join.Order {
		t.Fatalf("joining lost recordless facts: %+v", rejoined)
	}
}

func TestMembershipAdmissionAttributesOnlyPayloadProvenJoin(t *testing.T) {
	admin := models.ChatMemberTypeAdministrator
	for _, tc := range []struct {
		name  string
		old   chats.Membership
		joins bool
	}{
		{name: "promotion", old: chats.Membership{Status: models.ChatMemberTypeMember}},
		{name: "restricted member promoted", old: chats.Membership{Status: models.ChatMemberTypeRestricted, IsMember: true}},
		{name: "unknown old status"},
		{name: "previously left", old: chats.Membership{Status: models.ChatMemberTypeLeft}, joins: true},
		{name: "previously kicked", old: chats.Membership{Status: models.ChatMemberTypeBanned}, joins: true},
		{name: "restricted nonmember", old: chats.Membership{Status: models.ChatMemberTypeRestricted}, joins: true},
	} {
		for _, initial := range []struct {
			name  string
			state chats.ChatState
		}{{"new record", chats.NoRecord}, {"removed", chats.Removed}} {
			t.Run(initial.name+"/"+tc.name, func(t *testing.T) {
				now, policy := lifecycleNow(), lifecyclePolicy()
				state := lifecycleState(initial.state)
				seq := state.MemberEpoch
				ev := lifecycleUpdate(1, tc.old.Status, admin, chats.InviteUsers)
				ev.Old = tc.old
				d := chats.Step(state, ev, now, policy)
				row := lifecycleRequire[chats.Persist](t, d).Record
				wantBy, wantAt := int64(0), state.Record.AddedAt
				wantProbe := chats.StartProbe{Trigger: chats.ProbeTransition}
				if tc.joins {
					wantBy, wantAt = ev.FromID, now.Unix()
					wantProbe.Actions = chats.FollowUp{Onboarding: true, LanguageCode: ev.LanguageCode}
				}
				if row.State != chats.Active || row.OwnerID != 0 || !d.NewMemberEpoch || row.Watermark != ev.Order ||
					row.AddedBy != wantBy || row.AddedAt != wantAt || row.RemovedAt != 0 || row.RemovedReason != "" || row.PurgeAfter != 0 {
					t.Fatalf("admission stored incorrect membership: %+v", d)
				}
				if intent := lifecycleRequire[chats.StartProbe](t, d); intent != wantProbe {
					t.Fatalf("admission requested %+v, want %+v", intent, wantProbe)
				}
				state = lifecycleCommit(d, now, &seq)
				finished := chats.Step(state, lifecycleProbe(state.Probe.InFlight, admin, chats.InviteUsers), now, policy)
				msg, sent := lifecycleEffect[chats.OnboardingMessage](finished)
				if sent != tc.joins || sent && msg.ReceiverID != ev.FromID {
					t.Fatalf("admission sent an incorrect guide: %+v", finished.Effects)
				}
			})
		}
	}
}

func TestMissedJoinSupersedesGuideQueuedForPreviousMembership(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	admin, member, left := models.ChatMemberTypeAdministrator, models.ChatMemberTypeMember, models.ChatMemberTypeLeft
	state := lifecycleState(chats.NoRecord)
	seq := state.MemberEpoch
	state = lifecycleCommit(chats.Step(state, chats.ProbeTriggered{Trigger: chats.ProbeUnknown}, now, policy), now, &seq)
	request := state.Probe.InFlight
	for _, update := range []chats.MemberUpdate{
		lifecycleUpdate(1, left, member, 0),
		lifecycleUpdate(2, member, left, 0),
		lifecycleUpdate(3, member, admin, chats.InviteUsers),
	} {
		now = now.Add(time.Second)
		state = lifecycleCommit(chats.Step(state, update, now, policy), now, &seq)
	}
	finished := chats.Step(state, lifecycleProbe(request, admin, chats.InviteUsers), now, policy)
	if intent := lifecycleRequire[chats.StartProbe](t, finished); intent != (chats.StartProbe{Trigger: chats.ProbeTransition}) {
		t.Fatalf("recovery inherited a guide from the previous membership: %+v", intent)
	}
	lifecycleForbid[chats.OnboardingMessage](t, finished)
}

func TestIdleCheckRequiresCurrentProbeVersionAndMembership(t *testing.T) {
	for _, tc := range []struct {
		name          string
		state         chats.ChatState
		status        models.ChatMemberType
		newerUpdate   bool
		changeEpoch   bool
		wantIdleLeave bool
	}{
		{name: "confirmed onboarding", state: chats.Onboarding, status: models.ChatMemberTypeMember, wantIdleLeave: true},
		{name: "confirmed degraded", state: chats.Degraded, status: models.ChatMemberTypeAdministrator, wantIdleLeave: true},
		{name: "onboarding to degraded", state: chats.Onboarding, status: models.ChatMemberTypeAdministrator, wantIdleLeave: true},
		{name: "degraded to onboarding", state: chats.Degraded, status: models.ChatMemberTypeMember, wantIdleLeave: true},
		{name: "newer member event", state: chats.Onboarding, status: models.ChatMemberTypeMember, newerUpdate: true},
		{name: "membership snapshot differs", state: chats.Onboarding, status: models.ChatMemberTypeMember, changeEpoch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now, policy := lifecycleNow(), lifecyclePolicy()
			state := lifecycleState(tc.state)
			seq := state.MemberEpoch
			state = lifecycleCommit(chats.Step(state, chats.ProbeTriggered{Trigger: chats.ProbeIdle}, now, policy), now, &seq)
			request := state.Probe.InFlight
			if tc.newerUpdate {
				update := lifecycleUpdate(1, tc.status, tc.status, 0)
				state = lifecycleCommit(chats.Step(state, update, now, policy), now, &seq)
			}
			if tc.changeEpoch {
				state.MemberEpoch++
			}
			now = now.Add(time.Second)
			result := lifecycleProbe(request, tc.status, 0)
			completion := chats.Step(state, result, now, policy)
			wantState := chats.Onboarding
			if tc.status == models.ChatMemberTypeAdministrator {
				wantState = chats.Degraded
			}
			if completion.Next.Record.State != wantState ||
				tc.state != wantState && completion.Next.Record.UpdatedAt != now.Unix() {
				t.Fatalf("unexpected state or transition time: %+v", completion.Next.Record)
			}
			leave, ok := lifecycleEffect[chats.LeaveChat](completion)
			if ok != tc.wantIdleLeave {
				t.Fatalf("unexpected leave decision: %+v, want leave %v", completion, tc.wantIdleLeave)
			}
			if ok {
				want := chats.LeaveChat{Source: chats.LeaveIdle, Membership: state.MembershipSnapshot(), Class: sender.Background, NoRetry: true}
				if leave != want {
					t.Fatalf("idle leave = %+v, want %+v", leave, want)
				}
				finished := chats.LeaveFinished{Source: leave.Source, Membership: leave.Membership, Result: lifecycleResult(telegram.OK)}
				left := chats.Step(completion.Next, finished, now, policy)
				if row := lifecycleRequire[chats.Persist](t, left).Record; row.State != chats.Removed || row.PurgeAfter != now.Unix() {
					t.Fatalf("idle leave did not remove the chat: %+v", row)
				}
			}
		})
	}
}

func TestStaleIdleProbeSchedulesFreshVerification(t *testing.T) {
	for _, tc := range []struct {
		name       string
		age        time.Duration
		startsNext bool
	}{
		{"wait for rate limit", lifecycleProbeInterval - time.Nanosecond, false},
		{"start when in-flight Probe finishes", lifecycleProbeInterval, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now, policy := lifecycleNow(), lifecyclePolicy()
			state := lifecycleState(chats.Onboarding)
			seq := state.MemberEpoch
			unknown := chats.ProbeTriggered{Trigger: chats.ProbeUnknown}
			state = lifecycleCommit(chats.Step(state, unknown, now, policy), now, &seq)
			request := state.Probe.InFlight
			idle := chats.ProbeTriggered{Trigger: chats.ProbeIdle}
			state = chats.Step(state, idle, now.Add(time.Second), policy).Next
			claim := chats.Step(state, chats.OwnerSet{UserID: 99}, now.Add(2*time.Second), policy)
			lifecycleRequire[chats.Persist](t, claim)

			finishedAt := now.Add(tc.age)
			older := lifecycleProbe(request, models.ChatMemberTypeMember, 0)
			finish := chats.Step(claim.Next, older, finishedAt, policy)
			lifecycleForbid[chats.LeaveChat](t, finish)
			if finish.Next.Record.OwnerID != 99 || !finish.Next.Probe.IdlePending {
				t.Fatalf("stale result overwrote the claim or dropped the pending idle check: %+v", finish)
			}
			if _, started := lifecycleEffect[chats.StartProbe](finish); started != tc.startsNext {
				t.Fatalf("Probe start at completion = %v, want %v: %+v", started, tc.startsNext, finish)
			}
			if !tc.startsNext {
				finishedAt = now.Add(lifecycleProbeInterval)
				finish = chats.Step(finish.Next, idle, finishedAt, policy)
				lifecycleForbid[chats.LeaveChat](t, finish)
			}
			intent := lifecycleRequire[chats.StartProbe](t, finish)
			if intent.Trigger != chats.ProbeIdle || !intent.Actions.Idle {
				t.Fatalf("fresh Probe lost the idle action: %+v", intent)
			}
			state, current := chats.BeginProbe(finish.Next, intent, finishedAt)
			lifecycleForbid[chats.LeaveChat](t, chats.Step(state, older, finishedAt, policy))
			fresh := chats.Step(state, lifecycleProbe(current, models.ChatMemberTypeMember, 0), finishedAt, policy)
			lifecycleRequire[chats.LeaveChat](t, fresh)
		})
	}
}

func TestFailedProbeCannotAuthorizeIdleLeave(t *testing.T) {
	for _, trigger := range []chats.ProbeTrigger{chats.ProbeIdle, chats.ProbeUnknown} {
		for _, stage := range []string{"getChat", "getChatMember", "getChatAdministrators"} {
			for _, failure := range []struct {
				name   string
				result sender.Result
			}{
				{"retry", lifecycleResult(telegram.Retry)},
				{"dropped", sender.Result{Outcome: telegram.Outcome{Class: telegram.Retry}, Dropped: sender.Busy}},
			} {
				t.Run(string(trigger)+"/"+stage+"/"+failure.name, func(t *testing.T) {
					now, policy := lifecycleNow(), lifecyclePolicy()
					state := lifecycleState(chats.Onboarding)
					seq := state.MemberEpoch
					start := chats.Step(state, chats.ProbeTriggered{Trigger: trigger}, now, policy)
					lifecycleRequire[chats.StartProbe](t, start)
					state = lifecycleCommit(start, now, &seq)
					probe := lifecycleProbe(state.Probe.InFlight, models.ChatMemberTypeAdministrator, chats.InviteUsers)
					switch stage {
					case "getChat":
						probe.Chat, probe.Member, probe.Admins = chats.ProbeChat{Result: failure.result}, chats.ProbeMember{}, chats.ProbeAdmins{}
					case "getChatMember":
						probe.Member, probe.Admins = chats.ProbeMember{Result: failure.result}, chats.ProbeAdmins{}
					case "getChatAdministrators":
						probe.Admins = chats.ProbeAdmins{Result: failure.result}
					}
					completed := chats.Step(state, probe, now.Add(time.Second), policy)
					lifecycleForbid[chats.LeaveChat](t, completed)
					if completed.Next.Record != state.Record {
						t.Fatalf("failed Probe changed the chat record: %+v", completed.Next.Record)
					}
					idle := chats.ProbeTriggered{Trigger: chats.ProbeIdle}
					limited := chats.Step(completed.Next, idle, now.Add(2*time.Second), policy)
					lifecycleForbid[chats.LeaveChat](t, limited)
					lifecycleForbid[chats.StartProbe](t, limited)
					if metric := lifecycleRequire[chats.Metric](t, limited); metric.Kind != chats.ProbeLimited {
						t.Fatalf("retry inside the Probe interval was not rate limited: %+v", metric)
					}
					next := chats.Step(limited.Next, idle, now.Add(lifecycleProbeInterval), policy)
					lifecycleRequire[chats.StartProbe](t, next)
					lifecycleForbid[chats.LeaveChat](t, next)
				})
			}
		}
	}
}

func TestIdleLeaveRequiresFreshSuccessfulProbeEachTime(t *testing.T) {
	for _, tc := range []struct {
		name     string
		state    chats.ChatState
		member   models.ChatMemberType
		followUp chats.ProbeTrigger
		wait     time.Duration
	}{
		{"onboarding", chats.Onboarding, models.ChatMemberTypeMember, chats.ProbeIdle, time.Hour},
		{"degraded", chats.Degraded, models.ChatMemberTypeAdministrator, chats.ProbePeriodic, lifecycleProbeInterval},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now, policy := lifecycleNow(), lifecyclePolicy()
			state := lifecycleState(tc.state)
			seq := state.MemberEpoch
			idle := chats.ProbeTriggered{Trigger: chats.ProbeIdle}
			state = lifecycleCommit(chats.Step(state, idle, now, policy), now, &seq)
			first := lifecycleProbe(state.Probe.InFlight, tc.member, 0)
			completed := chats.Step(state, first, now.Add(time.Second), policy)
			leave := lifecycleRequire[chats.LeaveChat](t, completed)
			if len(matrixEffects[chats.LeaveChat](completed)) != 1 {
				t.Fatalf("Probe produced multiple idle leaves: %+v", completed)
			}
			duplicate := chats.Step(completed.Next, first, now.Add(time.Second), policy)
			lifecycleForbid[chats.LeaveChat](t, duplicate)
			limited := chats.Step(duplicate.Next, idle, now.Add(2*time.Second), policy)
			lifecycleForbid[chats.LeaveChat](t, limited)
			lifecycleForbid[chats.StartProbe](t, limited)
			if !limited.Next.Probe.IdlePending {
				t.Fatalf("rate-limited idle request was lost: %+v", limited)
			}
			retry := chats.LeaveFinished{Source: leave.Source, Membership: leave.Membership, Result: lifecycleResult(telegram.Retry)}
			failed := chats.Step(limited.Next, retry, now.Add(3*time.Second), policy)
			lifecycleForbid[chats.Persist](t, failed)
			lifecycleForbid[chats.LeaveChat](t, failed)
			if failed.Next.Record != limited.Next.Record {
				t.Fatalf("leave retry changed the chat record: %+v", failed)
			}

			due := now.Add(tc.wait)
			decision := chats.Step(failed.Next, chats.ProbeTriggered{Trigger: tc.followUp}, due, policy)
			intent := lifecycleRequire[chats.StartProbe](t, decision)
			lifecycleForbid[chats.LeaveChat](t, decision)
			if !intent.Actions.Idle || !decision.Next.Probe.IdlePending {
				t.Fatalf("unstarted Probe lost the pending idle request: %+v", decision)
			}
			state, freshRequest := chats.BeginProbe(decision.Next, intent, due)
			if state.Probe.IdlePending {
				t.Fatalf("started Probe left an idle request pending: %+v", state.Probe)
			}
			lifecycleForbid[chats.LeaveChat](t, chats.Step(state, first, due, policy))
			fresh := chats.Step(state, lifecycleProbe(freshRequest, tc.member, 0), due.Add(time.Second), policy)
			if got := lifecycleRequire[chats.LeaveChat](t, fresh); got != leave {
				t.Fatalf("fresh Probe requested a different idle leave: got %+v, want %+v", got, leave)
			}
		})
	}
}

func TestPendingIdleProbeRechecksEligibility(t *testing.T) {
	for _, tc := range []struct {
		name      string
		old, new  models.ChatMemberType
		wantProbe bool
	}{
		{"reentry", models.ChatMemberTypeLeft, models.ChatMemberTypeMember, true},
		{"state changed", models.ChatMemberTypeMember, models.ChatMemberTypeAdministrator, true},
		{"previously active", models.ChatMemberTypeAdministrator, models.ChatMemberTypeMember, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now, policy := lifecycleNow(), lifecyclePolicy()
			state := lifecycleState(chats.Onboarding)
			started, request := chats.BeginProbe(state, chats.StartProbe{Trigger: chats.ProbeUnknown}, now)
			state = chats.Step(started, lifecycleProbe(request, models.ChatMemberTypeMember, 0), now, policy).Next
			idle := chats.ProbeTriggered{Trigger: chats.ProbeIdle}
			limited := chats.Step(state, idle, now.Add(time.Second), policy)
			if !limited.Next.Probe.IdlePending {
				t.Fatalf("rate-limited idle request was lost: %+v", limited)
			}

			update := lifecycleUpdate(1, tc.old, tc.new, 0)
			changeAt := now.Add(2 * time.Second)
			changed := chats.Step(limited.Next, update, changeAt, policy)
			intent, ok := lifecycleEffect[chats.StartProbe](changed)
			if ok != tc.wantProbe || intent.Actions.Idle {
				t.Fatalf("unexpected Probe after idle eligibility changed: %+v", changed)
			}
			seq := state.MemberEpoch
			state = lifecycleCommit(changed, changeAt, &seq)
			due := chats.Step(state, idle, now.Add(lifecycleProbeInterval), policy)
			if len(due.Effects) != 0 || due.Next.Probe.IdlePending {
				t.Fatalf("ineligible chat kept idle work: %+v", due)
			}
		})
	}
}

func TestStaleProbeGuidesTheSameMembershipUsingCurrentState(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	join := lifecycleUpdate(1, models.ChatMemberTypeLeft, models.ChatMemberTypeMember, 0)
	seq := uint64(50)
	removed := lifecycleState(chats.Removed)
	removed.Record.GuardAssigned, removed.GuardKnown = true, true
	state := lifecycleCommit(chats.Step(removed, join, now, policy), now, &seq)
	request := state.Probe.InFlight
	update := lifecycleUpdate(2, models.ChatMemberTypeMember, models.ChatMemberTypeAdministrator, chats.InviteUsers)
	state = lifecycleCommit(chats.Step(state, update, now, policy), now, &seq)
	result := lifecycleProbe(request, models.ChatMemberTypeMember, 0)
	result.Chat.Type = models.ChatTypeChannel
	result.Chat.GuardAssigned, result.Chat.JoinByRequest = true, true
	completion := chats.Step(state, result, now, policy)
	message := lifecycleRequire[chats.OnboardingMessage](t, completion)
	if !message.Ephemeral || message.ReceiverID != join.FromID || message.Language.Actor != join.LanguageCode ||
		message.GuardKnown || message.JoinByRequest || message.NeedsInviteUsers ||
		!completion.Next.Record.GuardAssigned {
		t.Fatalf("onboarding used stale Probe data or lost the joining user: %+v", message)
	}
	pending := lifecycleRequire[chats.StartProbe](t, completion)
	if pending.Actions.Onboarding {
		t.Fatalf("same-membership state update queued another guide: %+v", pending)
	}
}

func TestProbeRefreshesGuardAssignmentOnlyWhenVisible(t *testing.T) {
	admin, member := models.ChatMemberTypeAdministrator, models.ChatMemberTypeMember
	for _, tc := range []struct {
		name         string
		status       models.ChatMemberType
		chatClass    telegram.Class
		memberClass  telegram.Class
		visible      bool
		wantKnown    bool
		wantAssigned bool
		wantJoin     bool
		wantPersist  bool
	}{
		{name: "member", status: member, chatClass: telegram.OK, memberClass: telegram.OK, wantAssigned: true, wantJoin: true, wantPersist: true},
		{name: "membership query failed", status: admin, chatClass: telegram.OK, memberClass: telegram.Retry, wantAssigned: true, wantJoin: true},
		{name: "chat query failed", chatClass: telegram.Retry, wantAssigned: true},
		{name: "visible assignment despite membership failure", status: admin, chatClass: telegram.OK, memberClass: telegram.Retry, visible: true, wantKnown: true, wantAssigned: true, wantJoin: true},
		{name: "admin confirmed no assignment", status: admin, chatClass: telegram.OK, memberClass: telegram.OK, wantKnown: true, wantJoin: true, wantPersist: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now, policy := lifecycleNow(), lifecyclePolicy()
			state := lifecycleState(chats.Active)
			state.Record.GuardAssigned, state.GuardKnown = true, true
			state, request := chats.BeginProbe(state, chats.StartProbe{Trigger: chats.ProbePeriodic}, now)
			var rights chats.BotRights
			if tc.status == admin {
				rights = chats.InviteUsers
			}
			result := lifecycleProbe(request, tc.status, rights)
			result.Chat.Result = lifecycleResult(tc.chatClass)
			result.Chat.JoinByRequest = true
			result.Chat.GuardVisible, result.Chat.GuardAssigned = tc.visible, tc.visible
			result.Member.Result = lifecycleResult(tc.memberClass)
			result.Admins.CreatorID = state.Record.OwnerID
			completed := chats.Step(state, result, now, policy)
			matrixPersist(t, completed, tc.wantPersist)
			if completed.Next.GuardKnown != tc.wantKnown || completed.Next.Record.GuardAssigned != tc.wantAssigned ||
				completed.Next.JoinByRequest != tc.wantJoin {
				t.Fatalf("unexpected guard or join request metadata: %+v", completed.Next)
			}
		})
	}
}

func TestStaleProbeKeepsCurrentMetadataAndChecksOwnerSeparately(t *testing.T) {
	for _, class := range []telegram.Class{telegram.OK, telegram.Permission} {
		t.Run(class.String(), func(t *testing.T) {
			now, policy := lifecycleNow(), lifecyclePolicy()
			state := lifecycleState(chats.Active)
			state, request := chats.BeginProbe(state, chats.StartProbe{Trigger: chats.ProbeUnknown}, now.Add(-time.Minute))
			downgrade := lifecycleUpdate(1, models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, 0)
			downgrade.Old.Rights = chats.InviteUsers
			downgrade.Title = "current title"
			state = chats.Step(state, downgrade, now, policy).Next
			old := lifecycleProbe(request, models.ChatMemberTypeAdministrator, chats.InviteUsers)
			old.Chat.Type, old.Chat.Title = models.ChatTypeGroup, "previous title"
			old.Chat.GuardAssigned, old.Chat.JoinByRequest = true, true
			old.Admins.Result = lifecycleResult(class)
			completion := chats.Step(state, old, now, policy)
			want := state.Record
			if class == telegram.OK {
				want.OwnerID = old.Admins.CreatorID
				lifecycleRequire[chats.Persist](t, completion)
			} else {
				lifecycleForbid[chats.Persist](t, completion)
			}
			if completion.Next.Record != want || completion.Next.GuardKnown != state.GuardKnown || completion.Next.JoinByRequest != state.JoinByRequest {
				t.Fatalf("record or metadata changed unexpectedly: %+v, want record %+v", completion.Next, want)
			}
			lifecycleForbid[chats.DowngradeMessage](t, completion)
			lifecycleRequire[chats.StartProbe](t, completion)
			stale := false
			for _, metric := range matrixEffects[chats.Metric](completion) {
				stale = stale || metric.Kind == chats.ProbeStaleResult && metric.Trigger == request.Trigger
			}
			if !stale {
				t.Fatalf("stale result was not reported: %+v", completion.Effects)
			}
		})
	}
}

func TestSameStateMemberUpdateInvalidatesOlderProbeRights(t *testing.T) {
	for _, tc := range []struct {
		name         string
		oldRights    chats.BotRights
		newRights    chats.BotRights
		probedRights chats.BotRights
	}{
		{"restored invite permission", 0, chats.InviteUsers, 0},
		{"updated optional permission", chats.InviteUsers, chats.InviteUsers | chats.DeleteMessages, chats.InviteUsers},
		{"identical update confirms rights", chats.InviteUsers, chats.InviteUsers, chats.InviteUsers | chats.DeleteMessages},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now, policy := lifecycleNow(), lifecyclePolicy()
			state := lifecycleState(chats.Active)
			state, request := chats.BeginProbe(state, chats.StartProbe{Trigger: chats.ProbePeriodic}, now)
			update := lifecycleUpdate(1, models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, tc.newRights)
			update.Old.Rights = tc.oldRights
			member := chats.Step(state, update, now.Add(time.Second), policy)
			row := lifecycleRequire[chats.Persist](t, member).Record
			if row.State != chats.Active || row.BotRights != tc.newRights || row.UpdatedAt != state.Record.UpdatedAt || member.NewMemberEpoch {
				t.Fatalf("member update did not confirm rights within the current membership: %+v", member)
			}
			lifecycleForbid[chats.StartProbe](t, member)
			old := lifecycleProbe(request, models.ChatMemberTypeAdministrator, tc.probedRights)
			completed := chats.Step(member.Next, old, now.Add(2*time.Second), policy)
			row.OwnerID = old.Admins.CreatorID
			if completed.Next.Record != row {
				t.Fatalf("older Probe overwrote confirmed membership facts: got %+v, want %+v", completed.Next.Record, row)
			}
			lifecycleForbid[chats.DowngradeMessage](t, completed)
		})
	}
}

func TestClearDiscardsQueuedProbeWork(t *testing.T) {
	admin := models.ChatMemberTypeAdministrator
	downgrade := lifecycleUpdate(1, admin, admin, 0)
	downgrade.Old.Rights = chats.InviteUsers
	for _, tc := range []struct {
		name    string
		initial chats.ChatState
		trigger chats.ProbeTrigger
		update  chats.MemberUpdate
	}{
		{"downgrade queues a transition", chats.Active, chats.ProbeUnknown, downgrade},
		{"reentry queues onboarding during idle verification", chats.Onboarding, chats.ProbeIdle, lifecycleUpdate(1, models.ChatMemberTypeLeft, admin, chats.InviteUsers)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now, policy := lifecycleNow(), lifecyclePolicy()
			state := lifecycleState(tc.initial)
			seq := state.MemberEpoch
			start := chats.Step(state, chats.ProbeTriggered{Trigger: tc.trigger}, now, policy)
			state, request := chats.BeginProbe(start.Next, lifecycleRequire[chats.StartProbe](t, start), now)

			change := chats.Step(state, tc.update, now.Add(time.Second), policy)
			lifecycleForbid[chats.StartProbe](t, change)
			state = lifecycleCommit(change, now.Add(time.Second), &seq)

			migrated := sender.Result{Outcome: telegram.Outcome{Class: telegram.Migrated, MigrateTo: -100456}}
			leave := chats.LeaveFinished{Source: chats.LeavePurge, Membership: state.MembershipSnapshot(), Result: migrated}
			now = now.Add(2 * time.Second)
			purged := chats.Step(state, leave, now, policy)
			if row := lifecycleRequire[chats.Persist](t, purged).Record; row.State != chats.Removed || row.MigratedTo != migrated.Outcome.MigrateTo || row.PurgeAfter != now.Unix() {
				t.Fatalf("Purge did not record the migration for immediate clearance: %+v", row)
			}
			state = lifecycleCommit(purged, now, &seq)
			clear := chats.ClearFinished{RemovedAt: state.Record.RemovedAt, MemberEpoch: state.MemberEpoch}
			cleared := chats.Step(state, clear, now.Add(time.Second), policy)
			lifecycleRequire[chats.DeleteRecord](t, cleared)
			if want := (chats.ProbeState{InFlight: request, LastStarted: request.StartedAt}); cleared.Next.Probe != want {
				t.Errorf("clear retained unexpected Probe bookkeeping: got %+v, want %+v", cleared.Next.Probe, want)
			}

			now = now.Add(2 * time.Second)
			unknown := chats.ProbeTriggered{Trigger: chats.ProbeUnknown}
			overlap := chats.Step(cleared.Next, unknown, now, policy)
			lifecycleForbid[chats.StartProbe](t, overlap)
			if metric := lifecycleRequire[chats.Metric](t, overlap); metric.Kind != chats.ProbeInFlight {
				t.Fatalf("the original Probe is no longer tracked after deletion: %+v", overlap)
			}

			finished := chats.Step(overlap.Next, chats.ProbeFinished{Request: request, Chat: chats.ProbeChat{Result: migrated}}, now, policy)
			lifecycleForbid[chats.StartProbe](t, finished)
			lifecycleForbid[chats.Persist](t, finished)
			lifecycleForbid[chats.LeaveChat](t, finished)
			lifecycleForbid[chats.OnboardingMessage](t, finished)
			if finished.Next.Record.State != chats.NoRecord {
				t.Fatalf("the old Probe recreated the deleted record: %+v", finished.Next.Record)
			}
			limited := chats.Step(finished.Next, unknown, now, policy)
			lifecycleForbid[chats.StartProbe](t, limited)
			if metric := lifecycleRequire[chats.Metric](t, limited); metric.Kind != chats.ProbeLimited {
				t.Fatalf("clearance reset the Probe interval: %+v", limited)
			}
			fresh := chats.Step(limited.Next, unknown, request.StartedAt.Add(lifecycleProbeInterval), policy)
			if intent := lifecycleRequire[chats.StartProbe](t, fresh); intent != (chats.StartProbe{Trigger: chats.ProbeUnknown}) {
				t.Fatalf("new report inherited work queued before clearance: %+v", intent)
			}
		})
	}
}

func TestClearDiscardsRateLimitedIdleCheck(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	state := lifecycleState(chats.Onboarding)
	seq := state.MemberEpoch
	unknown := chats.ProbeTriggered{Trigger: chats.ProbeUnknown}
	state = lifecycleCommit(chats.Step(state, unknown, now, policy), now, &seq)
	state = chats.Step(state, lifecycleProbe(state.Probe.InFlight, models.ChatMemberTypeMember, 0), now, policy).Next

	now = now.Add(time.Second)
	idle := chats.Step(state, chats.ProbeTriggered{Trigger: chats.ProbeIdle}, now, policy)
	if !idle.Next.Probe.IdlePending {
		t.Fatalf("idle check was not deferred by the rate limit: %+v", idle)
	}
	migrated := chats.Step(idle.Next, chats.MigrationReported{To: -100456}, now, policy).Next
	clear := chats.ClearFinished{RemovedAt: migrated.Record.RemovedAt, MemberEpoch: migrated.MemberEpoch}
	cleared := chats.Step(migrated, clear, now, policy)
	lifecycleRequire[chats.DeleteRecord](t, cleared)
	if cleared.Next.Probe.IdlePending {
		t.Fatalf("clear kept an idle check scheduled for the deleted chat: %+v", cleared.Next.Probe)
	}
	limited := chats.Step(cleared.Next, unknown, now, policy)
	lifecycleForbid[chats.StartProbe](t, limited)
	if metric := lifecycleRequire[chats.Metric](t, limited); metric.Kind != chats.ProbeLimited {
		t.Fatalf("clearance reset the Probe interval: %+v", limited)
	}
	join := lifecycleUpdate(1, models.ChatMemberTypeLeft, models.ChatMemberTypeMember, 0)
	rejoined := chats.Step(limited.Next, join, now, policy)
	if intent := lifecycleRequire[chats.StartProbe](t, rejoined); intent.Trigger != chats.ProbeTransition || !intent.Actions.Onboarding || intent.Actions.Idle {
		t.Fatalf("new membership did not bypass the interval or inherited an idle check: %+v", rejoined)
	}
}

func TestClearKeepsInFlightProbeWithoutItsActions(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	policy.IdleLeaveAfter = time.Second
	member, left := models.ChatMemberTypeMember, models.ChatMemberTypeLeft
	join := lifecycleUpdate(1, left, member, 0)
	seq := uint64(50)
	state := lifecycleCommit(chats.Step(lifecycleState(chats.NoRecord), join, now, policy), now, &seq)
	request := state.Probe.InFlight
	now = now.Add(time.Second)
	state = chats.Step(state, chats.ProbeTriggered{Trigger: chats.ProbeIdle}, now, policy).Next
	if !state.Probe.Actions.Idle {
		t.Fatal("idle intent was not attached to the running Probe")
	}
	departure := chats.Step(state, lifecycleUpdate(2, member, left, 0), now, policy).Next
	clear := chats.ClearFinished{RemovedAt: departure.Record.RemovedAt, MemberEpoch: departure.MemberEpoch}
	cleared := chats.Step(departure, clear, now, policy)
	lifecycleRequire[chats.DeleteRecord](t, cleared)
	if want := (chats.ProbeState{InFlight: request, LastStarted: request.StartedAt}); cleared.Next.Probe != want {
		t.Fatalf("clear retained unexpected Probe bookkeeping: got %+v, want %+v", cleared.Next.Probe, want)
	}
	result := lifecycleProbe(request, member, 0)
	finished := chats.Step(cleared.Next, result, now, policy)
	lifecycleForbid[chats.OnboardingMessage](t, finished)
	lifecycleForbid[chats.LeaveChat](t, finished)
	lifecycleForbid[chats.Persist](t, finished)
	if want := (chats.ProbeState{LastStarted: request.StartedAt}); finished.Next.Probe != want {
		t.Fatalf("completion did not release the retained request: got %+v, want %+v", finished.Next.Probe, want)
	}
}

func TestSameSecondLeaveCompletionKeepsRecoveryProbeValid(t *testing.T) {
	for _, source := range []chats.LeaveSource{chats.LeavePurge, chats.LeaveIdle} {
		t.Run(string(source), func(t *testing.T) {
			now, policy := lifecycleNow(), lifecyclePolicy()
			state := lifecycleState(chats.Removed)
			seq := state.MemberEpoch
			leave := chats.LeaveFinished{Source: source, Membership: state.MembershipSnapshot(), Result: lifecycleResult(telegram.OK)}
			first := chats.Step(state, leave, now, policy)
			if row := lifecycleRequire[chats.Persist](t, first).Record; row.PurgeAfter != now.Unix() {
				t.Fatalf("first completion did not set the deadline: %+v", row)
			}
			state = lifecycleCommit(first, now, &seq)
			unknown := chats.ProbeTriggered{Trigger: chats.ProbeUnknown}
			state = lifecycleCommit(chats.Step(state, unknown, now.Add(time.Millisecond), policy), now.Add(time.Millisecond), &seq)
			if source == chats.LeavePurge {
				leave.Result = lifecycleResult(telegram.Retry)
			}
			second := chats.Step(state, leave, now.Add(2*time.Millisecond), policy)
			lifecycleForbid[chats.Persist](t, second)
			if source == chats.LeavePurge && lifecycleRequire[chats.Reply](t, second).Status != chats.ReplyOK {
				t.Fatalf("duplicate Purge did not succeed: %+v", second)
			}
			if second.Next != state {
				t.Fatalf("equal-second deadline changed the state or fact version: %+v", second)
			}
			result := lifecycleProbe(state.Probe.InFlight, models.ChatMemberTypeMember, 0)
			finished := chats.Step(second.Next, result, now.Add(time.Second), policy)
			if row := lifecycleRequire[chats.Persist](t, finished).Record; row.State != chats.Onboarding || row.OwnerID != result.Admins.CreatorID || !finished.NewMemberEpoch {
				t.Fatalf("no-op leave completion invalidated recovery: %+v", finished)
			}
		})
	}
}

func TestSameOwnerClaimProtectsOwnerWithoutWriting(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	state, request := chats.BeginProbe(lifecycleState(chats.Active), chats.StartProbe{Trigger: chats.ProbeUnknown}, now)
	claim := chats.Step(state, chats.OwnerSet{UserID: state.Record.OwnerID}, now, policy)
	lifecycleForbid[chats.Persist](t, claim)
	want := state
	want.OwnerRevision++
	if claim.Next != want || lifecycleRequire[chats.Reply](t, claim).Status != chats.ReplyOK {
		t.Fatalf("idempotent claim lost the owner revision or changed chat facts: %+v", claim)
	}
	result := lifecycleProbe(request, models.ChatMemberTypeMember, 0)
	finished := chats.Step(claim.Next, result, now.Add(time.Second), policy)
	if row := lifecycleRequire[chats.Persist](t, finished).Record; row.State != chats.Onboarding || row.OwnerID != state.Record.OwnerID {
		t.Fatalf("idempotent claim did not protect the owner independently of state: %+v", finished)
	}
}

func TestProbeWithOnlyInMemoryMetadataChangesSkipsPersist(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	state, request := chats.BeginProbe(lifecycleState(chats.Active), chats.StartProbe{Trigger: chats.ProbeUnknown}, now)
	result := lifecycleProbe(request, models.ChatMemberTypeAdministrator, chats.InviteUsers)
	result.Admins.CreatorID = state.Record.OwnerID
	result.Chat.JoinByRequest = true
	finished := chats.Step(state, result, now.Add(time.Second), policy)
	lifecycleForbid[chats.Persist](t, finished)
	if finished.Next.Record != state.Record || finished.Next.Version != state.Version || !finished.Next.GuardKnown || !finished.Next.JoinByRequest {
		t.Fatalf("in-memory metadata change was lost or changed persisted facts: %+v", finished)
	}
}

func TestEphemeralCompletionKeepsDowngradeProbeValid(t *testing.T) {
	for _, class := range []telegram.Class{telegram.OK, telegram.Retry} {
		t.Run(class.String(), func(t *testing.T) {
			now, policy := lifecycleNow(), lifecyclePolicy()
			admin := models.ChatMemberTypeAdministrator
			join := lifecycleUpdate(1, models.ChatMemberTypeLeft, admin, chats.InviteUsers)
			seq := uint64(0)
			state := lifecycleCommit(chats.Step(lifecycleState(chats.NoRecord), join, now, policy), now, &seq)
			confirmed := chats.Step(state, lifecycleProbe(state.Probe.InFlight, admin, chats.InviteUsers), now, policy)
			message := lifecycleRequire[chats.OnboardingMessage](t, confirmed)
			if !message.Ephemeral {
				t.Fatalf("expected ephemeral onboarding: %+v", message)
			}

			now = now.Add(time.Second)
			downgrade := lifecycleUpdate(2, admin, admin, 0)
			downgrade.Old.Rights = chats.InviteUsers
			changed := chats.Step(confirmed.Next, downgrade, now, policy)
			state, request := chats.BeginProbe(changed.Next, lifecycleRequire[chats.StartProbe](t, changed), now)
			delivered := chats.Step(state, chats.OnboardingFinished{
				MemberEpoch: message.MemberEpoch, ReceiverID: message.ReceiverID,
				LanguageCode: message.Language.Actor, Result: lifecycleResult(class),
			}, now.Add(time.Second), policy)
			gone := sender.Result{Outcome: telegram.Outcome{Class: telegram.Done, Reason: telegram.ChatGone}}
			finished := chats.Step(delivered.Next, chats.ProbeFinished{Request: request, Chat: chats.ProbeChat{Result: gone}}, now.Add(2*time.Second), policy)
			if row := lifecycleRequire[chats.Persist](t, finished).Record; row.State != chats.Removed || row.RemovedReason != chats.RemovedChatGone {
				t.Fatalf("downgrade Probe did not record that the chat is gone: %+v", row)
			}
		})
	}
}

func TestPendingOnboardingUsesLatestAdderLanguage(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	state := lifecycleState(chats.Active)
	seq := state.MemberEpoch
	state, request := chats.BeginProbe(state, chats.StartProbe{Trigger: chats.ProbeUnknown}, now)
	admin := models.ChatMemberTypeAdministrator
	first := lifecycleUpdate(1, models.ChatMemberTypeLeft, admin, chats.InviteUsers)
	second := lifecycleUpdate(2, models.ChatMemberTypeLeft, admin, chats.InviteUsers)
	second.FromID, second.LanguageCode = first.FromID+1, "en"
	for _, update := range []chats.MemberUpdate{first, second} {
		now = now.Add(time.Second)
		changed := chats.Step(state, update, now, policy)
		lifecycleForbid[chats.StartProbe](t, changed)
		state = lifecycleCommit(changed, now, &seq)
	}

	completed := chats.Step(state, lifecycleProbe(request, admin, chats.InviteUsers), now, policy)
	lifecycleForbid[chats.OnboardingMessage](t, completed)
	pending := lifecycleRequire[chats.StartProbe](t, completed)
	state, request = chats.BeginProbe(completed.Next, pending, now)
	finished := chats.Step(state, lifecycleProbe(request, admin, chats.InviteUsers), now, policy)
	message := lifecycleRequire[chats.OnboardingMessage](t, finished)
	if got := i18n.Resolve(message.Language.Group, message.Language.Actor); !message.Ephemeral || message.ReceiverID != second.FromID ||
		message.Language.Actor != second.LanguageCode || got != "en" {
		t.Fatalf("second adder did not receive an English ephemeral message: %+v resolved to %q", message, got)
	}
}

func TestDowngradeLanguageCandidates(t *testing.T) {
	for _, tc := range []struct {
		name      string
		group     string
		actor     string
		fromProbe bool
		want      string
	}{
		{"auto uses actor", "auto", "zh-CN", false, "zh-Hans"},
		{"auto falls back to English", "auto", "es", false, "en"},
		{"group language wins", "en", "zh-CN", false, "en"},
		{"Probe uses group language", "zh-Hans", "", true, "zh-Hans"},
		{"Probe falls back to English", "auto", "", true, "en"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now, policy := lifecycleNow(), lifecyclePolicy()
			policy.Language = tc.group
			state := lifecycleState(chats.Active)
			var event chats.Event
			if tc.fromProbe {
				var request chats.ProbeRequest
				state, request = chats.BeginProbe(state, chats.StartProbe{Trigger: chats.ProbePermissionLost}, now)
				event = lifecycleProbe(request, models.ChatMemberTypeAdministrator, 0)
			} else {
				update := lifecycleUpdate(1, models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, 0)
				update.Old.Rights = chats.InviteUsers
				update.LanguageCode = tc.actor
				event = update
			}
			message := lifecycleRequire[chats.DowngradeMessage](t, chats.Step(state, event, now, policy))
			if got := i18n.Resolve(message.Language.Group, message.Language.Actor); got != tc.want || message.Language.Actor != tc.actor {
				t.Fatalf("language = %q from %+v, want %q with actor %q", got, message.Language, tc.want, tc.actor)
			}
		})
	}
}

func TestMigrationReportSupersedesEarlierRecoveryProbe(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	state := lifecycleState(chats.Removed)
	seq := state.MemberEpoch
	start := chats.Step(state, chats.ProbeTriggered{Trigger: chats.ProbeUnknown}, now, policy)
	state = lifecycleCommit(start, now, &seq)
	request := state.Probe.InFlight
	migration := chats.MigrationReported{To: -100456}
	decision := chats.Step(state, migration, now.Add(time.Second), policy)
	migrated := lifecycleRequire[chats.Persist](t, decision).Record
	if migrated.State != chats.Removed || migrated.RemovedReason != chats.RemovedMigrated || migrated.MigratedTo != migration.To {
		t.Fatalf("migration was not recorded: %+v", migrated)
	}
	state = lifecycleCommit(decision, now.Add(time.Second), &seq)
	completion := chats.Step(state, lifecycleProbe(request, models.ChatMemberTypeMember, 0), now.Add(2*time.Second), policy)
	if completion.Next.Record != migrated || completion.NewMemberEpoch {
		t.Fatalf("older Probe restored a migrated chat: %+v", completion)
	}
	lifecycleForbid[chats.Persist](t, completion)
}
