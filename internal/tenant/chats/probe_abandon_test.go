package chats_test

import (
	"testing"
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/tenant/chats"
)

func TestAbandonProbeDiscardsTheUncommittedCompletion(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	state := lifecycleState(chats.Onboarding)
	start := chats.Step(state, chats.ProbeTriggered{Trigger: chats.ProbeIdle}, now, policy)
	state, request := chats.BeginProbe(start.Next, lifecycleRequire[chats.StartProbe](t, start), now)
	result := lifecycleProbe(request, models.ChatMemberTypeMember, 0)
	result.Chat.Title = "uncommitted title"
	finishedAt := now.Add(time.Second)
	proposal := chats.Step(state, result, finishedAt, policy)
	lifecycleRequire[chats.Persist](t, proposal)
	lifecycleRequire[chats.LeaveChat](t, proposal)

	abandonment := chats.AbandonProbe(state, request, finishedAt, policy)
	abandoned := abandonment.Next
	want := state
	want.Probe.InFlight = chats.ProbeRequest{}
	want.Probe.Actions = chats.FollowUp{}
	if abandoned != want || len(abandonment.Effects) != 0 || abandonment.NewMemberEpoch {
		t.Fatalf("abandonment changed more than the in-flight slot and actions: got %+v, want %+v", abandoned, want)
	}
	if again := chats.AbandonProbe(abandoned, request, finishedAt, policy); again.Next != abandoned || len(again.Effects) != 0 {
		t.Fatalf("duplicate abandonment changed state: %+v", again)
	}
	late := chats.Step(abandoned, result, finishedAt, policy)
	if late.Next != abandoned || len(late.Effects) != 1 || lifecycleRequire[chats.Metric](t, late).Kind != chats.ProbeStaleResult {
		t.Fatalf("late result affected the abandoned chat: %+v", late)
	}

	unknown := chats.ProbeTriggered{Trigger: chats.ProbeUnknown}
	limited := chats.Step(abandoned, unknown, now.Add(lifecycleProbeInterval-time.Nanosecond), policy)
	lifecycleForbid[chats.StartProbe](t, limited)
	if metric := lifecycleRequire[chats.Metric](t, limited); metric.Kind != chats.ProbeLimited {
		t.Fatalf("abandonment failed to preserve the interval: %+v", limited)
	}
	due := now.Add(lifecycleProbeInterval)
	resume := chats.Step(limited.Next, unknown, due, policy)
	intent := lifecycleRequire[chats.StartProbe](t, resume)
	if intent != (chats.StartProbe{Trigger: chats.ProbeUnknown}) {
		t.Fatalf("new Probe inherited abandoned actions: %+v", intent)
	}
	state, _ = chats.BeginProbe(resume.Next, intent, due)
	if got := chats.AbandonProbe(state, request, due, policy); got.Next != state || len(got.Effects) != 0 {
		t.Fatalf("abandoning an older request released its replacement: %+v", got)
	}
}

func TestAbandonProbeIgnoresMismatchedRequest(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	state := lifecycleState(chats.Active)
	state, request := chats.BeginProbe(state, chats.StartProbe{
		Trigger: chats.ProbeUnknown, Actions: chats.FollowUp{Onboarding: true},
	}, now)
	for _, stale := range []chats.ProbeRequest{
		{},
		{Trigger: request.Trigger, StartedAt: request.StartedAt.Add(-time.Second)},
	} {
		if got := chats.AbandonProbe(state, stale, now, policy); got.Next != state || len(got.Effects) != 0 {
			t.Fatalf("abandonment of %+v changed request %+v: %+v", stale, request, got)
		}
	}
	empty := state
	empty.Probe.InFlight = chats.ProbeRequest{}
	empty.Probe.Actions = chats.FollowUp{}
	if got := chats.AbandonProbe(empty, chats.ProbeRequest{}, now, policy); got.Next != empty || len(got.Effects) != 0 {
		t.Fatalf("empty request matched an empty slot: %+v", got)
	}
}

func TestAbandonProbeImmediatelyStartsQueuedTransition(t *testing.T) {
	admin, left := models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft
	for _, tc := range []struct {
		name   string
		update chats.MemberUpdate
		guide  bool
	}{
		{"reentry", lifecycleUpdate(1, left, admin, chats.InviteUsers), true},
		{"downgrade", lifecycleUpdate(1, admin, admin, 0), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now, policy := lifecycleNow(), lifecyclePolicy()
			state, request := chats.BeginProbe(lifecycleState(chats.Active), chats.StartProbe{
				Trigger: chats.ProbeTransition, Actions: chats.FollowUp{Onboarding: true, LanguageCode: "en"},
			}, now)
			seq := state.MemberEpoch
			queued := chats.Step(state, tc.update, now.Add(time.Second), policy)
			lifecycleForbid[chats.StartProbe](t, queued)
			state = lifecycleCommit(queued, now.Add(time.Second), &seq)
			restartAt := now.Add(2 * time.Second)
			restart := chats.AbandonProbe(state, request, restartAt, policy)
			want := state
			want.Probe = chats.ProbeState{LastStarted: request.StartedAt}
			if restart.Next != want || restart.NewMemberEpoch || len(restart.Effects) != 1 {
				t.Fatalf("abandonment did not settle just the active and queued Probe work: %+v", restart)
			}
			intent := lifecycleRequire[chats.StartProbe](t, restart)
			wantIntent := chats.StartProbe{Trigger: chats.ProbeTransition}
			if tc.guide {
				wantIntent.Actions = chats.FollowUp{Onboarding: true, LanguageCode: tc.update.LanguageCode}
			}
			if intent != wantIntent {
				t.Fatalf("queued Probe intent = %+v, want %+v", intent, wantIntent)
			}
			state, replacement := chats.BeginProbe(restart.Next, intent, restartAt)
			if replacement.MemberEpoch != seq || replacement.Version != state.Version {
				t.Fatalf("replacement Probe did not snapshot current facts: %+v", replacement)
			}
			late := chats.Step(state, lifecycleProbe(request, admin, chats.InviteUsers), restartAt, policy)
			if late.Next != state || len(late.Effects) != 1 || lifecycleRequire[chats.Metric](t, late).Kind != chats.ProbeStaleResult {
				t.Fatalf("late result affected the replacement Probe: %+v", late)
			}
			finished := chats.Step(state, lifecycleProbe(replacement, admin, tc.update.New.Rights), restartAt, policy)
			lifecycleForbid[chats.StartProbe](t, finished)
			msg, sent := lifecycleEffect[chats.OnboardingMessage](finished)
			if sent != tc.guide || sent && (!msg.Ephemeral || msg.ReceiverID != tc.update.FromID || msg.Language.Actor != tc.update.LanguageCode) {
				t.Fatalf("replacement Probe sent an incorrect guide: %+v", finished.Effects)
			}
		})
	}
}
