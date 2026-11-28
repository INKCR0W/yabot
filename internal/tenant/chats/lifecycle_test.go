package chats_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
	"github.com/INKCR0W/yabot/internal/tenant/chats"
)

const (
	lifecycleProbeInterval   = 10 * time.Minute
	lifecycleMessageCooldown = 24 * time.Hour
)

func lifecycleNow() time.Time { return time.Unix(1_800_000_000, 0) }

func lifecyclePolicy() chats.Policy {
	return chats.Policy{
		AcceptNewChats: true, RemovedRetention: 30 * 24 * time.Hour, IdleLeaveAfter: 7 * 24 * time.Hour,
		Language: "auto",
	}
}

func lifecycleState(state chats.ChatState) chats.State {
	s := chats.State{Record: chats.Record{ChatID: -100123}}
	if state != chats.NoRecord {
		s.Record.State = state
		s.Record.Type = models.ChatTypeSupergroup
		s.Record.Title = "Example"
		s.Record.OwnerID = 4
		s.Record.AddedBy = 5
		s.Record.AddedAt = lifecycleNow().Add(-8 * 24 * time.Hour).Unix()
		s.Record.UpdatedAt = s.Record.AddedAt
		s.MemberEpoch = 6
		s.Version = 7
		s.OwnerRevision = 8
	}
	if state == chats.Active {
		s.Record.BotRights = chats.InviteUsers
		s.Record.EverActive = true
	}
	if state == chats.Removed {
		s.Record.RemovedAt = lifecycleNow().Add(-time.Hour).Unix()
		s.Record.PurgeAfter = s.Record.RemovedAt + int64(lifecyclePolicy().RemovedRetention/time.Second)
		s.Record.RemovedReason = chats.RemovedKicked
	}
	return s
}

func lifecycleUpdate(id int64, old, new models.ChatMemberType, rights chats.BotRights) chats.MemberUpdate {
	return chats.MemberUpdate{
		Order: dispatch.Order{Date: lifecycleNow().Unix(), ID: id},
		Type:  models.ChatTypeSupergroup, Title: "Example", FromID: 12, LanguageCode: "zh-CN",
		Old: chats.Membership{Status: old}, New: chats.Membership{Status: new, Rights: rights},
	}
}

func lifecycleResult(class telegram.Class) sender.Result {
	return sender.Result{Outcome: telegram.Outcome{Class: class}}
}

func lifecycleProbe(request chats.ProbeRequest, status models.ChatMemberType, rights chats.BotRights) chats.ProbeFinished {
	return chats.ProbeFinished{
		Request: request,
		Chat:    chats.ProbeChat{Result: lifecycleResult(telegram.OK), Type: models.ChatTypeSupergroup, Title: "Example"},
		Member:  chats.ProbeMember{Result: lifecycleResult(telegram.OK), Membership: chats.Membership{Status: status, Rights: rights}},
		Admins:  chats.ProbeAdmins{Result: lifecycleResult(telegram.OK), CreatorID: 13},
	}
}

func lifecycleEffect[T chats.Effect](d chats.Decision) (T, bool) {
	for _, effect := range d.Effects {
		if payload, ok := effect.(T); ok {
			return payload, true
		}
	}
	var zero T
	return zero, false
}

func lifecycleRequire[T chats.Effect](t *testing.T, d chats.Decision) T {
	t.Helper()
	effect, ok := lifecycleEffect[T](d)
	if !ok {
		t.Fatalf("missing %T in effects %#v", effect, d.Effects)
	}
	return effect
}

func lifecycleForbid[T chats.Effect](t *testing.T, d chats.Decision) {
	t.Helper()
	if value, ok := lifecycleEffect[T](d); ok {
		t.Fatalf("unexpected %T in effects %#v", value, d.Effects)
	}
}

func lifecycleCommit(d chats.Decision, now time.Time, seq *uint64) chats.State {
	next := d.Next
	if d.NewMemberEpoch {
		*seq++
		next.MemberEpoch = *seq
	}
	if intent, ok := lifecycleEffect[chats.StartProbe](d); ok {
		next, _ = chats.BeginProbe(next, intent, now)
	}
	return next
}

func TestMemberUpdateWatermarkAndRollback(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	s := lifecycleState(chats.NoRecord)
	leave := lifecycleUpdate(10, models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, 0)
	d := chats.Step(s, leave, now, policy)
	if d.Next.Record.State != chats.NoRecord || d.Stale || d.Next.MemoryWatermark != leave.Order {
		t.Fatalf("unexpected recordless departure decision %+v", d)
	}
	if effect := lifecycleRequire[chats.UpdateWatermark](t, d); effect.Order != leave.Order {
		t.Fatalf("watermark effect has order %+v", effect.Order)
	}
	lifecycleForbid[chats.Persist](t, d)
	s = d.Next
	join := lifecycleUpdate(11, models.ChatMemberTypeLeft, models.ChatMemberTypeAdministrator, chats.InviteUsers)
	join.Order.Date--
	for _, ev := range []chats.MemberUpdate{leave, lifecycleUpdate(9, models.ChatMemberTypeLeft, models.ChatMemberTypeMember, 0)} {
		d = chats.Step(s, ev, now, policy)
		if !d.Stale || d.Next != s || len(d.Effects) != 0 {
			t.Fatalf("recordless stale event %v changed state %+v", ev.Order, d)
		}
	}
	d = chats.Step(s, join, now, policy)
	if record := lifecycleRequire[chats.Persist](t, d).Record; record.State != chats.Active || record.Watermark != join.Order {
		t.Fatalf("join persisted %+v", record)
	}
	if replay := chats.Step(s, join, now, policy); !reflect.DeepEqual(replay, d) {
		t.Fatalf("retry after discarding proposed commit changed result %+v", replay)
	}
	seq := uint64(20)
	s = lifecycleCommit(d, now, &seq)
	unchanged := lifecycleUpdate(12, models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, chats.InviteUsers|chats.ManageChat)
	d = chats.Step(s, unchanged, now.Add(time.Second), policy)
	if p := lifecycleRequire[chats.Persist](t, d); p.Record.Watermark != unchanged.Order ||
		p.Record.UpdatedAt != s.Record.UpdatedAt || d.Next.Version != s.Version+1 || d.NewMemberEpoch {
		t.Fatalf("same-state watermark update %+v", d)
	}
	s = d.Next
	for _, ev := range []chats.MemberUpdate{join, unchanged, leave} {
		d = chats.Step(s, ev, now, policy)
		if !d.Stale || d.Next != s || len(d.Effects) != 0 {
			t.Fatalf("applied state regressed for %+v with decision %+v", ev.Order, d)
		}
	}
	leave.Order = dispatch.Order{Date: unchanged.Order.Date + 24*60*60 + 1, ID: 1}
	d = chats.Step(s, leave, now.Add(25*time.Hour), policy)
	if d.Stale || d.Next.Record.State != chats.Removed || d.Next.Record.Watermark != leave.Order || d.Next.Record.MigratedTo != 0 {
		t.Fatalf("new ID cycle departure was never applied %+v", d)
	}
}

func TestCommitAllocatesEpochForInferredReentry(t *testing.T) {
	for _, departed := range []models.ChatMemberType{models.ChatMemberTypeLeft, models.ChatMemberTypeBanned, models.ChatMemberTypeRestricted} {
		t.Run(string(departed), func(t *testing.T) {
			now, policy := lifecycleNow(), lifecyclePolicy()
			s := lifecycleState(chats.Active)
			seq := s.MemberEpoch
			started := chats.Step(s, chats.ProbeTriggered{Trigger: chats.ProbeUnknown}, now.Add(-time.Second), policy)
			s = lifecycleCommit(started, now.Add(-time.Second), &seq)
			oldProbe := s.Probe.InFlight
			join := lifecycleUpdate(1, departed, models.ChatMemberTypeAdministrator, chats.InviteUsers)
			d := chats.Step(s, join, now, policy)
			record := lifecycleRequire[chats.Persist](t, d).Record
			if !d.NewMemberEpoch || d.Next.MemberEpoch != s.MemberEpoch || record.OwnerID != 0 || record.AddedBy != join.FromID ||
				record.AddedAt != now.Unix() || record.UpdatedAt != s.Record.UpdatedAt || record.State != chats.Active || !d.Next.Probe.Pending {
				t.Fatalf("same-state reentry was not proposed correctly %+v", d)
			}
			lifecycleForbid[chats.StartProbe](t, d)
			s = lifecycleCommit(d, now, &seq)
			d = chats.Step(s, lifecycleProbe(oldProbe, models.ChatMemberTypeAdministrator, chats.InviteUsers), now, policy)
			if d.Next.Record.OwnerID != 0 || d.Next.MemberEpoch != seq {
				t.Fatalf("old Probe wrote owner or epoch across reentry %+v", d)
			}
			lifecycleForbid[chats.OnboardingMessage](t, d)
			newProbe := lifecycleRequire[chats.StartProbe](t, d)
			if newProbe.Trigger != chats.ProbeTransition || !newProbe.Actions.Onboarding || newProbe.Actions.LanguageCode != join.LanguageCode {
				t.Fatalf("pending Probe lost reentry context %+v", newProbe)
			}
			s = lifecycleCommit(d, now, &seq)
			if s.Probe.InFlight.MemberEpoch != seq || s.Probe.InFlight.MemberEpoch == oldProbe.MemberEpoch {
				t.Fatalf("Probe did not snapshot committed membership %+v", s.Probe.InFlight)
			}
			d = chats.Step(s, lifecycleProbe(s.Probe.InFlight, models.ChatMemberTypeAdministrator, chats.InviteUsers), now, policy)
			if msg := lifecycleRequire[chats.OnboardingMessage](t, d); !msg.Ephemeral || msg.ReceiverID != join.FromID || msg.MemberEpoch != seq {
				t.Fatalf("onboarding targeted wrong membership %+v", msg)
			}
		})
	}
}

func TestProbeStateAndOwnerHaveSeparateWriteGuards(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	for _, tc := range []struct {
		name        string
		intervening chats.Event
		probeRights chats.BotRights
		wantState   chats.ChatState
		wantOwner   int64
		wantRights  chats.BotRights
	}{
		{"state changed", lifecycleUpdate(1, models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, 0), chats.InviteUsers, chats.Degraded, 13, 0},
		{"owner claimed", chats.OwnerSet{UserID: 30}, 0, chats.Active, 30, chats.InviteUsers},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := lifecycleState(chats.Active)
			seq := s.MemberEpoch
			s = lifecycleCommit(chats.Step(s, chats.ProbeTriggered{Trigger: chats.ProbeUnknown}, now, policy), now, &seq)
			request := s.Probe.InFlight
			s = lifecycleCommit(chats.Step(s, tc.intervening, now, policy), now, &seq)
			d := chats.Step(s, lifecycleProbe(request, models.ChatMemberTypeAdministrator, tc.probeRights), now, policy)
			if d.Next.Record.State != tc.wantState || d.Next.Record.OwnerID != tc.wantOwner || d.Next.Record.BotRights != tc.wantRights || d.Next.MemberEpoch != seq {
				t.Fatalf("wrong outcome for original request %+v after %+v, decision %+v", request, tc.intervening, d)
			}
		})
	}
}

func TestProbeAdmissionAndIdleAfterRecentCompletion(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	for _, tc := range []struct {
		netAge  time.Duration
		idle    bool
		started bool
	}{
		{lifecycleProbeInterval - time.Second, false, false},
		{lifecycleProbeInterval, false, true},
		{lifecycleProbeInterval - time.Nanosecond, true, false},
		{lifecycleProbeInterval, true, true},
	} {
		t.Run(tc.netAge.String()+map[bool]string{true: "/idle", false: "/report"}[tc.idle], func(t *testing.T) {
			s := lifecycleState(chats.Onboarding)
			startedAt := now.Add(-tc.netAge)
			s, request := chats.BeginProbe(s, chats.StartProbe{Trigger: chats.ProbeUnknown}, startedAt)
			s = chats.Step(s, lifecycleProbe(request, models.ChatMemberTypeMember, 0), startedAt.Add(time.Second), policy).Next
			trigger := chats.ProbeUnknown
			if tc.idle {
				trigger = chats.ProbeIdle
			}
			d := chats.Step(s, chats.ProbeTriggered{Trigger: trigger}, now, policy)
			if _, ok := lifecycleEffect[chats.StartProbe](d); ok != tc.started {
				t.Fatalf("start at age %v got %+v", tc.netAge, d.Effects)
			}
			lifecycleForbid[chats.LeaveChat](t, d)
			if d.Next.Probe.IdlePending != (tc.idle && !tc.started) {
				t.Fatalf("unexpected pending idle check: %+v", d)
			}
		})
	}

	recentStart := lifecycleState(chats.Onboarding)
	recentStart.Probe.LastStarted = now.Add(-time.Minute)
	d := chats.Step(recentStart, chats.ProbeTriggered{Trigger: chats.ProbeIdle}, now, policy)
	lifecycleForbid[chats.StartProbe](t, d)
	lifecycleForbid[chats.LeaveChat](t, d)
	if metric := lifecycleRequire[chats.Metric](t, d); metric.Kind != chats.ProbeLimited {
		t.Fatalf("unexpected metric for recent start without recent completion %+v", metric)
	}
	s := lifecycleState(chats.Onboarding)
	seq := s.MemberEpoch
	s = lifecycleCommit(chats.Step(s, chats.ProbeTriggered{Trigger: chats.ProbeUnknown}, now, policy), now, &seq)
	d = chats.Step(s, chats.ProbeTriggered{Trigger: chats.ProbeOwnerStale}, now, policy)
	lifecycleForbid[chats.StartProbe](t, d)
	if d.Next.Probe.Pending || d.Next != s {
		t.Fatalf("overlapping reporter changed state %+v", d)
	}
	d = chats.Step(s, chats.ProbeTriggered{Trigger: chats.ProbeIdle}, now, policy)
	lifecycleForbid[chats.StartProbe](t, d)
	lifecycleForbid[chats.LeaveChat](t, d)
	s = d.Next
	stillOnboarding := chats.Step(s, lifecycleProbe(s.Probe.InFlight, models.ChatMemberTypeMember, 0), now, policy)
	if leave := lifecycleRequire[chats.LeaveChat](t, stillOnboarding); leave.Source != chats.LeaveIdle || leave.Membership != s.MembershipSnapshot() {
		t.Fatalf("in-flight Probe lost attached idle check %+v", stillOnboarding)
	}
	d = chats.Step(s, lifecycleProbe(s.Probe.InFlight, models.ChatMemberTypeAdministrator, chats.InviteUsers), now, policy)
	if d.Next.Record.State != chats.Active {
		t.Fatalf("idle Probe failed to restore active state %+v", d)
	}
	lifecycleForbid[chats.LeaveChat](t, d)
}

func TestPurgeAfterMembershipEvents(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	left := lifecycleUpdate(10, models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, 0)
	joined := lifecycleUpdate(11, models.ChatMemberTypeLeft, models.ChatMemberTypeAdministrator, chats.InviteUsers)
	restricted := joined
	restricted.Old = chats.Membership{Status: models.ChatMemberTypeRestricted}
	downgraded := lifecycleUpdate(11, models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, 0)
	for _, tc := range []struct {
		name       string
		updates    []chats.MemberUpdate
		outcome    telegram.Class
		wantReply  chats.ReplyStatus
		wantState  chats.ChatState
		wantWrites bool
	}{
		{"left then rejoined", []chats.MemberUpdate{left, joined}, telegram.OK, chats.ReplyMembershipChanged, chats.Active, false},
		{"rejoined then stale left", []chats.MemberUpdate{joined, left}, telegram.OK, chats.ReplyMembershipChanged, chats.Active, false},
		{"rejoined from restricted nonmember then stale left", []chats.MemberUpdate{restricted, left}, telegram.OK, chats.ReplyMembershipChanged, chats.Active, false},
		{"permissions changed", []chats.MemberUpdate{downgraded}, telegram.OK, chats.ReplyOK, chats.Removed, true},
		{"left before retry", []chats.MemberUpdate{left}, telegram.Retry, chats.ReplyOK, chats.Removed, true},
		{"still present after retry", nil, telegram.Retry, chats.ReplyNotLeft, chats.Active, false},
		{"unauthorized", nil, telegram.Unauthorized, chats.ReplyNotLeft, chats.Active, false},
		{"fatal", nil, telegram.Fatal, chats.ReplyNotLeft, chats.Active, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := lifecycleState(chats.Active)
			seq := s.MemberEpoch
			snapshot := s.MembershipSnapshot()
			for _, ev := range tc.updates {
				d := chats.Step(s, ev, now, policy)
				s = lifecycleCommit(d, now, &seq)
			}
			result := lifecycleResult(tc.outcome)
			if tc.outcome != telegram.OK {
				result.Err = errors.New("leave request did not complete")
			}
			d := chats.Step(s, chats.LeaveFinished{Source: chats.LeavePurge, Membership: snapshot, Result: result}, now.Add(time.Second), policy)
			reply := lifecycleRequire[chats.Reply](t, d)
			if reply.Operation != chats.ReplyPurge || reply.Status != tc.wantReply || d.Next.Record.State != tc.wantState || d.NewMemberEpoch || d.Next.MemberEpoch != seq ||
				d.Stale != (tc.wantReply == chats.ReplyMembershipChanged) {
				t.Fatalf("Purge returned %+v, proposed %+v", reply, d)
			}
			if tc.wantReply == chats.ReplyNotLeft && reply.Result != result {
				t.Fatalf("Purge discarded sender result: %+v, want %+v", reply.Result, result)
			}
			if tc.outcome != telegram.OK {
				if metric := lifecycleRequire[chats.Metric](t, d); metric.Kind != chats.LeaveOutcome || metric.Outcome != result.Outcome || metric.Error != (tc.outcome != telegram.Retry) {
					t.Fatalf("incorrect failed leave metric: %+v", metric)
				}
			}
			if tc.wantWrites {
				row := lifecycleRequire[chats.Persist](t, d).Record
				if row.PurgeAfter != now.Add(time.Second).Unix() || row.MigratedTo != 0 {
					t.Fatalf("Purge set wrong deadline %+v", row)
				}
				if s.Record.State == chats.Removed {
					want := s.Record
					want.PurgeAfter = row.PurgeAfter
					if row != want {
						t.Fatalf("Purge changed removed record beyond deadline: got %+v want %+v", row, want)
					}
				}
			} else {
				if d.Next != s {
					t.Fatalf("Purge changed the state without a write %+v", d)
				}
			}
			lifecycleForbid[chats.LeaveChat](t, d)
		})
	}
}

func TestIdleLeaveUsesCurrentEpochAndPreservesRemoval(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	s := lifecycleState(chats.Onboarding)
	s.Record.UpdatedAt = now.Add(-policy.IdleLeaveAfter).Unix()
	seq := s.MemberEpoch
	d := chats.Step(s, chats.ProbeTriggered{Trigger: chats.ProbeIdle}, now, policy)
	s = lifecycleCommit(d, now, &seq)
	probe := lifecycleProbe(s.Probe.InFlight, models.ChatMemberTypeMember, 0)
	d = chats.Step(s, probe, now, policy)
	leave := lifecycleRequire[chats.LeaveChat](t, d)
	if leave.Source != chats.LeaveIdle || !leave.NoRetry || leave.Class != sender.Background || leave.Membership != s.MembershipSnapshot() {
		t.Fatalf("idle leave has wrong request %+v", leave)
	}
	s = lifecycleCommit(d, now, &seq)
	for _, tc := range []struct {
		name       string
		update     chats.MemberUpdate
		wantWrites bool
	}{
		{"membership changed", lifecycleUpdate(1, models.ChatMemberTypeLeft, models.ChatMemberTypeMember, 0), false},
		{"member already left", lifecycleUpdate(1, models.ChatMemberTypeMember, models.ChatMemberTypeLeft, 0), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d = chats.Step(s, tc.update, now, policy)
			state := lifecycleCommit(d, now, &seq)
			d = chats.Step(state, chats.LeaveFinished{Source: leave.Source, Membership: leave.Membership, Result: lifecycleResult(telegram.OK)}, now.Add(time.Second), policy)
			if !tc.wantWrites {
				lifecycleForbid[chats.Persist](t, d)
				if d.Next != state || !d.Stale {
					t.Fatalf("old leave changed new membership %+v", d)
				}
				return
			}
			want := state.Record
			want.PurgeAfter = now.Add(time.Second).Unix()
			if row := lifecycleRequire[chats.Persist](t, d).Record; row != want {
				t.Fatalf("idle changed removed record beyond deadline: got %+v want %+v", row, want)
			}
		})
	}
	for _, tc := range []struct {
		name string
		age  time.Duration
		ever bool
	}{
		{"too recent", policy.IdleLeaveAfter - time.Second, false},
		{"previously active", policy.IdleLeaveAfter, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := s
			state.Record.UpdatedAt = now.Add(-tc.age).Unix()
			state.Record.EverActive = tc.ever
			d := chats.Step(state, chats.ProbeTriggered{Trigger: chats.ProbeIdle}, now, policy)
			if len(d.Effects) != 0 || d.Next != state {
				t.Fatalf("ineligible chat started idle work: %+v", d)
			}
		})
	}
	justBeforeIdle := chats.Step(s, chats.ProbeTriggered{Trigger: chats.ProbeIdle}, now.Add(-time.Nanosecond), policy)
	if len(justBeforeIdle.Effects) != 0 || justBeforeIdle.Next != s {
		t.Fatalf("idle work started before the age boundary: %+v", justBeforeIdle)
	}
}

func TestOldAdminBackfillsEverActiveAndStopsIdleLeave(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	s := lifecycleState(chats.Onboarding)
	ev := lifecycleUpdate(1, models.ChatMemberTypeAdministrator, models.ChatMemberTypeMember, 0)
	d := chats.Step(s, ev, now, policy)
	if row := lifecycleRequire[chats.Persist](t, d).Record; !row.EverActive || row.UpdatedAt != s.Record.UpdatedAt || row.State != chats.Onboarding {
		t.Fatalf("same-state update lost historical admin evidence %+v", d)
	}
	d = chats.Step(d.Next, chats.ProbeTriggered{Trigger: chats.ProbeIdle}, now, policy)
	if len(d.Effects) != 0 {
		t.Fatalf("idle requested work for previously active chat %+v", d)
	}
}

func TestRecordlessDeparturePreservesAdministratorEvidence(t *testing.T) {
	admin, member, left := models.ChatMemberTypeAdministrator, models.ChatMemberTypeMember, models.ChatMemberTypeLeft
	for _, recoverBy := range []string{"member update", "Probe"} {
		for _, old := range []models.ChatMemberType{admin, member} {
			t.Run(recoverBy+"/"+string(old), func(t *testing.T) {
				now, policy := lifecycleNow(), lifecyclePolicy()
				state := lifecycleState(chats.NoRecord)
				departure := lifecycleUpdate(2, old, left, 0)
				if old == admin {
					departure.Old.Rights = chats.InviteUsers
				}
				d := chats.Step(state, departure, now, policy)
				lifecycleForbid[chats.Persist](t, d)
				state = d.Next

				promotion := lifecycleUpdate(1, member, admin, chats.InviteUsers)
				stale := chats.Step(state, promotion, now, policy)
				if !stale.Stale || stale.Next != state || len(stale.Effects) != 0 {
					t.Fatalf("older promotion changed the recordless chat: %+v", stale)
				}
				d = chats.Step(state, lifecycleUpdate(3, member, left, 0), now, policy)
				state = d.Next
				if recoverBy == "member update" {
					d = chats.Step(state, lifecycleUpdate(4, left, member, 0), now, policy)
				} else {
					start := chats.Step(state, chats.ProbeTriggered{Trigger: chats.ProbeUnknown}, now, policy)
					state, request := chats.BeginProbe(start.Next, lifecycleRequire[chats.StartProbe](t, start), now)
					d = chats.Step(state, lifecycleProbe(request, member, 0), now, policy)
				}
				row := lifecycleRequire[chats.Persist](t, d).Record
				if row.State != chats.Onboarding || row.EverActive != (old == admin) || d.Next.MemoryEverActive {
					t.Fatalf("record creation retained the wrong administrator history: %+v", d)
				}
			})
		}
	}
}

func TestOldAdminEvidenceSurvivesUpdatesThatCannotAdmitChat(t *testing.T) {
	admin, member, left := models.ChatMemberTypeAdministrator, models.ChatMemberTypeMember, models.ChatMemberTypeLeft
	for _, tc := range []struct {
		name   string
		before chats.ChatState
		new    models.ChatMemberType
	}{
		{"rejected without a record", chats.NoRecord, member},
		{"rejected with a removed record", chats.Removed, member},
		{"unrecognized new status", chats.NoRecord, "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now, policy := lifecycleNow(), lifecyclePolicy()
			policy.AcceptNewChats = false
			state := lifecycleState(tc.before)
			d := chats.Step(state, lifecycleUpdate(1, admin, tc.new, 0), now, policy)
			if d.Next.Record.State != tc.before || d.NewMemberEpoch {
				t.Fatalf("chat was admitted: %+v", d)
			}
			policy.AcceptNewChats = true
			joined := chats.Step(d.Next, lifecycleUpdate(2, left, member, 0), now, policy)
			if row := lifecycleRequire[chats.Persist](t, joined).Record; row.State != chats.Onboarding || !row.EverActive {
				t.Fatalf("joining lost evidence of a prior administrator: %+v", row)
			}
		})
	}
}

func TestIdleCompletionCannotRecreateClearedChat(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	s := lifecycleState(chats.Removed)
	s.MemberEpoch = 0
	snapshot := s.MembershipSnapshot()
	clear := chats.ClearFinished{RemovedAt: s.Record.RemovedAt, MemberEpoch: snapshot.Epoch}
	d := chats.Step(s, clear, now, policy)
	lifecycleRequire[chats.DeleteRecord](t, d)
	s = d.Next
	d = chats.Step(s, chats.LeaveFinished{Source: chats.LeaveIdle, Membership: snapshot, Result: lifecycleResult(telegram.OK)}, now, policy)
	if d.Next != s || len(d.Effects) != 0 || !d.Stale {
		t.Fatalf("idle completion recreated cleared chat %+v", d)
	}
}

func TestClearAndRebuildUseDistinctEpoch(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	s := lifecycleState(chats.Removed)
	seq := s.MemberEpoch
	old := s.MembershipSnapshot()
	clear := chats.ClearFinished{RemovedAt: s.Record.RemovedAt, MemberEpoch: s.MemberEpoch}
	for _, ev := range []chats.ClearFinished{
		{RemovedAt: clear.RemovedAt - 1, MemberEpoch: clear.MemberEpoch},
		{RemovedAt: clear.RemovedAt, MemberEpoch: clear.MemberEpoch - 1},
	} {
		d := chats.Step(s, ev, now, policy)
		if d.Next != s || len(d.Effects) != 0 || !d.Stale {
			t.Fatalf("wrong clearance evidence triggered deletion %+v", d)
		}
	}
	d := chats.Step(s, clear, now, policy)
	if del := lifecycleRequire[chats.DeleteRecord](t, d); del.ChatID != s.Record.ChatID || d.Next.Record.State != chats.NoRecord {
		t.Fatalf("matching clearance did not delete chat %+v", d)
	}
	s = lifecycleCommit(d, now, &seq)
	d = chats.Step(s, lifecycleUpdate(1, models.ChatMemberTypeBanned, models.ChatMemberTypeMember, 0), now, policy)
	s = lifecycleCommit(d, now, &seq)
	if s.MemberEpoch <= old.Epoch || s.Record.State != chats.Onboarding {
		t.Fatalf("rebuilt chat reused membership %+v", s)
	}
	for _, snapshot := range []chats.MembershipSnapshot{old, {}} {
		d = chats.Step(s, chats.LeaveFinished{Source: chats.LeavePurge, Membership: snapshot, Result: lifecycleResult(telegram.OK)}, now, policy)
		if reply := lifecycleRequire[chats.Reply](t, d); reply.Status != chats.ReplyMembershipChanged || d.Next != s {
			t.Fatalf("Purge affected a rebuilt chat %+v", d)
		}
	}
	d = chats.Step(s, clear, now, policy)
	lifecycleForbid[chats.DeleteRecord](t, d)
}

func TestAdmissionDisabledKeepsWatermarkWithoutAcceptingChat(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	policy.AcceptNewChats = false
	for _, state := range []chats.ChatState{chats.NoRecord, chats.Removed} {
		t.Run("from "+string(state), func(t *testing.T) {
			s := lifecycleState(state)
			join := lifecycleUpdate(40, models.ChatMemberTypeLeft, models.ChatMemberTypeMember, 0)
			d := chats.Step(s, join, now, policy)
			if leave := lifecycleRequire[chats.LeaveChat](t, d); leave.Source != chats.LeaveRejected || leave.Class != sender.Background || leave.NoRetry {
				t.Fatalf("emergency switch requested wrong leave %+v", leave)
			}
			if d.Next.Record.State != state || d.NewMemberEpoch || d.Next.MemberEpoch != s.MemberEpoch {
				t.Fatalf("rejected group entered membership %+v", d)
			}
			if state == chats.NoRecord {
				lifecycleForbid[chats.Persist](t, d)
				if water := lifecycleRequire[chats.UpdateWatermark](t, d); water.Order != join.Order {
					t.Fatalf("reject lost in-memory watermark %+v", d)
				}
			} else {
				want := s.Record
				want.Watermark = join.Order
				if row := lifecycleRequire[chats.Persist](t, d).Record; row != want {
					t.Fatalf("reject changed removed record: got %+v want %+v", row, want)
				}
			}
			lifecycleForbid[chats.StartProbe](t, d)
			lifecycleForbid[chats.OnboardingMessage](t, d)
			seq := s.MemberEpoch
			s = lifecycleCommit(d, now, &seq)
			s = lifecycleCommit(chats.Step(s, chats.ProbeTriggered{Trigger: chats.ProbeUnknown}, now, policy), now, &seq)
			d = chats.Step(s, lifecycleProbe(s.Probe.InFlight, models.ChatMemberTypeMember, 0), now, policy)
			lifecycleRequire[chats.LeaveChat](t, d)
			lifecycleForbid[chats.OnboardingMessage](t, d)
			if d.Next.Record.State != state || d.NewMemberEpoch || d.Next.Record.Watermark != s.Record.Watermark || d.Next.MemoryWatermark != s.MemoryWatermark {
				t.Fatalf("rejected Probe moved group or watermark %+v", d)
			}
		})
	}
	for _, state := range []chats.ChatState{chats.Active, chats.Removed} {
		s := lifecycleState(state)
		d := chats.Step(s, lifecycleUpdate(1, models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, 0), now, policy)
		lifecycleForbid[chats.LeaveChat](t, d)
		if row := lifecycleRequire[chats.Persist](t, d).Record; row.State != chats.Removed || row.Watermark.ID != 1 {
			t.Fatalf("emergency switch interfered with departure from %s: %+v", state, row)
		}
	}
	s := lifecycleState(chats.Active)
	d := chats.Step(s, lifecycleUpdate(1, models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, 0), now, policy)
	lifecycleForbid[chats.LeaveChat](t, d)
	if row := lifecycleRequire[chats.Persist](t, d).Record; row.State != chats.Degraded {
		t.Fatalf("emergency switch interfered with existing membership %+v", d)
	}
}

func TestProbeMigrationCopiesWatermarkAndCreatesARecord(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	s := lifecycleState(chats.NoRecord)
	s.MemoryWatermark = dispatch.Order{Date: now.Unix(), ID: 11}
	before := s.MembershipSnapshot()
	seq := uint64(50)
	s = lifecycleCommit(chats.Step(s, chats.ProbeTriggered{Trigger: chats.ProbeUnknown}, now, policy), now, &seq)
	probe := chats.ProbeFinished{
		Request: s.Probe.InFlight,
		Chat:    chats.ProbeChat{Result: sender.Result{Outcome: telegram.Outcome{Class: telegram.Migrated, MigrateTo: -100456}}},
	}
	d := chats.Step(s, probe, now, policy)
	if row := lifecycleRequire[chats.Persist](t, d).Record; row.Watermark != s.MemoryWatermark || d.Next.MemoryWatermark != (dispatch.Order{}) ||
		row.State != chats.Removed || row.MigratedTo != -100456 || d.NewMemberEpoch {
		t.Fatalf("migration lost known watermark or allocated membership %+v", d)
	}
	s = lifecycleCommit(d, now, &seq)
	d = chats.Step(s, chats.LeaveFinished{Source: chats.LeavePurge, Membership: before, Result: lifecycleResult(telegram.OK)}, now, policy)
	if reply := lifecycleRequire[chats.Reply](t, d); reply.Status != chats.ReplyMembershipChanged || d.Next != s {
		t.Fatalf("recordless Purge snapshot matched a new removed record %+v", d)
	}
}

func TestMigrationEnrichmentIsIdempotentAndClearsOnRejoin(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	for _, tc := range []struct {
		name    string
		reason  chats.RemovedReason
		oldTo   int64
		changed bool
	}{
		{"reason only", chats.RemovedKicked, -100456, true},
		{"destination only", chats.RemovedMigrated, -100123, true},
		{"identical report", chats.RemovedMigrated, -100456, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := lifecycleState(chats.Removed)
			s.Record.MigratedTo, s.Record.RemovedReason = tc.oldTo, tc.reason
			d := chats.Step(s, chats.MigrationReported{To: -100456}, now, policy)
			if _, wrote := lifecycleEffect[chats.Persist](d); wrote != tc.changed {
				t.Fatalf("unexpected migration write %+v", d)
			}
			want := s.Record
			want.MigratedTo, want.RemovedReason = -100456, chats.RemovedMigrated
			if d.Next.Record != want || d.NewMemberEpoch {
				t.Fatalf("migration changed unrelated record fields: got %+v want %+v", d.Next.Record, want)
			}
			d = chats.Step(d.Next, lifecycleUpdate(1, models.ChatMemberTypeLeft, models.ChatMemberTypeMember, 0), now, policy)
			if row := lifecycleRequire[chats.Persist](t, d).Record; row.MigratedTo != 0 || row.RemovedAt != 0 || row.RemovedReason != "" || row.PurgeAfter != 0 {
				t.Fatalf("rejoin retained migration metadata %+v", row)
			}
		})
	}
}

func TestOnboardingDeliveryAndCooldown(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	policy.Language = "zh-Hans"
	for _, tc := range []struct {
		name      string
		chatType  models.ChatType
		status    models.ChatMemberType
		rights    chats.BotRights
		last      time.Duration
		want      bool
		ephemeral bool
	}{
		{"ordinary at boundary", models.ChatTypeSupergroup, models.ChatMemberTypeMember, 0, lifecycleMessageCooldown, true, false},
		{"ordinary before boundary", models.ChatTypeSupergroup, models.ChatMemberTypeMember, 0, lifecycleMessageCooldown - time.Second, false, false},
		{"administrator with invite", models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, chats.InviteUsers, time.Second, true, true},
		{"administrator missing invite", models.ChatTypeSupergroup, models.ChatMemberTypeAdministrator, 0, time.Second, true, true},
		{"channel", models.ChatTypeChannel, models.ChatMemberTypeAdministrator, chats.InviteUsers, lifecycleMessageCooldown, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := lifecycleState(chats.Removed)
			s.LastOnboarding = now.Add(-tc.last)
			var seq uint64
			join := lifecycleUpdate(1, models.ChatMemberTypeLeft, tc.status, tc.rights)
			join.Type = tc.chatType
			d := chats.Step(s, join, now, policy)
			s = lifecycleCommit(d, now, &seq)
			probe := lifecycleProbe(s.Probe.InFlight, tc.status, tc.rights)
			probe.Chat.Type, probe.Chat.GuardAssigned, probe.Chat.JoinByRequest = tc.chatType, tc.ephemeral, true
			d = chats.Step(s, probe, now, policy)
			message, ok := lifecycleEffect[chats.OnboardingMessage](d)
			if ok != tc.want {
				t.Fatalf("unexpected onboarding effects %+v", d.Effects)
			}
			if !ok {
				return
			}
			wantReceiver, wantLast := int64(0), now
			if tc.ephemeral {
				wantReceiver, wantLast = join.FromID, s.LastOnboarding
			}
			want := chats.OnboardingMessage{
				Language:   chats.LanguageCandidates{Group: policy.Language, Actor: join.LanguageCode},
				ReceiverID: wantReceiver, MemberEpoch: seq, Ephemeral: tc.ephemeral,
				GuardAssigned: tc.ephemeral, GuardKnown: tc.ephemeral, JoinByRequest: true, NeedsInviteUsers: tc.rights == 0,
			}
			if message != want || d.Next.LastOnboarding != wantLast {
				t.Fatalf("wrong onboarding or cooldown: got %+v at %v, want %+v at %v", message, d.Next.LastOnboarding, want, wantLast)
			}
		})
	}
}

func TestEphemeralFailureFallsBackOnlyForCurrentMembership(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	for _, tc := range []struct {
		name     string
		result   sender.Result
		epoch    uint64
		receiver int64
		chatType models.ChatType
		last     time.Duration
		want     bool
	}{
		{"ordinary fallback", lifecycleResult(telegram.Retry), 6, 5, models.ChatTypeSupergroup, lifecycleMessageCooldown, true},
		{"cooldown", lifecycleResult(telegram.Retry), 6, 5, models.ChatTypeSupergroup, lifecycleMessageCooldown - time.Second, false},
		{"sent", lifecycleResult(telegram.OK), 6, 5, models.ChatTypeSupergroup, lifecycleMessageCooldown, false},
		{"rejoined", lifecycleResult(telegram.Fatal), 5, 5, models.ChatTypeSupergroup, lifecycleMessageCooldown, false},
		{"different recipient", lifecycleResult(telegram.Fatal), 6, 4, models.ChatTypeSupergroup, lifecycleMessageCooldown, false},
		{"channel", lifecycleResult(telegram.Fatal), 6, 5, models.ChatTypeChannel, lifecycleMessageCooldown, false},
		{"dropped", sender.DroppedResult(sender.Expired, errors.New("expired")), 6, 5, models.ChatTypeSupergroup, lifecycleMessageCooldown, true},
		{"shutdown", sender.DroppedResult(sender.Shutdown, errors.New("stopped")), 6, 5, models.ChatTypeSupergroup, lifecycleMessageCooldown, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := lifecycleState(chats.Active)
			s.Record.Type, s.LastOnboarding = tc.chatType, now.Add(-tc.last)
			d := chats.Step(s, chats.OnboardingFinished{MemberEpoch: tc.epoch, ReceiverID: tc.receiver, LanguageCode: "zh-CN", Result: tc.result}, now, policy)
			if d.Stale != (tc.epoch != s.MemberEpoch) {
				t.Fatalf("ephemeral completion staleness = %v for epoch %d", d.Stale, tc.epoch)
			}
			if d.Next.Version != s.Version {
				t.Fatalf("ephemeral completion fact version = %d, want %d", d.Next.Version, s.Version)
			}
			if metric := lifecycleRequire[chats.Metric](t, d); metric.Kind != chats.OnboardingOutcome || metric.Outcome != tc.result.Outcome ||
				metric.Dropped != tc.result.Dropped || metric.Error != (tc.result.Outcome.Class == telegram.Fatal) {
				t.Fatalf("unexpected ephemeral delivery metric %+v", metric)
			}
			message, ok := lifecycleEffect[chats.OnboardingMessage](d)
			if ok != tc.want || ok && (message.Ephemeral || message.ReceiverID != 0 || message.Language != (chats.LanguageCandidates{Actor: "zh-CN"}) || d.Next.LastOnboarding != now) {
				t.Fatalf("wrong fallback effects %+v and state %+v", d.Effects, d.Next)
			}
		})
	}
}

func TestOnboardingStillRunsWhenProbeFailsUnlessAddingUserWasErased(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	join := lifecycleUpdate(1, models.ChatMemberTypeLeft, models.ChatMemberTypeMember, 0)
	seq := uint64(50)
	s := lifecycleCommit(chats.Step(lifecycleState(chats.NoRecord), join, now, policy), now, &seq)
	probe := chats.ProbeFinished{Request: s.Probe.InFlight, Chat: chats.ProbeChat{Result: lifecycleResult(telegram.Fatal)}}
	d := chats.Step(s, probe, now, policy)
	if msg := lifecycleRequire[chats.OnboardingMessage](t, d); msg.GuardKnown || msg.Ephemeral || msg.Language.Actor != join.LanguageCode {
		t.Fatalf("failed Probe lost generic onboarding context %+v", msg)
	}
	s.Record.AddedBy = 0
	d = chats.Step(s, probe, now, policy)
	lifecycleForbid[chats.OnboardingMessage](t, d)
}

func TestDowngradeNotificationCooldownAndChannels(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	policy.Language = "en"
	cases := []struct {
		name   string
		age    time.Duration
		typeOf models.ChatType
		want   bool
	}{
		{"exact boundary", lifecycleMessageCooldown, models.ChatTypeSupergroup, true},
		{"before boundary", lifecycleMessageCooldown - time.Second, models.ChatTypeSupergroup, false},
		{"channel", lifecycleMessageCooldown, models.ChatTypeChannel, false},
	}
	for _, target := range []struct {
		role  models.ChatMemberType
		state chats.ChatState
	}{
		{models.ChatMemberTypeAdministrator, chats.Degraded},
		{models.ChatMemberTypeMember, chats.Onboarding},
	} {
		for _, tc := range cases {
			for _, source := range []string{"member update", "probe"} {
				t.Run(string(target.state)+"/"+tc.name+"/"+source, func(t *testing.T) {
					s := lifecycleState(chats.Active)
					s.LastDowngrade = now.Add(-tc.age)
					actor := ""
					var event chats.Event
					if source == "member update" {
						update := lifecycleUpdate(1, models.ChatMemberTypeAdministrator, target.role, 0)
						update.Type = tc.typeOf
						update.Old.Rights = chats.InviteUsers
						actor, event = update.LanguageCode, update
					} else {
						var request chats.ProbeRequest
						s, request = chats.BeginProbe(s, chats.StartProbe{Trigger: chats.ProbePermissionLost}, now)
						result := lifecycleProbe(request, target.role, 0)
						result.Chat.Type = tc.typeOf
						event = result
					}
					d := chats.Step(s, event, now, policy)
					msg, ok := lifecycleEffect[chats.DowngradeMessage](d)
					last := s.LastDowngrade
					if tc.want {
						last = now
					}
					if ok != tc.want || ok && msg.Language != (chats.LanguageCandidates{Group: "en", Actor: actor}) || d.Next.LastDowngrade != last {
						t.Fatalf("wrong downgrade effect or cooldown %+v", d)
					}
					if d.Next.Record.State != target.state || d.NewMemberEpoch {
						t.Fatalf("downgrade applied wrong membership %+v", d)
					}
					lifecycleForbid[chats.OnboardingMessage](t, d)
				})
			}
		}
	}
}

func TestDowngradeUpdateOvertakesPromotion(t *testing.T) {
	admin, member := models.ChatMemberTypeAdministrator, models.ChatMemberTypeMember
	now, policy := lifecycleNow(), lifecyclePolicy()
	for _, target := range []models.ChatMemberType{member, admin, models.ChatMemberTypeRestricted} {
		t.Run(string(target), func(t *testing.T) {
			state := lifecycleState(chats.Onboarding)
			promoted := lifecycleUpdate(1, member, admin, chats.InviteUsers)
			downgraded := lifecycleUpdate(2, admin, target, 0)
			downgraded.Old = promoted.New
			downgraded.New.IsMember = target == models.ChatMemberTypeRestricted

			d := chats.Step(state, downgraded, now, policy)
			message := lifecycleRequire[chats.DowngradeMessage](t, d)
			row := lifecycleRequire[chats.Persist](t, d).Record
			wantState := chats.Onboarding
			if target == admin {
				wantState = chats.Degraded
			}
			if row.State != wantState || !row.EverActive || row.Watermark != downgraded.Order ||
				message.Language.Actor != downgraded.LanguageCode || d.Next.LastDowngrade != now || d.NewMemberEpoch {
				t.Fatalf("downgrade did not apply the latest Telegram facts: %+v", d)
			}
			stale := chats.Step(d.Next, promoted, now, policy)
			if !stale.Stale || stale.Next != d.Next || len(stale.Effects) != 0 {
				t.Fatalf("older promotion changed the applied downgrade: %+v", stale)
			}
		})
	}
}

func TestReentryAsMemberUsesOnboardingWithoutDowngrade(t *testing.T) {
	member := models.ChatMemberTypeMember
	for _, old := range []models.ChatMemberType{models.ChatMemberTypeLeft, models.ChatMemberTypeBanned, models.ChatMemberTypeRestricted} {
		t.Run(string(old), func(t *testing.T) {
			now, policy := lifecycleNow(), lifecyclePolicy()
			state := lifecycleState(chats.Active)
			join := lifecycleUpdate(1, old, member, 0)
			d := chats.Step(state, join, now, policy)
			lifecycleForbid[chats.DowngradeMessage](t, d)
			if row := lifecycleRequire[chats.Persist](t, d).Record; row.State != chats.Onboarding || row.OwnerID != 0 ||
				row.AddedBy != join.FromID || !d.NewMemberEpoch || d.Next.LastDowngrade != state.LastDowngrade {
				t.Fatalf("reentry was not treated as new membership: %+v", d)
			}
			seq := state.MemberEpoch
			state = lifecycleCommit(d, now, &seq)
			finished := chats.Step(state, lifecycleProbe(state.Probe.InFlight, member, 0), now, policy)
			lifecycleRequire[chats.OnboardingMessage](t, finished)
			lifecycleForbid[chats.DowngradeMessage](t, finished)
		})
	}
}

func TestProbeAllowsOwnerToBeUnknownAndUsesFreshMetadata(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	s := lifecycleState(chats.Active)
	seq := s.MemberEpoch
	s = lifecycleCommit(chats.Step(s, chats.ProbeTriggered{Trigger: chats.ProbeOwnerStale}, now, policy), now, &seq)
	probe := lifecycleProbe(s.Probe.InFlight, models.ChatMemberTypeAdministrator, chats.InviteUsers)
	probe.Chat.GuardAssigned, probe.Chat.JoinByRequest, probe.Chat.Title = true, true, "Renamed"
	probe.Admins.CreatorID = 0
	d := chats.Step(s, probe, now, policy)
	if row := lifecycleRequire[chats.Persist](t, d).Record; row.OwnerID != 0 || !row.GuardAssigned || row.Title != "Renamed" ||
		!d.Next.GuardKnown || !d.Next.JoinByRequest || row.UpdatedAt != s.Record.UpdatedAt || d.Next.Version != s.Version {
		t.Fatalf("creator absence or getChat metadata did not apply correctly %+v", d)
	}
	probe.Admins.Result = lifecycleResult(telegram.Permission)
	d = chats.Step(s, probe, now, policy)
	if row := lifecycleRequire[chats.Persist](t, d).Record; row.OwnerID != s.Record.OwnerID || !row.GuardAssigned || row.Title != "Renamed" {
		t.Fatalf("failed administrator query updated owner or lost chat metadata %+v", d)
	}
}

func TestProbeResultsIgnoreOtherRequestsAndFlagUnknownMembership(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	s := lifecycleState(chats.Active)
	s, request := chats.BeginProbe(s, chats.StartProbe{Trigger: chats.ProbeUnknown}, now)
	before := request
	before.StartedAt = before.StartedAt.Add(-time.Minute)
	d := chats.Step(s, lifecycleProbe(before, models.ChatMemberTypeMember, 0), now, policy)
	if metric := lifecycleRequire[chats.Metric](t, d); metric.Kind != chats.ProbeStaleResult || metric.Trigger != before.Trigger || d.Next != s || len(d.Effects) != 1 {
		t.Fatalf("mismatched completion consumed current Probe %+v", d)
	}
	probe := lifecycleProbe(request, models.ChatMemberType("future_status"), 0)
	probe.Admins = chats.ProbeAdmins{}
	d = chats.Step(s, probe, now, policy)
	found := false
	for _, effect := range d.Effects {
		if metric, ok := effect.(chats.Metric); ok && metric.Kind == chats.InvalidMember && metric.Error {
			found = true
		}
	}
	if !found || d.Next.Record != s.Record {
		t.Fatalf("unknown membership was accepted or unreported %+v", d)
	}
	lifecycleForbid[chats.Persist](t, d)
}

func TestProbeCanNotifyOfDowngrade(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	s := lifecycleState(chats.Active)
	s, request := chats.BeginProbe(s, chats.StartProbe{Trigger: chats.ProbePermissionLost}, now)
	d := chats.Step(s, lifecycleProbe(request, models.ChatMemberTypeAdministrator, 0), now, policy)
	lifecycleRequire[chats.DowngradeMessage](t, d)
	if row := lifecycleRequire[chats.Persist](t, d).Record; row.State != chats.Degraded || d.NewMemberEpoch || d.Next.LastDowngrade != now {
		t.Fatalf("Probe downgrade did not update record and notify %+v", d)
	}
}

func TestStepPreservesInput(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	for _, state := range []chats.ChatState{chats.NoRecord, chats.Active, chats.Removed} {
		s := lifecycleState(state)
		s, request := chats.BeginProbe(s, chats.StartProbe{Trigger: chats.ProbeUnknown, Actions: chats.FollowUp{Idle: true}}, now)
		before := s
		events := []chats.Event{
			lifecycleUpdate(1, models.ChatMemberTypeLeft, models.ChatMemberTypeAdministrator, chats.InviteUsers),
			lifecycleProbe(request, models.ChatMemberTypeMember, 0),
			chats.LeaveFinished{Source: chats.LeavePurge, Membership: s.MembershipSnapshot(), Result: lifecycleResult(telegram.OK)},
			chats.ClearFinished{RemovedAt: s.Record.RemovedAt, MemberEpoch: s.MemberEpoch},
			chats.OwnerSet{UserID: 22},
		}
		for _, ev := range events {
			chats.Step(s, ev, now, policy)
			if !reflect.DeepEqual(s, before) {
				t.Fatalf("event %T changed input: got %+v, want %+v", ev, s, before)
			}
		}
	}
}

func TestSameStateReentryDiscardsOlderProbeState(t *testing.T) {
	now, policy := lifecycleNow(), lifecyclePolicy()
	s := lifecycleState(chats.Active)
	seq := s.MemberEpoch
	s = lifecycleCommit(chats.Step(s, chats.ProbeTriggered{Trigger: chats.ProbeUnknown}, now, policy), now, &seq)
	oldProbe := s.Probe.InFlight
	s = lifecycleCommit(chats.Step(s, lifecycleUpdate(1, models.ChatMemberTypeLeft, models.ChatMemberTypeAdministrator, chats.InviteUsers), now, policy), now, &seq)
	d := chats.Step(s, lifecycleProbe(oldProbe, models.ChatMemberTypeMember, 0), now, policy)
	if d.Next.Record.State != chats.Active || d.Next.Record.BotRights != chats.InviteUsers {
		t.Fatalf("Probe started before reentry changed state %+v", d.Next.Record)
	}
}
