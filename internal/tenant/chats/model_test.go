package chats_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
	"github.com/INKCR0W/yabot/internal/tenant/chats"
)

type referenceChat struct {
	status           chats.ChatState
	watermark        dispatch.Order
	owner            int64
	epoch            uint64
	sequence         uint64
	version          uint64
	claimCount       uint64
	rights           chats.BotRights
	hadAdmin         bool
	chatType         models.ChatType
	title            string
	guardAssigned    bool
	guardKnown       bool
	joinByRequest    bool
	migratedTo       int64
	removedReason    chats.RemovedReason
	updatedAt        time.Time
	addedAt          time.Time
	removedAt        time.Time
	purgeAfter       int64
	request          chats.ProbeRequest
	offeredProbe     chats.ProbeTrigger
	offeredGuide     bool
	guideAfterProbe  bool
	repeatProbe      bool
	guideAfterRepeat bool
	idleAfterProbe   bool
	idleWaiting      bool
	idleLeaveEpoch   uint64
	idleLeaveVersion uint64
	lastProbe        time.Time
	lastDowngrade    time.Time
}

type modelSnapshot struct {
	status        chats.ChatState
	rights        chats.BotRights
	hadAdmin      bool
	watermark     dispatch.Order
	owner         int64
	epoch         uint64
	version       uint64
	claimCount    uint64
	addedAt       int64
	purgeAfter    int64
	chatType      models.ChatType
	title         string
	guardBot      bool
	guardKnown    bool
	joinByRequest bool
	migratedTo    int64
}

func (m referenceChat) snapshot() modelSnapshot {
	var addedAt int64
	if !m.addedAt.IsZero() {
		addedAt = m.addedAt.Unix()
	}
	return modelSnapshot{
		status: m.status, rights: m.rights, hadAdmin: m.hadAdmin, watermark: m.watermark, owner: m.owner, epoch: m.epoch, version: m.version, claimCount: m.claimCount,
		chatType: m.chatType, title: m.title, guardBot: m.guardAssigned, guardKnown: m.guardKnown,
		joinByRequest: m.joinByRequest, migratedTo: m.migratedTo, addedAt: addedAt, purgeAfter: m.purgeAfter,
	}
}

func (m *referenceChat) apply(event chats.Event, now time.Time) ([]string, bool) {
	effects, stale := m.applyEvent(event, now)
	if _, isCompletion := event.(chats.ProbeFinished); !isCompletion &&
		(slices.Contains(effects, "persist") || slices.Contains(effects, "memory-watermark") || slices.Contains(effects, "delete")) {
		m.version++
	}
	if m.offeredProbe != "" {
		if m.idleWaiting {
			m.idleAfterProbe = m.isIdle(now)
			m.idleWaiting = false
		}
		m.lastProbe = now
		m.guideAfterProbe = m.offeredGuide
		m.request = chats.ProbeRequest{
			Trigger: m.offeredProbe, Version: m.version, OwnerRevision: m.claimCount,
			MemberEpoch: m.epoch, StartedAt: now,
		}
		m.offeredProbe = ""
		m.offeredGuide = false
	}
	return effects, stale
}

func (m *referenceChat) applyEvent(event chats.Event, now time.Time) ([]string, bool) {
	if update, ok := event.(chats.MemberUpdate); ok {
		return m.memberUpdate(update, now)
	}
	if claim, ok := event.(chats.OwnerSet); ok {
		if !hasReferenceMembership(m.status) {
			return []string{"reply"}, false
		}
		m.claimCount++
		if m.owner == claim.UserID {
			return []string{"reply"}, false
		}
		m.owner = claim.UserID
		return []string{"persist", "reply"}, false
	}
	if trigger, ok := event.(chats.ProbeTriggered); ok {
		return m.requestProbe(trigger.Trigger, now), false
	}
	if completion, ok := event.(chats.ProbeFinished); ok {
		return m.completeProbe(completion, now), false
	}
	if migration, ok := event.(chats.MigrationReported); ok {
		if m.migrate(migration.To, now) {
			return []string{"persist"}, false
		}
		return nil, false
	}
	if completed, ok := event.(chats.LeaveFinished); ok {
		return m.finishIdleLeave(completed, now)
	}
	if clear, ok := event.(chats.ClearFinished); ok {
		if m.status != chats.Removed || clear.RemovedAt != m.removedAt.Unix() || clear.MemberEpoch != m.epoch {
			return nil, true
		}
		m.status, m.watermark, m.epoch, m.claimCount = chats.NoRecord, dispatch.Order{}, 0, 0
		m.rights, m.owner, m.hadAdmin = 0, 0, false
		m.chatType, m.title, m.migratedTo, m.removedReason = "", "", 0, ""
		m.guardKnown, m.guardAssigned, m.joinByRequest = false, false, false
		m.addedAt, m.updatedAt, m.removedAt = time.Time{}, time.Time{}, time.Time{}
		m.purgeAfter = 0
		m.lastDowngrade = time.Time{}
		m.repeatProbe, m.guideAfterRepeat = false, false
		m.guideAfterProbe = false
		m.idleAfterProbe, m.idleWaiting = false, false
		return []string{"delete"}, false
	}
	panic(fmt.Sprintf("unsupported reference event %T", event))
}

func (m *referenceChat) memberUpdate(event chats.MemberUpdate, now time.Time) ([]string, bool) {
	idWindowSeconds := int64(24 * 60 * 60)
	difference := event.Order.Date - m.watermark.Date
	newer := event.Order.ID > m.watermark.ID
	if difference < -idWindowSeconds || difference > idWindowSeconds {
		newer = event.Order.Date > m.watermark.Date
	}
	if !newer {
		return nil, true
	}
	m.watermark = event.Order

	previous := m.status
	incoming := referenceMembershipStatus(event.New)
	if event.Old.Status == models.ChatMemberTypeAdministrator {
		m.hadAdmin = true
	}
	if previous == chats.NoRecord && incoming == chats.Removed {
		return []string{"memory-watermark"}, false
	}
	effects := []string{"persist"}
	if previous != incoming {
		m.updatedAt = now
	}
	m.status = incoming
	m.chatType, m.title = event.Type, event.Title
	if incoming != chats.Removed {
		m.rights = event.New.Rights
	}
	if incoming == chats.Active {
		m.hadAdmin = true
	}
	if !hasReferenceMembership(incoming) {
		if previous != chats.Removed {
			m.rights, m.removedAt, m.removedReason = 0, now, chats.RemovedLeft
			m.purgeAfter = now.Add(referenceRemovedRetention).Unix()
			if event.New.Status == models.ChatMemberTypeBanned {
				m.removedReason = chats.RemovedKicked
			}
		}
		return effects, false
	}

	wasOutside := event.Old.Status == models.ChatMemberTypeLeft || event.Old.Status == models.ChatMemberTypeBanned ||
		event.Old.Status == models.ChatMemberTypeRestricted && !event.Old.IsMember
	startsMembership := !hasReferenceMembership(previous) || wasOutside
	if startsMembership {
		if wasOutside {
			m.addedAt = now
		}
		m.owner = 0
		m.guardKnown, m.guardAssigned, m.joinByRequest = false, false, false
		m.migratedTo, m.removedAt, m.removedReason = 0, time.Time{}, ""
		m.purgeAfter = 0
		m.advanceEpoch()
	}
	if startsMembership || previous != incoming {
		if m.request.Trigger != "" {
			m.repeatProbe = true
			m.guideAfterRepeat = m.guideAfterRepeat || startsMembership
		} else {
			m.offerProbe(chats.ProbeTransition, startsMembership)
			effects = append(effects, "probe")
		}
	}
	return m.downgrade(referenceMembershipStatus(event.Old), now, effects), false
}

func (m *referenceChat) requestProbe(trigger chats.ProbeTrigger, now time.Time) []string {
	idle := trigger == chats.ProbeIdle
	if idle && !m.isIdle(now) {
		m.idleWaiting = false
		return nil
	}
	if trigger == chats.ProbePeriodic && !hasReferenceMembership(m.status) {
		return nil
	}
	if m.request.Trigger != "" {
		m.idleAfterProbe = m.idleAfterProbe || idle
		return []string{"metric"}
	}
	if !m.lastProbe.IsZero() && now.Before(m.lastProbe.Add(10*time.Minute)) {
		m.idleWaiting = m.idleWaiting || idle
		return []string{"metric"}
	}
	m.offerProbe(trigger, false)
	m.idleAfterProbe = m.idleAfterProbe || idle
	return []string{"probe"}
}

func (m *referenceChat) offerProbe(trigger chats.ProbeTrigger, guide bool) {
	m.offeredProbe = trigger
	m.offeredGuide = guide
}

func (m *referenceChat) completeProbe(event chats.ProbeFinished, now time.Time) []string {
	if m.request.Trigger == "" || m.request != event.Request {
		return []string{"metric"}
	}
	previous := m.snapshot()
	wasIdle := m.idleAfterProbe && m.isIdle(now)
	previousRemoval := m.removedReason
	outcome := referenceProbeStatus(event)
	effects := []string{"metric"}

	current := m.version == event.Request.Version
	discoversMembership := current && hasReferenceMembership(outcome) && !hasReferenceMembership(previous.status)
	if !current {
		effects = append(effects, "metric")
	}
	if current && outcome == chats.Removed {
		for _, stage := range []sender.Result{event.Chat.Result, event.Member.Result, event.Admins.Result} {
			if stage.Outcome.Class == telegram.Migrated {
				m.migrate(stage.Outcome.MigrateTo, now)
				break
			}
		}
		if hasReferenceMembership(m.status) {
			m.assignStatus(chats.Removed, now)
			m.rights = 0
			m.removedAt = now
			m.purgeAfter = now.Add(referenceRemovedRetention).Unix()
		}
	}
	if current && hasReferenceMembership(outcome) {
		if discoversMembership {
			m.advanceEpoch()
			m.addedAt = now
			m.migratedTo, m.removedAt, m.removedReason = 0, time.Time{}, ""
			m.purgeAfter = 0
			m.owner = 0
			m.guardKnown, m.guardAssigned, m.joinByRequest = false, false, false
		}
		m.assignStatus(outcome, now)
		m.rights = event.Member.Membership.Rights
	}
	chatRead := event.Chat.Result
	if current && m.status != chats.NoRecord && chatRead.Outcome.Class == telegram.OK && chatRead.Dropped == 0 {
		m.chatType, m.title, m.joinByRequest = event.Chat.Type, event.Chat.Title, event.Chat.JoinByRequest
		memberRead := event.Member.Result
		confirmedAdmin := memberRead.Dropped == 0 && memberRead.Outcome.Class == telegram.OK &&
			event.Member.Membership.Status == models.ChatMemberTypeAdministrator
		m.guardKnown = event.Chat.GuardVisible || confirmedAdmin
		if m.guardKnown {
			m.guardAssigned = event.Chat.GuardAssigned
		}
	}
	unchangedOwner := m.claimCount == event.Request.OwnerRevision && m.epoch == event.Request.MemberEpoch
	if (discoversMembership || unchangedOwner && hasReferenceMembership(m.status) && hasReferenceMembership(outcome)) && !event.SkipOwner {
		m.owner = event.Admins.CreatorID
	}
	if m.status != previous.status || m.owner != previous.owner || m.chatType != previous.chatType || m.title != previous.title ||
		m.migratedTo != previous.migratedTo || m.removedReason != previousRemoval || m.rights != previous.rights ||
		m.hadAdmin != previous.hadAdmin || m.guardAssigned != previous.guardBot {
		effects = append(effects, "persist")
	}

	if current {
		effects = m.downgrade(previous.status, now, effects)
	}
	inMembershipThatRequestedGuide := m.guideAfterProbe && m.epoch == event.Request.MemberEpoch
	if (discoversMembership || inMembershipThatRequestedGuide) && hasReferenceMembership(m.status) && m.chatType != models.ChatTypeChannel {
		effects = append(effects, "onboarding")
	}
	if wasIdle {
		if !current {
			m.idleWaiting = true
		} else if hasReferenceMembership(outcome) && (m.status == chats.Onboarding || m.status == chats.Degraded) &&
			!m.hadAdmin && m.epoch == event.Request.MemberEpoch {
			m.idleLeaveEpoch, m.idleLeaveVersion = m.epoch, m.version
			effects = append(effects, "leave")
		}
	}
	m.idleAfterProbe = false

	m.request = chats.ProbeRequest{}
	if m.repeatProbe {
		m.offerProbe(chats.ProbeTransition, m.guideAfterRepeat)
		m.repeatProbe = false
		m.guideAfterRepeat = false
		effects = append(effects, "probe")
	} else if m.idleWaiting && !now.Before(m.lastProbe.Add(10*time.Minute)) {
		m.offerProbe(chats.ProbeIdle, false)
		effects = append(effects, "probe")
	}
	return effects
}

func (m *referenceChat) migrate(to int64, now time.Time) bool {
	if m.status == chats.Removed && m.removedReason == chats.RemovedMigrated && m.migratedTo == to {
		return false
	}
	if m.status != chats.Removed {
		m.removedAt = now
		m.purgeAfter = now.Add(referenceRemovedRetention).Unix()
	}
	m.assignStatus(chats.Removed, now)
	m.removedReason = chats.RemovedMigrated
	m.migratedTo = to
	m.rights = 0
	return true
}

const (
	referenceIdleAfter        = 2 * time.Minute
	referenceRemovedRetention = 30 * 24 * time.Hour
)

func (m *referenceChat) isIdle(now time.Time) bool {
	return !m.hadAdmin && (m.status == chats.Onboarding || m.status == chats.Degraded) &&
		now.Sub(m.updatedAt) >= referenceIdleAfter && now.Sub(m.addedAt) >= referenceIdleAfter
}

func (m *referenceChat) assignStatus(status chats.ChatState, now time.Time) {
	if m.status != status {
		m.updatedAt = now
	}
	m.status = status
	if status == chats.Active {
		m.hadAdmin = true
	}
}

func (m *referenceChat) advanceEpoch() {
	m.sequence++
	m.epoch = m.sequence
}

func (m *referenceChat) finishIdleLeave(event chats.LeaveFinished, now time.Time) ([]string, bool) {
	if event.Source != chats.LeaveIdle {
		panic(fmt.Sprintf("unsupported leave source %s", event.Source))
	}
	if m.status == chats.NoRecord || !event.Membership.Exists || event.Membership.Epoch != m.epoch {
		return nil, true
	}
	confirmedIdleDeparture := m.status == chats.Removed && !m.hadAdmin
	class := event.Result.Outcome.Class
	left := event.Result.Dropped == 0 &&
		(class == telegram.OK || class == telegram.Forbidden || class == telegram.Done || class == telegram.Migrated)
	if !confirmedIdleDeparture {
		if m.version != event.FactVersion || m.hadAdmin || (m.status != chats.Onboarding && m.status != chats.Degraded) {
			return nil, true
		}
		if !left {
			return []string{"metric"}, false
		}
	}
	if m.status == chats.Removed {
		if m.purgeAfter == now.Unix() {
			return nil, false
		}
		m.purgeAfter = now.Unix()
		return []string{"persist"}, false
	}
	m.assignStatus(chats.Removed, now)
	m.rights, m.removedAt, m.removedReason = 0, now, chats.RemovedByIdle
	if class == telegram.Migrated {
		m.migratedTo, m.removedReason = event.Result.Outcome.MigrateTo, chats.RemovedMigrated
	}
	m.purgeAfter = now.Unix()
	return []string{"persist"}, false
}

func (m *referenceChat) downgrade(previous chats.ChatState, now time.Time, effects []string) []string {
	if previous == chats.Active && (m.status == chats.Onboarding || m.status == chats.Degraded) && m.chatType != models.ChatTypeChannel {
		if m.lastDowngrade.IsZero() || !now.Before(m.lastDowngrade.Add(24*time.Hour)) {
			m.lastDowngrade = now
			return append(effects, "downgrade")
		}
	}
	return effects
}

func hasReferenceMembership(status chats.ChatState) bool {
	return status == chats.Active || status == chats.Degraded || status == chats.Onboarding
}

func referenceMembershipStatus(member chats.Membership) chats.ChatState {
	if member.Status == models.ChatMemberTypeAdministrator {
		if member.Rights&chats.InviteUsers != 0 {
			return chats.Active
		}
		return chats.Degraded
	}
	if member.Status == models.ChatMemberTypeMember {
		return chats.Onboarding
	}
	if member.Status == models.ChatMemberTypeRestricted && member.IsMember {
		return chats.Onboarding
	}
	return chats.Removed
}

func referenceProbeStatus(event chats.ProbeFinished) chats.ChatState {
	for _, response := range []sender.Result{event.Chat.Result, event.Member.Result} {
		if response.Outcome.Class == telegram.Forbidden || response.Outcome.Class == telegram.Migrated ||
			(response.Outcome.Class == telegram.Done && response.Outcome.Reason == telegram.ChatGone) {
			return chats.Removed
		}
		if response.Outcome.Class != telegram.OK {
			return chats.NoRecord
		}
	}
	membership := referenceMembershipStatus(event.Member.Membership)
	if membership == chats.Removed {
		return chats.Removed
	}
	if event.Admins.Result.Outcome.Class == telegram.Forbidden || event.Admins.Result.Outcome.Class == telegram.Migrated ||
		(event.Admins.Result.Outcome.Class == telegram.Done && event.Admins.Result.Outcome.Reason == telegram.ChatGone) {
		return chats.Removed
	}
	if event.Admins.Result.Outcome.Class != telegram.OK {
		return chats.NoRecord
	}
	return membership
}

type modelEvents struct {
	random  *rand.Rand
	updates []chats.MemberUpdate
}

func (e *modelEvents) next(m referenceChat, index int, now time.Time, chatType models.ChatType) chats.Event {
	draw := e.random.IntN(100)
	if draw < 49 {
		if len(e.updates) != 0 && e.random.IntN(5) == 0 {
			return e.updates[e.random.IntN(len(e.updates))]
		}
		date := now.Add(-time.Duration(e.random.IntN(24*60*60)) * time.Second).Unix()
		id := int64(index + 200 + e.random.IntN(51) - 25)
		update := chats.MemberUpdate{
			Order: dispatch.Order{Date: date, ID: id}, Type: chatType, Title: fmt.Sprintf("Update %d", index%5),
			Old: e.member(), New: e.member(),
		}
		e.updates = append(e.updates, update)
		return update
	}
	if draw < 67 && m.request.Trigger != "" {
		return e.probeResult(m.request, chatType)
	}
	if draw < 80 {
		return chats.OwnerSet{UserID: int64(1 + e.random.IntN(10))}
	}
	if draw < 84 {
		return chats.MigrationReported{To: -int64(1 + e.random.IntN(3))}
	}
	if draw < 87 && m.epoch != 0 {
		return chats.LeaveFinished{Source: chats.LeaveIdle, Membership: chats.MembershipSnapshot{Exists: true, Epoch: m.epoch - 1},
			FactVersion: m.version, Result: sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}}}
	}
	triggers := []chats.ProbeTrigger{chats.ProbeUnknown, chats.ProbePermissionLost, chats.ProbeOwnerStale, chats.ProbeIdle}
	return chats.ProbeTriggered{Trigger: triggers[e.random.IntN(len(triggers))]}
}

func (e *modelEvents) member() chats.Membership {
	members := []chats.Membership{
		{Status: models.ChatMemberTypeAdministrator, Rights: chats.InviteUsers},
		{Status: models.ChatMemberTypeAdministrator, Rights: chats.InviteUsers | chats.ManageChat},
		{Status: models.ChatMemberTypeAdministrator, Rights: chats.ManageChat},
		{Status: models.ChatMemberTypeMember},
		{Status: models.ChatMemberTypeRestricted, IsMember: true},
		{Status: models.ChatMemberTypeRestricted},
		{Status: models.ChatMemberTypeLeft},
		{Status: models.ChatMemberTypeBanned},
	}
	return members[e.random.IntN(len(members))]
}

func (e *modelEvents) probeResult(request chats.ProbeRequest, chatType models.ChatType) chats.ProbeFinished {
	ok := sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}}
	result := chats.ProbeFinished{
		Request: request,
		Chat: chats.ProbeChat{
			Result: ok, Type: chatType, Title: fmt.Sprintf("Probe %d", e.random.IntN(3)),
			GuardAssigned: e.random.IntN(2) == 0, JoinByRequest: e.random.IntN(2) == 0,
		},
		Member: chats.ProbeMember{Result: ok, Membership: e.member()},
		Admins: chats.ProbeAdmins{Result: ok, CreatorID: int64(e.random.IntN(10))},
	}
	result.Chat.GuardVisible = result.Chat.GuardAssigned && e.random.IntN(2) == 0
	if referenceMembershipStatus(result.Member.Membership) == chats.Removed {
		result.Admins = chats.ProbeAdmins{}
	}
	switch e.random.IntN(12) {
	case 0:
		result.Chat = chats.ProbeChat{Result: sender.Result{Outcome: telegram.Outcome{Class: telegram.Retry}}}
		result.Member = chats.ProbeMember{}
		result.Admins = chats.ProbeAdmins{}
	case 1:
		result.Member = chats.ProbeMember{Result: sender.Result{
			Outcome: telegram.Outcome{Class: telegram.Retry}, Dropped: sender.Busy,
		}}
		result.Admins = chats.ProbeAdmins{}
	case 2:
		result.Member = chats.ProbeMember{Result: sender.Result{Outcome: telegram.Outcome{Class: telegram.Forbidden}}}
		result.Admins = chats.ProbeAdmins{}
	case 3:
		if result.Admins.Result.Outcome.Class == telegram.OK {
			result.Admins = chats.ProbeAdmins{Result: sender.Result{Outcome: telegram.Outcome{Class: telegram.Permission}}}
		}
	case 4:
		if result.Admins.Result.Outcome.Class == telegram.OK {
			result.Admins = chats.ProbeAdmins{Result: sender.Result{
				Outcome: telegram.Outcome{Class: telegram.Done, Reason: telegram.ChatGone},
			}}
		}
	case 5:
		result.Chat = chats.ProbeChat{Result: sender.Result{Outcome: telegram.Outcome{Class: telegram.Migrated, MigrateTo: -4}}}
		result.Member = chats.ProbeMember{}
		result.Admins = chats.ProbeAdmins{}
	case 6, 7:
		result.SkipOwner = result.Admins.CreatorID != 0
	}
	return result
}

func modelEffectKinds(effects []chats.Effect) []string {
	var kinds []string
	for _, effect := range effects {
		switch effect.(type) {
		case chats.Persist:
			kinds = append(kinds, "persist")
		case chats.UpdateWatermark:
			kinds = append(kinds, "memory-watermark")
		case chats.DeleteRecord:
			kinds = append(kinds, "delete")
		case chats.StartProbe:
			kinds = append(kinds, "probe")
		case chats.Reply:
			kinds = append(kinds, "reply")
		case chats.Metric:
			kinds = append(kinds, "metric")
		case chats.OnboardingMessage:
			kinds = append(kinds, "onboarding")
		case chats.DowngradeMessage:
			kinds = append(kinds, "downgrade")
		case chats.LeaveChat:
			kinds = append(kinds, "leave")
		default:
			kinds = append(kinds, fmt.Sprintf("%T", effect))
		}
	}
	slices.Sort(kinds)
	return kinds
}

func TestLifecycleAgainstReferenceModel(t *testing.T) {
	t.Parallel()
	for _, chatType := range []models.ChatType{models.ChatTypeSupergroup, models.ChatTypeChannel} {
		for _, seed := range []uint64{71, 306, 1279} {
			t.Run(fmt.Sprintf("%s/%d", chatType, seed), func(t *testing.T) {
				t.Parallel()
				runReferenceSequence(t, chatType, seed, nil)
				for _, scenario := range referenceScenarios() {
					t.Run(scenario.name, func(t *testing.T) {
						runReferenceSequence(t, chatType, seed, scenario.steps)
					})
				}
			})
		}
	}
}

type referenceEvent func(referenceChat, time.Time, models.ChatType) chats.Event

type referenceStep struct {
	wait  time.Duration
	event referenceEvent
}

type referenceScenario struct {
	name  string
	steps []referenceStep
}

func referenceScenarios() []referenceScenario {
	member := chats.Membership{Status: models.ChatMemberTypeMember}
	left := chats.Membership{Status: models.ChatMemberTypeLeft}
	admin := chats.Membership{Status: models.ChatMemberTypeAdministrator, Rights: chats.InviteUsers}
	adminWithDelete := chats.Membership{Status: models.ChatMemberTypeAdministrator, Rights: chats.InviteUsers | chats.DeleteMessages}
	degraded := chats.Membership{Status: models.ChatMemberTypeAdministrator}
	const nextProbe = 10 * time.Minute
	unknown := referenceTrigger(chats.ProbeUnknown)
	idle := referenceTrigger(chats.ProbeIdle)
	result := referenceCompletion(member)
	leaveRetry := referenceIdleLeave(telegram.Retry)
	leaveOK := referenceIdleLeave(telegram.OK)
	clear := func(m referenceChat, _ time.Time, _ models.ChatType) chats.Event {
		return chats.ClearFinished{RemovedAt: m.removedAt.Unix(), MemberEpoch: m.epoch}
	}
	failure := func(m referenceChat, _ time.Time, _ models.ChatType) chats.Event {
		return chats.ProbeFinished{Request: m.request, Chat: chats.ProbeChat{Result: lifecycleResult(telegram.Retry)}}
	}
	scenarios := []referenceScenario{
		{"recordless departure", []referenceStep{
			{0, unknown},
			{0, referenceMemberUpdate(1, member, left)},
			{0, result},
		}},
		{"aged onboarding reentry", []referenceStep{
			{0, referenceMemberUpdate(1, left, member)},
			{0, result},
			{8 * 24 * time.Hour, unknown},
			{0, referenceMemberUpdate(2, left, member)},
			{0, referenceCompletion(admin)},
			{0, idle},
			{0, result},
		}},
		{"same state rights update", []referenceStep{
			{0, referenceMemberUpdate(1, left, admin)},
			{0, referenceCompletion(admin)},
			{nextProbe, referenceTrigger(chats.ProbePeriodic)},
			{0, referenceMemberUpdate(2, admin, adminWithDelete)},
			{0, referenceCompletion(admin)},
		}},
		{"idle verification superseded by a newer fact", []referenceStep{
			{0, unknown},
			{0, result},
			{nextProbe, unknown},
			{0, idle},
			{0, referenceMemberUpdate(1, member, member)},
			{0, result},
			{nextProbe, idle},
			{0, result},
		}},
		{"idle leave superseded by a newer watermark", []referenceStep{
			{0, referenceMemberUpdate(1, left, member)},
			{0, result},
			{nextProbe, idle},
			{0, result},
			{0, referenceMemberUpdate(2, member, member)},
			{0, leaveOK},
		}},
		{"idle leave with departure before an uncertain result", []referenceStep{
			{0, referenceMemberUpdate(1, left, member)},
			{0, result},
			{nextProbe, idle},
			{0, result},
			{0, referenceMemberUpdate(2, member, left)},
			{0, leaveRetry},
		}},
		{"idle leave with promotion and departure before the result", []referenceStep{
			{0, referenceMemberUpdate(1, left, member)},
			{0, result},
			{nextProbe, idle},
			{0, result},
			{0, referenceMemberUpdate(2, member, admin)},
			{0, referenceMemberUpdate(3, admin, left)},
			{0, leaveOK},
		}},
		{"clear with pending completion", []referenceStep{
			{0, referenceMemberUpdate(1, left, member)},
			{0, result},
			{0, referenceMemberUpdate(2, member, left)},
			{nextProbe, unknown},
			{0, clear},
			{0, unknown},
			{0, result},
			{nextProbe, unknown},
		}},
	}
	for _, observed := range []chats.Membership{member, degraded} {
		confirm := referenceCompletion(observed)
		scenarios = append(scenarios, referenceScenario{"idle Probe and leave retry " + string(observed.Status), []referenceStep{
			{0, unknown},
			{0, confirm},
			{nextProbe, idle},
			{0, failure},
			{0, idle},
			{nextProbe, idle},
			{0, confirm},
			{0, idle},
			{0, leaveRetry},
			{time.Hour, idle},
			{0, confirm},
		}})
	}
	return scenarios
}

func referenceTrigger(trigger chats.ProbeTrigger) referenceEvent {
	return func(_ referenceChat, _ time.Time, _ models.ChatType) chats.Event {
		return chats.ProbeTriggered{Trigger: trigger}
	}
}

func referenceIdleLeave(outcome telegram.Class) referenceEvent {
	return func(m referenceChat, _ time.Time, _ models.ChatType) chats.Event {
		return chats.LeaveFinished{
			Source: chats.LeaveIdle, Membership: chats.MembershipSnapshot{Exists: true, Epoch: m.idleLeaveEpoch},
			FactVersion: m.idleLeaveVersion, Result: lifecycleResult(outcome),
		}
	}
}

func referenceMemberUpdate(id int64, old, new chats.Membership) referenceEvent {
	return func(_ referenceChat, now time.Time, chatType models.ChatType) chats.Event {
		return chats.MemberUpdate{
			Order: dispatch.Order{Date: now.Unix(), ID: id}, Type: chatType, Title: "Member event", Old: old, New: new,
		}
	}
}

func referenceCompletion(member chats.Membership) referenceEvent {
	return func(m referenceChat, _ time.Time, chatType models.ChatType) chats.Event {
		ok := sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}}
		return chats.ProbeFinished{
			Request: m.request, Chat: chats.ProbeChat{Result: ok, Type: chatType, Title: "Known chat"},
			Member: chats.ProbeMember{Result: ok, Membership: member}, Admins: chats.ProbeAdmins{Result: ok, CreatorID: 9},
		}
	}
}

func runReferenceSequence(t *testing.T, chatType models.ChatType, seed uint64, prefix []referenceStep) {
	t.Helper()
	const initialSequence uint64 = 45
	policy := chats.Policy{AcceptNewChats: true, RemovedRetention: referenceRemovedRetention, IdleLeaveAfter: referenceIdleAfter}
	model := referenceChat{sequence: initialSequence}
	current := chats.State{Record: chats.Record{ChatID: -723}}
	commitSequence := initialSequence
	now := time.Unix(1_764_000_000, 0).UTC()
	generator := modelEvents{random: rand.New(rand.NewPCG(seed, seed+1))}
	sequence := make([]string, 0, 500)
	var staleUpdates, staleProbeVersions, staleProbeClaims, staleProbeEpochs int

	for index := range cap(sequence) {
		now = now.Add(time.Duration(generator.random.IntN(20)+1) * time.Second)
		var event chats.Event
		if index < len(prefix) {
			now = now.Add(prefix[index].wait)
			event = prefix[index].event(model, now, chatType)
		} else {
			if generator.random.IntN(33) == 0 {
				now = now.Add(25 * time.Hour)
			} else if generator.random.IntN(16) == 0 {
				now = now.Add(10 * time.Minute)
			}
			event = generator.next(model, index, now, chatType)
		}
		sequence = append(sequence, fmt.Sprintf("%d %s %T %+v", index, now.Format(time.RFC3339), event, event))
		fail := func(format string, values ...any) {
			t.Helper()
			t.Fatalf("seed %d, step %d: %s\nevent sequence:\n%s", seed, index, fmt.Sprintf(format, values...), strings.Join(sequence, "\n"))
		}

		if completion, ok := event.(chats.ProbeFinished); ok && hasReferenceMembership(referenceProbeStatus(completion)) {
			if completion.Request.Version != model.version {
				staleProbeVersions++
			}
			if completion.Request.OwnerRevision != model.claimCount {
				staleProbeClaims++
			}
			if completion.Request.MemberEpoch != model.epoch {
				staleProbeEpochs++
			}
		}
		before := model.sequence
		wantEffects, wantStale := model.apply(event, now)
		if _, isMemberUpdate := event.(chats.MemberUpdate); wantStale && isMemberUpdate {
			staleUpdates++
		}
		decision := chats.Step(current, event, now, policy)
		if decision.Stale != wantStale || decision.NewMemberEpoch != (before != model.sequence) {
			fail("stale/new epoch = %v/%v, want %v/%v", decision.Stale, decision.NewMemberEpoch, wantStale, before != model.sequence)
		}
		gotEffects := modelEffectKinds(decision.Effects)
		slices.Sort(wantEffects)
		if !slices.Equal(gotEffects, wantEffects) {
			fail("effect kinds = %v, want %v", gotEffects, wantEffects)
		}

		current = decision.Next
		if decision.NewMemberEpoch {
			commitSequence++
			current.MemberEpoch = commitSequence
		}
		for _, effect := range decision.Effects {
			switch effect := effect.(type) {
			case chats.StartProbe:
				current, _ = chats.BeginProbe(current, effect, now, 0)
			case chats.LeaveChat:
				if effect.Source == chats.LeaveIdle &&
					(effect.Membership.Epoch != model.idleLeaveEpoch || !effect.Membership.Exists || effect.FactVersion != model.idleLeaveVersion) {
					fail("idle leave has wrong epoch or facts: %+v", effect)
				}
			}
		}
		if current.Probe.InFlight != model.request || current.Probe.Actions.Idle != model.idleAfterProbe ||
			current.Probe.IdlePending != model.idleWaiting {
			fail("probe = %+v, want request %+v, idle attached %v, idle pending %v",
				current.Probe, model.request, model.idleAfterProbe, model.idleWaiting)
		}
		watermark := current.Record.Watermark
		if current.Record.State == chats.NoRecord {
			watermark = current.MemoryWatermark
		}
		got := modelSnapshot{
			status: current.Record.State, rights: current.Record.BotRights, hadAdmin: current.Record.EverActive || current.MemoryEverActive,
			watermark: watermark, owner: current.Record.OwnerID, epoch: current.MemberEpoch,
			version: current.Version, claimCount: current.OwnerRevision, chatType: current.Record.Type, title: current.Record.Title,
			guardBot: current.Record.GuardAssigned, guardKnown: current.GuardKnown, joinByRequest: current.JoinByRequest,
			migratedTo: current.Record.MigratedTo, addedAt: current.Record.AddedAt, purgeAfter: current.Record.PurgeAfter,
		}
		if want := model.snapshot(); got != want {
			fail("snapshot = %+v, want %+v", got, want)
		}
	}
	if len(prefix) == 0 && (staleUpdates == 0 || staleProbeVersions == 0 || staleProbeClaims == 0 || staleProbeEpochs == 0) {
		t.Fatalf("seed %d did not cover stale updates, versions, claims and epochs: %d, %d, %d, %d",
			seed, staleUpdates, staleProbeVersions, staleProbeClaims, staleProbeEpochs)
	}
}
