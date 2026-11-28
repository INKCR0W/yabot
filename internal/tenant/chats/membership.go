package chats

import (
	"errors"

	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

func MembershipOf(member models.ChatMember) Membership {
	membership := Membership{Status: member.Type}
	switch member.Type {
	case models.ChatMemberTypeAdministrator:
		admin := member.Administrator
		if admin == nil {
			return Membership{}
		}
		membership.Rights = sharedRights(admin.CanInviteUsers, admin.CanPinMessages, admin.CanChangeInfo)
		if admin.CanManageChat {
			membership.Rights |= ManageChat
		}
		if admin.CanDeleteMessages {
			membership.Rights |= DeleteMessages
		}
		if admin.CanRestrictMembers {
			membership.Rights |= RestrictMembers
		}
		if admin.CanPromoteMembers {
			membership.Rights |= PromoteMembers
		}
	case models.ChatMemberTypeRestricted:
		restricted := member.Restricted
		if restricted == nil {
			return Membership{}
		}
		membership.IsMember = restricted.IsMember
		if membership.IsMember {
			membership.Rights = sharedRights(restricted.CanInviteUsers, restricted.CanPinMessages, restricted.CanChangeInfo)
		}
	}
	return membership
}

func sharedRights(invite, pin, changeInfo bool) BotRights {
	var rights BotRights
	if invite {
		rights |= InviteUsers
	}
	if pin {
		rights |= PinMessages
	}
	if changeInfo {
		rights |= ChangeInfo
	}
	return rights
}

// MemberUpdateOf accepts only standalone my_chat_member updates for groups, supergroups, and channels.
func MemberUpdateOf(update *models.Update) (MemberUpdate, bool) {
	if update == nil || update.MyChatMember == nil {
		return MemberUpdate{}, false
	}
	member := update.MyChatMember
	switch member.Chat.Type {
	case models.ChatTypeGroup, models.ChatTypeSupergroup, models.ChatTypeChannel:
	default:
		return MemberUpdate{}, false
	}
	order, ok := dispatch.OrderOf(update)
	if !ok {
		return MemberUpdate{}, false
	}
	return MemberUpdate{
		Order: order, Type: member.Chat.Type, Title: member.Chat.Title,
		Old: MembershipOf(member.OldChatMember), New: MembershipOf(member.NewChatMember),
		LanguageCode: member.From.LanguageCode,
	}, true
}

func (m Membership) state() (ChatState, RemovedReason) {
	switch m.Status {
	case models.ChatMemberTypeAdministrator:
		if m.Rights&InviteUsers != 0 {
			return Active, ""
		}
		return Degraded, ""
	case models.ChatMemberTypeRestricted:
		if m.IsMember {
			return Onboarding, ""
		}
		return Removed, RemovedLeft
	case models.ChatMemberTypeMember:
		return Onboarding, ""
	case models.ChatMemberTypeLeft:
		return Removed, RemovedLeft
	case models.ChatMemberTypeBanned:
		return Removed, RemovedKicked
	default:
		return NoRecord, ""
	}
}

func (s *step) memberUpdate(event MemberUpdate) {
	after, reason := event.New.state()
	if after == NoRecord {
		s.effects = append(s.effects, Metric{Kind: InvalidMember, Error: true})
		return
	}
	record := &s.next.Record
	watermark := record.Watermark
	if record.State == NoRecord {
		watermark = s.next.MemoryWatermark
	}
	if !event.Order.After(watermark) {
		s.stale = true
		return
	}

	s.applyMembership(event, after, reason)
	if record.State == NoRecord {
		s.next.MemoryWatermark = event.Order
		s.effects = append([]Effect{UpdateWatermark{Order: event.Order}}, s.effects...)
		return
	}
	record.Watermark = event.Order
}

func (s *step) applyMembership(event MemberUpdate, after ChatState, reason RemovedReason) {
	record := &s.next.Record
	before := record.State
	if event.Old.Status == models.ChatMemberTypeAdministrator {
		if before == NoRecord {
			s.next.MemoryEverActive = true
		} else {
			record.EverActive = true
		}
	}

	if after == Removed && before == NoRecord {
		return
	}
	if after != Removed && !before.isPresent() && !s.policy.AcceptNewChats {
		s.rejectChat()
		return
	}

	record.Type = event.Type
	record.Title = event.Title
	if after == Removed {
		if before.isPresent() {
			s.remove(reason, 0, false)
		}
		return
	}

	record.BotRights = event.New.Rights
	oldState, _ := event.Old.state()
	if !before.isPresent() || oldState == Removed {
		s.enterMembership(after)
		if oldState == Removed {
			record.AddedAt = s.now.Unix()
		}
		s.triggerProbe(ProbeTransition, FollowUp{Onboarding: true, LanguageCode: event.LanguageCode})
	} else if before != after {
		s.setState(after)
		s.triggerProbe(ProbeTransition, FollowUp{})
	}
	s.notifyDowngrade(oldState, event.LanguageCode)
}

func (s *step) setState(to ChatState) {
	record := &s.next.Record
	if record.State != to {
		if record.State == NoRecord {
			record.Watermark = s.next.MemoryWatermark
			record.EverActive = s.next.MemoryEverActive
			s.next.MemoryWatermark = dispatch.Order{}
			s.next.MemoryEverActive = false
		}
		record.State = to
		record.UpdatedAt = s.now.Unix()
	}
	if to == Active {
		record.EverActive = true
	}
}

func (s *step) enterMembership(to ChatState) {
	s.setState(to)
	record := &s.next.Record
	record.OwnerID = 0
	record.GuardAssigned = false
	record.RemovedAt = 0
	record.RemovedReason = ""
	record.MigratedTo = 0
	record.PurgeAfter = 0
	s.next.GuardKnown = false
	s.next.JoinByRequest = false
	s.newEpoch = true
}

func (s *step) remove(reason RemovedReason, migratedTo int64, purgeNow bool) {
	s.setState(Removed)
	record := &s.next.Record
	record.BotRights = 0
	record.RemovedAt = s.now.Unix()
	record.RemovedReason = reason
	record.MigratedTo = migratedTo
	record.PurgeAfter = s.now.Add(s.policy.RemovedRetention).Unix()
	if purgeNow {
		record.PurgeAfter = record.RemovedAt
	}
}

func (s *step) rejectChat() {
	s.effects = append(s.effects, LeaveChat{Source: LeaveRejected, Class: sender.Background})
}

func (s *step) reportMigration(event MigrationReported) {
	record := &s.next.Record
	if record.State == Removed {
		record.RemovedReason = RemovedMigrated
		record.MigratedTo = event.To
		return
	}
	s.remove(RemovedMigrated, event.To, false)
}

func (s *step) setOwner(event OwnerSet) {
	if !s.next.Record.State.isPresent() {
		s.effects = append(s.effects, Reply{Operation: ReplySetOwner, Status: ReplyForbidden})
		return
	}
	s.next.Record.OwnerID = event.UserID
	s.next.OwnerRevision++
	s.effects = append(s.effects, Reply{Operation: ReplySetOwner, Status: ReplyOK})
}

func (s *step) leaveFinished(event LeaveFinished) {
	if event.Source == LeavePurge && errors.Is(event.Result.Err, errPurgeLeaveCanceled) {
		s.finishPurge(event, false)
		return
	}
	if event.Source == LeaveIdle {
		if !s.sameMembership(event.Membership) {
			s.stale = true
			return
		}
		if s.next.Record.State == Removed && !s.next.Record.EverActive {
			s.next.Record.PurgeAfter = s.now.Unix()
			return
		}
		if event.FactVersion != s.next.Version || !s.next.Record.State.canLeaveIdle() || s.next.Record.EverActive {
			s.stale = true
			return
		}
	}
	left := hasLeft(event.Result)
	if !left {
		s.effects = append(s.effects, Metric{
			Kind: LeaveOutcome, Outcome: event.Result.Outcome, Dropped: event.Result.Dropped,
			Error: event.Result.Outcome.Class != telegram.Retry,
		})
	}
	switch event.Source {
	case LeavePurge:
		s.finishPurge(event, left)
	case LeaveIdle:
		if left {
			s.expediteRemoval(event.Result.Outcome, RemovedByIdle)
		}
	}
}

func hasLeft(result sender.Result) bool {
	if result.Dropped != 0 {
		return false
	}
	switch result.Outcome.Class {
	case telegram.OK, telegram.Done, telegram.Forbidden, telegram.Migrated:
		return true
	default:
		return false
	}
}

func (s *step) sameMembership(snapshot MembershipSnapshot) bool {
	return snapshot.Exists && snapshot == s.next.MembershipSnapshot()
}

func (s *step) finishPurge(event LeaveFinished, left bool) {
	reply := Reply{Operation: ReplyPurge, Status: ReplyOK}
	switch {
	case s.next.Record.State == NoRecord:
	case !s.sameMembership(event.Membership):
		s.stale = true
		reply.Status = ReplyMembershipChanged
	case s.next.Record.State == Removed || left:
		s.expediteRemoval(event.Result.Outcome, RemovedByPurge)
	default:
		reply.Status = ReplyNotLeft
		reply.Result = event.Result
	}
	s.effects = append(s.effects, reply)
}

func (s *step) expediteRemoval(outcome telegram.Outcome, reason RemovedReason) {
	if s.next.Record.State == Removed {
		s.next.Record.PurgeAfter = s.now.Unix()
		return
	}
	if outcome.Class == telegram.Migrated {
		s.remove(RemovedMigrated, outcome.MigrateTo, true)
		return
	}
	s.remove(reason, 0, true)
}

func (s *step) clearFinished(event ClearFinished) {
	record := s.next.Record
	if record.State != Removed || record.RemovedAt != event.RemovedAt || s.next.MemberEpoch != event.MemberEpoch {
		s.stale = true
		return
	}
	probe := s.next.Probe
	s.next = State{
		Record: Record{ChatID: record.ChatID}, Version: s.next.Version,
		Probe: ProbeState{InFlight: probe.InFlight, LastStarted: probe.LastStarted},
	}
	s.effects = append(s.effects, DeleteRecord{ChatID: record.ChatID})
}
