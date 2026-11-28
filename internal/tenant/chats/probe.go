package chats

import (
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

// BeginProbe consumes a StartProbe effect after any associated commit, once the
// shard has installed its new state and assigned any new member epoch.
func BeginProbe(state State, intent StartProbe, now time.Time) (State, ProbeRequest) {
	request := ProbeRequest{
		Trigger:       intent.Trigger,
		Version:       state.Version,
		OwnerRevision: state.OwnerRevision,
		MemberEpoch:   state.MemberEpoch,
		StartedAt:     now,
	}
	state.Probe.InFlight = request
	state.Probe.Actions = intent.Actions
	state.Probe.Pending = false
	state.Probe.PendingActions = FollowUp{}
	state.Probe.LastStarted = now
	state.Probe.IdlePending = false
	return state, request
}

// AbandonProbe discards a matching request and its attached actions, returning StartProbe for any queued transition.
// Install Next and execute Effects in the shard before processing another event.
func AbandonProbe(state State, request ProbeRequest, now time.Time, policy Policy) Decision {
	if request.Trigger == "" || state.Probe.InFlight != request {
		return Decision{Next: state}
	}
	s := step{next: state, now: now, policy: policy}
	s.settleProbe(state.Probe.IdlePending)
	return Decision{Next: s.next, Effects: s.effects}
}

func (s *step) triggerProbe(trigger ProbeTrigger, actions FollowUp) {
	switch trigger {
	case ProbeIdle:
		if !s.isIdle() {
			s.next.Probe.IdlePending = false
			return
		}
		actions.Idle = true
	case ProbePeriodic:
		if !s.next.Record.State.isPresent() {
			return
		}
	case ProbeTransition, ProbeUnknown, ProbePermissionLost, ProbeOwnerStale:
	default:
		return
	}

	probe := &s.next.Probe
	if probe.InFlight.Trigger != "" {
		if trigger == ProbeTransition {
			probe.Pending = true
			if actions.Onboarding {
				probe.PendingActions.Onboarding = true
				probe.PendingActions.LanguageCode = actions.LanguageCode
			}
			return
		}
		if actions.Idle {
			probe.Actions.Idle = true
		}
		s.effects = append(s.effects, Metric{Kind: ProbeInFlight, Trigger: trigger})
		return
	}

	if trigger != ProbeTransition && within(s.now, probe.LastStarted, ProbeInterval) {
		s.effects = append(s.effects, Metric{Kind: ProbeLimited, Trigger: trigger})
		if actions.Idle {
			probe.IdlePending = true
		}
		return
	}
	if probe.IdlePending {
		actions.Idle = s.isIdle()
	}
	s.effects = append(s.effects, StartProbe{Trigger: trigger, Actions: actions})
}

type probeResolution struct {
	result        sender.Result
	state         ChatState
	reason        RemovedReason
	invalidMember bool
}

func resolveProbe(event ProbeFinished) probeResolution {
	if !probeOK(event.Chat.Result) {
		return failedProbe(event.Chat.Result)
	}
	if !probeOK(event.Member.Result) {
		return failedProbe(event.Member.Result)
	}
	state, reason := event.Member.Membership.state()
	if state == Removed || state == NoRecord {
		return probeResolution{
			result:        event.Member.Result,
			state:         state,
			reason:        reason,
			invalidMember: state == NoRecord,
		}
	}
	if !probeOK(event.Admins.Result) {
		return failedProbe(event.Admins.Result)
	}
	return probeResolution{result: event.Admins.Result, state: state}
}

func failedProbe(result sender.Result) probeResolution {
	resolution := probeResolution{result: result}
	if result.Dropped != 0 {
		return resolution
	}
	switch result.Outcome.Class {
	case telegram.Migrated:
		resolution.state, resolution.reason = Removed, RemovedMigrated
	case telegram.Forbidden:
		resolution.state, resolution.reason = Removed, RemovedForbidden
	case telegram.Done:
		if result.Outcome.Reason == telegram.ChatGone {
			resolution.state, resolution.reason = Removed, RemovedChatGone
		}
	}
	return resolution
}

func probeOK(result sender.Result) bool {
	return result.Outcome.Class == telegram.OK && result.Dropped == 0
}

func unexpectedOutcome(result sender.Result) bool {
	if result.Dropped != 0 {
		return false
	}
	switch result.Outcome.Class {
	case telegram.OK, telegram.Retry, telegram.Permission, telegram.Forbidden, telegram.Migrated:
		return false
	case telegram.Done:
		return result.Outcome.Reason != telegram.ChatGone
	default:
		return true
	}
}

func (s *step) finishProbe(event ProbeFinished) {
	probe := &s.next.Probe
	if event.Request.Trigger == "" || probe.InFlight != event.Request {
		s.effects = append(s.effects, Metric{Kind: ProbeStaleResult, Trigger: event.Request.Trigger})
		return
	}

	resolution := resolveProbe(event)
	s.effects = append(s.effects, Metric{
		Kind:    ProbeOutcome,
		Trigger: event.Request.Trigger,
		Outcome: resolution.result.Outcome,
		Dropped: resolution.result.Dropped,
		Error:   unexpectedOutcome(resolution.result),
	})
	if resolution.invalidMember {
		s.effects = append(s.effects, Metric{Kind: InvalidMember, Trigger: event.Request.Trigger, Error: true})
	}

	idle := probe.Actions.Idle && s.isIdle()
	accepted := event.Request.Version == s.next.Version
	if accepted {
		before := s.next.Record.State
		s.applyProbeState(event.Member.Membership, resolution)
		s.next.GuardKnown = false
		if s.next.Record.State != NoRecord && probeOK(event.Chat.Result) {
			s.next.Record.Type = event.Chat.Type
			s.next.Record.Title = event.Chat.Title
			s.next.JoinByRequest = event.Chat.JoinByRequest
			// Telegram exposes guard_bot only to chat administrators.
			if event.Chat.GuardVisible || probeOK(event.Member.Result) && event.Member.Membership.Status == models.ChatMemberTypeAdministrator {
				s.next.Record.GuardAssigned = event.Chat.GuardAssigned
				s.next.GuardKnown = true
			}
		}
		s.notifyDowngrade(before, "")
	} else {
		s.effects = append(s.effects, Metric{Kind: ProbeStaleResult, Trigger: event.Request.Trigger})
	}
	s.applyProbeOwner(event, resolution)
	s.completeProbeActions(event.Request, resolution.state, accepted, idle)
}

func (s *step) applyProbeState(membership Membership, resolution probeResolution) {
	if resolution.state == NoRecord {
		return
	}
	record := &s.next.Record
	switch {
	case resolution.reason == RemovedMigrated:
		s.reportMigration(MigrationReported{To: resolution.result.Outcome.MigrateTo})
	case resolution.state == Removed:
		if record.State.isPresent() {
			s.remove(resolution.reason, 0, false)
		}
	case record.State.isPresent():
		s.setState(resolution.state)
		record.BotRights = membership.Rights
	case s.policy.AcceptNewChats:
		s.enterMembership(resolution.state)
		record.AddedAt = s.now.Unix()
		record.BotRights = membership.Rights
	default:
		s.rejectChat()
	}
}

func (s *step) applyProbeOwner(event ProbeFinished, resolution probeResolution) {
	request := event.Request
	ownerUnchanged := request.OwnerRevision == s.next.OwnerRevision && request.MemberEpoch == s.next.MemberEpoch
	if resolution.state.isPresent() && s.next.Record.State.isPresent() && (s.newEpoch || ownerUnchanged) {
		s.next.Record.OwnerID = event.Admins.CreatorID
	}
}

func (s *step) completeProbeActions(request ProbeRequest, observed ChatState, accepted, idle bool) {
	actions := s.next.Probe.Actions
	s.settleProbe(idle && !accepted)
	record := s.next.Record
	if actions.Onboarding && s.canGuide(request.MemberEpoch, record.AddedBy) {
		s.sendOnboarding(actions.LanguageCode, record.State != Onboarding)
	}
	if idle && accepted && observed.isPresent() && record.State.canLeaveIdle() && !record.EverActive &&
		!s.newEpoch && request.MemberEpoch == s.next.MemberEpoch {
		s.leaveIdleChat(request.MemberEpoch)
	}
}

func (s *step) settleProbe(idlePending bool) {
	completed := s.next.Probe
	s.next.Probe = ProbeState{LastStarted: completed.LastStarted, IdlePending: idlePending}
	if completed.Pending {
		s.triggerProbe(ProbeTransition, completed.PendingActions)
	} else if idlePending && !within(s.now, completed.LastStarted, ProbeInterval) {
		s.triggerProbe(ProbeIdle, FollowUp{})
	}
}

func (state ChatState) canLeaveIdle() bool {
	return state == Onboarding || state == Degraded
}

func (s *step) isIdle() bool {
	record := s.next.Record
	idleSince := max(record.UpdatedAt, record.AddedAt)
	return record.State.canLeaveIdle() && !record.EverActive &&
		s.now.Sub(time.Unix(idleSince, 0)) >= s.policy.IdleLeaveAfter
}

func (s *step) leaveIdleChat(epoch uint64) {
	s.effects = append(s.effects, LeaveChat{
		Source:     LeaveIdle,
		Membership: MembershipSnapshot{Exists: true, Epoch: epoch},
		Class:      sender.Background,
		NoRetry:    true,
	})
}

func (s *step) canGuide(epoch uint64, receiverID int64) bool {
	record := s.next.Record
	return !s.newEpoch && epoch == s.next.MemberEpoch && record.State.isPresent() &&
		record.Type != models.ChatTypeChannel && receiverID != 0 && record.AddedBy == receiverID
}

func (s *step) sendOnboarding(languageCode string, ephemeral bool) {
	message := OnboardingMessage{
		Language:         s.languages(languageCode),
		MemberEpoch:      s.next.MemberEpoch,
		Ephemeral:        ephemeral,
		GuardAssigned:    s.next.Record.GuardAssigned,
		GuardKnown:       s.next.GuardKnown,
		JoinByRequest:    s.next.JoinByRequest,
		NeedsInviteUsers: s.next.Record.State != Active,
	}
	if ephemeral {
		message.ReceiverID = s.next.Record.AddedBy
	} else {
		if within(s.now, s.next.LastOnboarding, MessageCooldown) {
			return
		}
		s.next.LastOnboarding = s.now
	}
	s.effects = append(s.effects, message)
}

func (s *step) finishOnboarding(event OnboardingFinished) {
	s.effects = append(s.effects, Metric{
		Kind:    OnboardingOutcome,
		Outcome: event.Result.Outcome,
		Dropped: event.Result.Dropped,
		Error:   unexpectedOutcome(event.Result),
	})
	if probeOK(event.Result) || event.Result.Dropped == sender.Shutdown {
		return
	}
	if !s.sameMembership(MembershipSnapshot{Exists: true, Epoch: event.MemberEpoch}) {
		s.stale = true
		return
	}
	if s.canGuide(event.MemberEpoch, event.ReceiverID) {
		s.sendOnboarding(event.LanguageCode, false)
	}
}

func (s *step) notifyDowngrade(before ChatState, languageCode string) {
	record := s.next.Record
	if before != Active || (record.State != Degraded && record.State != Onboarding) ||
		record.Type == models.ChatTypeChannel || within(s.now, s.next.LastDowngrade, MessageCooldown) {
		return
	}
	s.next.LastDowngrade = s.now
	s.effects = append(s.effects, DowngradeMessage{Language: s.languages(languageCode)})
}

func (s *step) languages(actor string) LanguageCandidates {
	group := s.policy.Language
	if group == "auto" {
		group = ""
	}
	return LanguageCandidates{Group: group, Actor: actor}
}

func within(now, last time.Time, interval time.Duration) bool {
	return !last.IsZero() && now.Sub(last) < interval
}
