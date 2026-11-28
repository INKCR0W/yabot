package chats

import (
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

// BeginProbe consumes a StartProbe effect after any associated commit, once the
// shard has installed its new state and assigned any new member epoch.
func BeginProbe(state State, intent StartProbe, now time.Time, erasureSequence uint64) (State, ProbeRequest) {
	request := ProbeRequest{
		Trigger:         intent.Trigger,
		Version:         state.Version,
		OwnerRevision:   state.OwnerRevision,
		MemberEpoch:     state.MemberEpoch,
		ErasureSequence: erasureSequence,
		StartedAt:       now,
	}
	state.Probe.InFlight = request
	state.Probe.Actions = intent.Actions
	state.Probe.Pending = false
	state.Probe.PendingActions = FollowUp{}
	state.Probe.LastStarted = now
	state.Probe.IdlePending = false
	return state, request
}

// AbandonProbe settles a matching request using the last committed state for onboarding.
// Install Next and execute Effects in the shard before processing another event.
func AbandonProbe(state State, request ProbeRequest, now time.Time, policy Policy) Decision {
	if request.Trigger == "" || state.Probe.InFlight != request {
		return Decision{Next: state}
	}
	s := step{next: state, now: now, policy: policy}
	s.settleProbe(state.Probe.IdlePending)
	s.guideAfterProbe(request, state.Probe.Actions)
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
		if s.next.Record.State != NoRecord && probeOK(event.Chat.Result) {
			s.next.Record.Type = event.Chat.Type
			s.next.Record.Title = event.Chat.Title
			s.next.JoinByRequest = event.Chat.JoinByRequest
			s.next.GuardKnown = false
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
	if !event.SkipOwner && resolution.state.isPresent() && s.next.Record.State.isPresent() && (s.newEpoch || ownerUnchanged) {
		s.next.Record.OwnerID = event.Admins.CreatorID
	}
}

func (s *step) completeProbeActions(request ProbeRequest, observed ChatState, accepted, idle bool) {
	actions := s.next.Probe.Actions
	s.settleProbe(idle && !accepted)
	record := s.next.Record
	s.guideAfterProbe(request, actions)
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
	return record.State.isIdle(record.EverActive, max(record.UpdatedAt, record.AddedAt), s.now, s.policy.IdleLeaveAfter)
}

func (state ChatState) isIdle(everActive bool, idleSince int64, now time.Time, idleAfter time.Duration) bool {
	return state.canLeaveIdle() && !everActive && now.Sub(time.Unix(idleSince, 0)) >= idleAfter
}

func (s *step) leaveIdleChat(epoch uint64) {
	s.effects = append(s.effects, LeaveChat{
		Source:      LeaveIdle,
		Membership:  MembershipSnapshot{Exists: true, Epoch: epoch},
		FactVersion: s.next.Version,
		Class:       sender.Background,
		NoRetry:     true,
	})
}

func (s *step) guideAfterProbe(request ProbeRequest, actions FollowUp) {
	record := s.next.Record
	if !record.State.isPresent() || record.Type == models.ChatTypeChannel {
		return
	}
	if !s.newEpoch && (!actions.Onboarding || request.MemberEpoch != s.next.MemberEpoch) {
		return
	}
	languageCode := actions.LanguageCode
	if s.newEpoch {
		languageCode = ""
	}
	s.effects = append(s.effects, s.next.onboardingMessage(s.languages(languageCode)))
}

func (s State) onboardingMessage(language LanguageCandidates) OnboardingMessage {
	return OnboardingMessage{
		Language:         language,
		GuardAssigned:    s.Record.GuardAssigned,
		GuardKnown:       s.GuardKnown,
		JoinByRequest:    s.JoinByRequest,
		NeedsInviteUsers: s.Record.State != Active,
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
