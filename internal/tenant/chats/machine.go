package chats

import (
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

type ChatState string

const (
	NoRecord   ChatState = ""
	Onboarding ChatState = "onboarding"
	Active     ChatState = "active"
	Degraded   ChatState = "degraded"
	Removed    ChatState = "removed"
)

func (s ChatState) isPresent() bool {
	return s == Onboarding || s == Active || s == Degraded
}

type BotRights uint8

const (
	ManageChat BotRights = 1 << iota
	DeleteMessages
	RestrictMembers
	InviteUsers
	PinMessages
	PromoteMembers
	ChangeInfo
)

type RemovedReason string

const (
	RemovedLeft      RemovedReason = "left"
	RemovedKicked    RemovedReason = "kicked"
	RemovedMigrated  RemovedReason = "migrated"
	RemovedForbidden RemovedReason = "forbidden"
	RemovedChatGone  RemovedReason = "chat_gone"
	RemovedByPurge   RemovedReason = "purge"
	RemovedByIdle    RemovedReason = "idle"
)

// Record uses Unix seconds for stored times and zero for absent optional values.
type Record struct {
	ChatID        int64
	Type          models.ChatType
	Title         string
	State         ChatState
	BotRights     BotRights
	GuardAssigned bool
	OwnerID       int64
	AddedBy       int64
	AddedAt       int64
	UpdatedAt     int64
	EverActive    bool
	RemovedAt     int64
	RemovedReason RemovedReason
	MigratedTo    int64
	PurgeAfter    int64
	Watermark     dispatch.Order
}

// State belongs to one chat. Keep Record.ChatID set even when Record.State is NoRecord.
type State struct {
	Record          Record
	MemoryWatermark dispatch.Order
	// MemoryEverActive carries administrator evidence alongside the recordless watermark.
	MemoryEverActive bool
	MemberEpoch      uint64
	// Version tracks changes to chat facts outside Probe triggers and completions.
	// Retain it with a recordless chat's memory watermark.
	Version        uint64
	OwnerRevision  uint64
	Probe          ProbeState
	JoinByRequest  bool
	GuardKnown     bool
	LastOnboarding time.Time
	LastDowngrade  time.Time
}

func (s State) MembershipSnapshot() MembershipSnapshot {
	return MembershipSnapshot{Exists: s.Record.State != NoRecord, Epoch: s.MemberEpoch}
}

type MembershipSnapshot struct {
	Exists bool
	Epoch  uint64
}

type Policy struct {
	AcceptNewChats   bool
	RemovedRetention time.Duration
	IdleLeaveAfter   time.Duration
	Language         string
}

const (
	ProbeInterval   = 10 * time.Minute
	MessageCooldown = 24 * time.Hour
)

type Membership struct {
	Status   models.ChatMemberType
	IsMember bool
	Rights   BotRights
}

type ProbeTrigger string

const (
	ProbeTransition     ProbeTrigger = "transition"
	ProbeUnknown        ProbeTrigger = "report_unknown"
	ProbePermissionLost ProbeTrigger = "report_permission_lost"
	ProbeOwnerStale     ProbeTrigger = "report_owner_stale"
	ProbePeriodic       ProbeTrigger = "periodic"
	ProbeIdle           ProbeTrigger = "idle"
)

type FollowUp struct {
	Onboarding   bool
	LanguageCode string
	Idle         bool
}

type ProbeRequest struct {
	Trigger       ProbeTrigger
	Version       uint64
	OwnerRevision uint64
	MemberEpoch   uint64
	StartedAt     time.Time
}

// An empty InFlight.Trigger means no Probe is running.
type ProbeState struct {
	InFlight       ProbeRequest
	Actions        FollowUp
	Pending        bool
	PendingActions FollowUp
	LastStarted    time.Time
	// IdlePending asks the shard to submit ProbeIdle once LastStarted + ProbeInterval is due.
	// A fresh Probe may take this check earlier if it is exempt from the interval.
	IdlePending bool
}

type ProbeChat struct {
	Result        sender.Result
	Type          models.ChatType
	Title         string
	GuardAssigned bool
	// GuardVisible reports that getChat returned guard_bot at all, which Telegram does only for administrators.
	GuardVisible  bool
	JoinByRequest bool
}

type ProbeMember struct {
	Result     sender.Result
	Membership Membership
}

type ProbeAdmins struct {
	Result    sender.Result
	CreatorID int64
}

type LeaveSource string

const (
	LeaveRejected LeaveSource = "rejected"
	LeavePurge    LeaveSource = "purge"
	LeaveIdle     LeaveSource = "idle"
)

type Event interface{ isEvent() }

type MemberUpdate struct {
	Order        dispatch.Order
	Type         models.ChatType
	Title        string
	Old, New     Membership
	FromID       int64
	LanguageCode string
}

type ProbeTriggered struct{ Trigger ProbeTrigger }

// ProbeFinished carries the request returned by BeginProbe. Leave unattempted stages zero.
type ProbeFinished struct {
	Request ProbeRequest
	Chat    ProbeChat
	Member  ProbeMember
	Admins  ProbeAdmins
}

type MigrationReported struct{ To int64 }

type OwnerSet struct{ UserID int64 }

type LeaveFinished struct {
	Source     LeaveSource
	Membership MembershipSnapshot
	Result     sender.Result
}

type ClearFinished struct {
	RemovedAt   int64
	MemberEpoch uint64
}

// OnboardingFinished is the completion of an ephemeral message, including its fallback context.
type OnboardingFinished struct {
	MemberEpoch  uint64
	ReceiverID   int64
	LanguageCode string
	Result       sender.Result
}

func (MemberUpdate) isEvent()       {}
func (ProbeTriggered) isEvent()     {}
func (ProbeFinished) isEvent()      {}
func (MigrationReported) isEvent()  {}
func (OwnerSet) isEvent()           {}
func (LeaveFinished) isEvent()      {}
func (ClearFinished) isEvent()      {}
func (OnboardingFinished) isEvent() {}

type Effect interface{ isEffect() }

type Persist struct{ Record Record }
type DeleteRecord struct{ ChatID int64 }
type UpdateWatermark struct{ Order dispatch.Order }

// StartProbe reserves no in-flight slot until the runtime calls BeginProbe.
type StartProbe struct {
	Trigger ProbeTrigger
	Actions FollowUp
}

// LanguageCandidates are resolved in group, actor, then English order.
type LanguageCandidates struct {
	Group string
	Actor string
}

// Use OnboardingMessage.GuardAssigned only when GuardKnown is true.
type OnboardingMessage struct {
	Language         LanguageCandidates
	ReceiverID       int64
	MemberEpoch      uint64
	Ephemeral        bool
	GuardAssigned    bool
	GuardKnown       bool
	JoinByRequest    bool
	NeedsInviteUsers bool
}

type DowngradeMessage struct{ Language LanguageCandidates }

type LeaveChat struct {
	Source     LeaveSource
	Membership MembershipSnapshot
	Class      sender.Class
	NoRetry    bool
}

type ReplyOperation string

const (
	ReplySetOwner ReplyOperation = "set_owner"
	ReplyPurge    ReplyOperation = "purge"
)

type ReplyStatus string

const (
	ReplyOK                ReplyStatus = "ok"
	ReplyForbidden         ReplyStatus = "forbidden"
	ReplyMembershipChanged ReplyStatus = "membership_changed"
	ReplyNotLeft           ReplyStatus = "not_left"
)

type Reply struct {
	Operation ReplyOperation
	Status    ReplyStatus
	Result    sender.Result
}

type MetricKind string

const (
	ProbeOutcome      MetricKind = "probe_outcome"
	ProbeInFlight     MetricKind = "probe_in_flight"
	ProbeLimited      MetricKind = "probe_limited"
	ProbeStaleResult  MetricKind = "probe_stale_result"
	InvalidMember     MetricKind = "invalid_member"
	LeaveOutcome      MetricKind = "leave_outcome"
	OnboardingOutcome MetricKind = "onboarding_outcome"
)

type Metric struct {
	Kind    MetricKind
	Trigger ProbeTrigger
	Outcome telegram.Outcome
	Dropped sender.DropReason
	Error   bool
}

func (Persist) isEffect()           {}
func (DeleteRecord) isEffect()      {}
func (UpdateWatermark) isEffect()   {}
func (StartProbe) isEffect()        {}
func (OnboardingMessage) isEffect() {}
func (DowngradeMessage) isEffect()  {}
func (LeaveChat) isEffect()         {}
func (Reply) isEffect()             {}
func (Metric) isEffect()            {}

type Decision struct {
	Next           State
	Effects        []Effect
	NewMemberEpoch bool
	// Stale reports an ignored member update or leave, clear, or onboarding completion.
	// An ignored Probe completion is reported by a ProbeStaleResult metric instead.
	// Any returned effects still apply.
	Stale bool
}

// Step proposes state and effects for one chat. If the first effect is Persist or DeleteRecord, publish Next
// and run remaining effects after commit, assigning a fresh shard sequence to Next.MemberEpoch if NewMemberEpoch is set.
// Discard failed decisions. To permanently discard a ProbeFinished, apply AbandonProbe to the current state.
func Step(state State, event Event, now time.Time, policy Policy) Decision {
	s := step{next: state, now: now, policy: policy}
	switch ev := event.(type) {
	case MemberUpdate:
		s.memberUpdate(ev)
	case ProbeTriggered:
		s.triggerProbe(ev.Trigger, FollowUp{})
	case ProbeFinished:
		s.finishProbe(ev)
	case MigrationReported:
		s.reportMigration(ev)
	case OwnerSet:
		s.setOwner(ev)
	case LeaveFinished:
		s.leaveFinished(ev)
	case ClearFinished:
		s.clearFinished(ev)
	case OnboardingFinished:
		s.finishOnboarding(ev)
	default:
		panic("chats: unsupported event")
	}
	if s.next.Record.State != NoRecord && s.next.Record != state.Record {
		s.effects = append([]Effect{Persist{Record: s.next.Record}}, s.effects...)
	}
	if advancesVersion(event, s.effects, s.newEpoch) {
		s.next.Version++
	}
	return Decision{Next: s.next, Effects: s.effects, NewMemberEpoch: s.newEpoch, Stale: s.stale}
}

func advancesVersion(event Event, effects []Effect, newEpoch bool) bool {
	switch event.(type) {
	case ProbeTriggered, ProbeFinished:
		return false
	}
	if newEpoch {
		return true
	}
	for _, effect := range effects {
		switch effect.(type) {
		case Persist, DeleteRecord, UpdateWatermark:
			return true
		}
	}
	return false
}

type step struct {
	next     State
	now      time.Time
	policy   Policy
	effects  []Effect
	newEpoch bool
	stale    bool
}
