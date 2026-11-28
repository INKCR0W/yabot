package chats

import (
	"cmp"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
)

const (
	changeCapacity = 4096
	viewBuckets    = 256
)

type View interface {
	State(chatID int64) (ChatSnapshot, bool)
	Version() uint64
	ChangesSince(version uint64) (changes []Change, now uint64, full bool)
}

// ChatSnapshot uses Unix seconds for times and zero for absent optional values.
type ChatSnapshot struct {
	ChatID        int64
	Type          models.ChatType
	Title         string
	State         ChatState
	BotRights     BotRights
	GuardAssigned bool
	GuardKnown    bool
	JoinByRequest bool
	OwnerID       int64
	AddedAt       int64
	UpdatedAt     int64
	EverActive    bool
	RemovedAt     int64
	RemovedReason RemovedReason
	MigratedTo    int64
	PurgeAfter    int64
	MemberEpoch   uint64
	Watermark     dispatch.Order
}

// Change reports NoRecord when the chat row was deleted.
type Change struct {
	Version uint64
	ChatID  int64
	State   ChatState
}

type ChatRef struct {
	ID            int64
	Title         string
	Type          models.ChatType
	State         ChatState
	GuardAssigned bool
}

type viewEntry struct {
	snapshot               ChatSnapshot
	lastProbe              time.Time
	factVersion            uint64
	committedOwnerErasures *atomic.Uint64
	observedOwnerErasures  uint64
}

// The SQLite writer can commit an erasure even when the published owner is already empty.
func (e viewEntry) currentFactVersion() uint64 {
	return e.factVersion + (e.committedOwnerErasures.Load() - e.observedOwnerErasures)
}

type viewRows map[int64]viewEntry

type view struct {
	buckets [viewBuckets]atomic.Pointer[viewRows]
	version atomic.Uint64

	mu           sync.Mutex
	changes      [changeCapacity]Change
	stateCounts  map[ChatState]*metrics.Gauge
	ownerMissing *metrics.Gauge
}

func (v *view) init(set *metrics.Set) {
	if set == nil {
		return
	}
	v.stateCounts = make(map[ChatState]*metrics.Gauge, 4)
	for _, state := range []ChatState{Onboarding, Active, Degraded, Removed} {
		v.stateCounts[state] = set.GetOrCreateGauge(`yabot_chats{state="`+string(state)+`"}`, nil)
	}
	v.ownerMissing = set.GetOrCreateGauge("yabot_chats_owner_missing", nil)
}

func (v *view) State(chatID int64) (ChatSnapshot, bool) {
	entry, ok := v.lookup(chatID)
	return entry.snapshot, ok
}

func (v *view) lookup(chatID int64) (viewEntry, bool) {
	if rows := v.bucket(chatID).Load(); rows != nil {
		entry, ok := (*rows)[chatID]
		return entry, ok
	}
	return viewEntry{}, false
}

func (v *view) Version() uint64 { return v.version.Load() }

func (v *view) ChangesSince(version uint64) (changes []Change, now uint64, full bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	now = v.version.Load()
	if version > now || now-version > changeCapacity {
		return nil, now, true
	}
	if version == now {
		return nil, now, false
	}
	changes = make([]Change, now-version)
	for i := range changes {
		changes[i] = v.changes[(version+uint64(i))%changeCapacity]
	}
	return changes, now, false
}

// OwnedBy scans the snapshot and sorts matching chats by ID in ascending order.
func (v *view) OwnedBy(userID int64) []ChatRef {
	refs := make([]ChatRef, 0)
	if userID == 0 {
		return refs
	}
	for i := range v.buckets {
		if rows := v.buckets[i].Load(); rows != nil {
			for _, entry := range *rows {
				s := entry.snapshot
				if s.OwnerID == userID && s.State.isPresent() {
					refs = append(refs, ChatRef{ID: s.ChatID, Title: s.Title, Type: s.Type, State: s.State, GuardAssigned: s.GuardAssigned})
				}
			}
		}
	}
	slices.SortFunc(refs, func(a, b ChatRef) int { return cmp.Compare(a.ID, b.ID) })
	return refs
}

func (v *view) bucket(chatID int64) *atomic.Pointer[viewRows] {
	return &v.buckets[uint64(chatID)%viewBuckets]
}

func (v *view) publish(entity *chatEntity) {
	s := entity.state
	record := s.Record
	var next viewEntry
	if record.State != NoRecord {
		next = viewEntry{
			snapshot: ChatSnapshot{
				ChatID: record.ChatID, Type: record.Type, Title: record.Title, State: record.State,
				BotRights: record.BotRights, GuardAssigned: record.GuardAssigned, GuardKnown: s.GuardKnown, JoinByRequest: s.JoinByRequest,
				OwnerID: record.OwnerID, AddedAt: record.AddedAt, UpdatedAt: record.UpdatedAt,
				EverActive: record.EverActive, RemovedAt: record.RemovedAt, RemovedReason: record.RemovedReason,
				MigratedTo: record.MigratedTo, PurgeAfter: record.PurgeAfter, MemberEpoch: s.MemberEpoch, Watermark: record.Watermark,
			},
			lastProbe:              s.Probe.LastStarted,
			factVersion:            s.Version,
			committedOwnerErasures: &entity.committedOwnerErasures,
			observedOwnerErasures:  entity.observedOwnerErasures,
		}
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	bucket := v.bucket(record.ChatID)
	var oldRows viewRows
	if rows := bucket.Load(); rows != nil {
		oldRows = *rows
	}
	prev := oldRows[record.ChatID]
	if prev == next {
		return
	}
	newRows := make(viewRows, len(oldRows)+1)
	maps.Copy(newRows, oldRows)
	if record.State == NoRecord {
		delete(newRows, record.ChatID)
	} else {
		newRows[record.ChatID] = next
	}
	bucket.Store(&newRows)
	if prev.snapshot.State != record.State {
		version := v.version.Load()
		v.changes[version%changeCapacity] = Change{Version: version + 1, ChatID: record.ChatID, State: record.State}
		v.version.Store(version + 1)
	}
	v.updateGauges(prev.snapshot, next.snapshot)
}

func (v *view) updateGauges(prev, next ChatSnapshot) {
	if v.stateCounts == nil {
		return
	}
	if prev.State != next.State {
		if gauge := v.stateCounts[prev.State]; gauge != nil {
			gauge.Dec()
		}
		if gauge := v.stateCounts[next.State]; gauge != nil {
			gauge.Inc()
		}
	}
	before := prev.State.isPresent() && prev.OwnerID == 0
	after := next.State.isPresent() && next.OwnerID == 0
	switch {
	case before && !after:
		v.ownerMissing.Dec()
	case !before && after:
		v.ownerMissing.Inc()
	}
}

func (v *view) nextPeriodic(after int64, now time.Time, interval time.Duration) (int64, bool) {
	var first, later int64
	var haveFirst, haveLater bool
	for i := range v.buckets {
		rows := v.buckets[i].Load()
		if rows == nil {
			continue
		}
		for id, entry := range *rows {
			if !entry.snapshot.State.isPresent() || within(now, entry.lastProbe, interval) {
				continue
			}
			if !haveFirst || id < first {
				first, haveFirst = id, true
			}
			if id > after && (!haveLater || id < later) {
				later, haveLater = id, true
			}
		}
	}
	if haveLater {
		return later, true
	}
	return first, haveFirst
}
