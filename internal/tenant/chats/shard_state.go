package chats

import (
	"container/list"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
)

const (
	recordlessCapacity = 100_000
	memoryTTL          = 10 * time.Minute
)

type chatEntity struct {
	state  State
	replay *dispatch.Item
}

type memoryEntry struct {
	entity    *chatEntity
	touchedAt time.Time
}

type shardState struct {
	records        map[int64]*chatEntity
	pending        map[int64]*pendingWrite
	memory         map[int64]*list.Element
	clearedProbes  map[int64]*chatEntity
	memoryCapacity int
	lru            list.List
	idleDue        map[int64]time.Time
	onboarding     onboardingCooldowns
	epoch          uint64
}

func newShardState(memoryCapacity int) *shardState {
	return &shardState{
		records:        make(map[int64]*chatEntity),
		pending:        make(map[int64]*pendingWrite),
		memory:         make(map[int64]*list.Element),
		clearedProbes:  make(map[int64]*chatEntity),
		memoryCapacity: memoryCapacity,
		idleDue:        make(map[int64]time.Time),
	}
}

func (c *Chats) local(sh *dispatch.Shard) *shardState {
	if local, ok := c.shards.Load(sh.Index()); ok {
		return local.(*shardState)
	}
	local := newShardState(recordlessCapacity)
	c.shards.Store(sh.Index(), local)
	return local
}

func (c *Chats) entity(sh *dispatch.Shard, shard *shardState, chatID int64, now time.Time) *chatEntity {
	if entity := shard.records[chatID]; entity != nil {
		return entity
	}
	if entity := shard.clearedProbes[chatID]; entity != nil {
		return entity
	}
	if entry := shard.memory[chatID]; entry != nil {
		memory := entry.Value.(*memoryEntry)
		if now.Sub(memory.touchedAt) < memoryTTL || memory.pinned(sh) {
			memory.touchedAt = now
			shard.lru.MoveToFront(entry)
			return memory.entity
		}
		c.evictExpiredMemory(shard, entry)
	}
	return &chatEntity{state: State{Record: Record{ChatID: chatID}}}
}

func (c *Chats) install(sh *dispatch.Shard, shard *shardState, entity *chatEntity, next State, now time.Time) bool {
	id := next.Record.ChatID
	before := entity.state
	entity.state = next
	if next.Record.State == NoRecord {
		delete(shard.records, id)
		deleted := before.Record.State != NoRecord
		if deleted {
			c.publish(entity)
		}
		if !c.installRecordless(sh, shard, entity, now, deleted) {
			return false
		}
	} else {
		delete(shard.clearedProbes, id)
		if entry := shard.memory[id]; entry != nil {
			shard.removeMemory(entry)
		}
		shard.records[id] = entity
		c.publish(entity)
	}
	if next.Probe.IdlePending && next.Probe.InFlight.Trigger == "" {
		shard.idleDue[id] = next.Probe.LastStarted.Add(ProbeInterval)
	} else {
		delete(shard.idleDue, id)
	}
	return true
}

func (c *Chats) installRecordless(sh *dispatch.Shard, shard *shardState, entity *chatEntity, now time.Time, deleted bool) bool {
	id := entity.state.Record.ChatID
	_, clearedProbe := shard.clearedProbes[id]
	inFlight := entity.state.Probe.InFlight.Trigger != ""
	if clearedProbe && inFlight {
		return true
	}
	delete(shard.clearedProbes, id)
	if entity.state == (State{Record: Record{ChatID: id}}) || c.retainMemory(sh, shard, entity, now) {
		return true
	}
	if deleted && inFlight {
		// A committed delete cannot discard the request awaiting its completion.
		shard.clearedProbes[id] = entity
		return true
	}
	if deleted || clearedProbe {
		c.count(`yabot_chats_memory_evictions_total{reason="capacity"}`)
		return true
	}
	return false
}

func (c *Chats) ensureRecordlessMemory(sh *dispatch.Shard, entity *chatEntity) bool {
	if entity.state.Record.State != NoRecord {
		return true
	}
	shard := c.local(sh)
	id := entity.state.Record.ChatID
	return shard.memory[id] != nil || shard.clearedProbes[id] != nil || c.retainMemory(sh, shard, entity, c.now())
}

func (c *Chats) retainMemory(sh *dispatch.Shard, shard *shardState, entity *chatEntity, now time.Time) bool {
	id := entity.state.Record.ChatID
	if entry := shard.memory[id]; entry != nil {
		entry.Value = &memoryEntry{entity: entity, touchedAt: now}
		shard.lru.MoveToFront(entry)
		return true
	}
	c.expireMemory(sh, shard, now)
	if len(shard.memory) >= shard.memoryCapacity {
		oldest := shard.lru.Back()
		for oldest != nil && oldest.Value.(*memoryEntry).pinned(sh) {
			oldest = oldest.Prev()
		}
		if oldest == nil {
			c.count(`yabot_chats_overload_total{source="recordless_memory"}`)
			c.log.Warn("recordless chat memory is full of pinned entries", "chat_id", id)
			return false
		}
		c.log.Warn("recordless chat memory is full", "chat_id", oldest.Value.(*memoryEntry).entity.state.Record.ChatID)
		shard.removeMemory(oldest)
		c.count(`yabot_chats_memory_evictions_total{reason="capacity"}`)
	}
	shard.memory[id] = shard.lru.PushFront(&memoryEntry{entity: entity, touchedAt: now})
	return true
}

func (c *Chats) expireMemory(sh *dispatch.Shard, shard *shardState, now time.Time) {
	for entry := shard.lru.Back(); entry != nil; {
		memory := entry.Value.(*memoryEntry)
		if now.Sub(memory.touchedAt) < memoryTTL {
			return
		}
		previous := entry.Prev()
		if !memory.pinned(sh) {
			c.evictExpiredMemory(shard, entry)
		}
		entry = previous
	}
}

func (c *Chats) evictExpiredMemory(shard *shardState, entry *list.Element) {
	shard.removeMemory(entry)
	c.count(`yabot_chats_memory_evictions_total{reason="ttl"}`)
}

func (m *memoryEntry) pinned(sh *dispatch.Shard) bool {
	entity := m.entity
	return entity.replay != nil || entity.state.Probe.InFlight.Trigger != "" || sh.Busy(keyOf(entity.state.Record.ChatID))
}

func (s *shardState) removeMemory(entry *list.Element) {
	delete(s.memory, entry.Value.(*memoryEntry).entity.state.Record.ChatID)
	s.lru.Remove(entry)
}
