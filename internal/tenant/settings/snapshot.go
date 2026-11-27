package settings

import (
	"maps"
	"sync/atomic"
)

const defaultETag = `"0"`

type rowKey struct {
	chatID  int64
	feature string
}

type entry struct {
	value any
	etag  string
	rev   int64
}

type snapshot map[rowKey]entry

type snapshotShard struct {
	rows atomic.Pointer[snapshot]
}

func (r *Registry) shardIndex(chatID int64) int {
	return int(uint64(chatID) % uint64(len(r.shards)))
}

func (r *Registry) current(spec *registeredSpec, chatID int64) entry {
	return r.readSnapshot(chatID).current(spec, chatID)
}

func (r *Registry) readSnapshot(chatID int64) snapshot {
	shard := &r.shards[r.shardIndex(chatID)]
	if rows := shard.rows.Load(); rows != nil {
		return *rows
	}
	return nil
}

func (rows snapshot) current(spec *registeredSpec, chatID int64) entry {
	if current, exists := rows[rowKey{chatID, spec.Feature}]; exists {
		return current
	}
	return entry{value: spec.defaults, etag: defaultETag}
}

func (r *Registry) publish(key rowKey, next entry) {
	rows := &r.shards[r.shardIndex(key.chatID)].rows
	for {
		previous := rows.Load()
		var current snapshot
		if previous != nil {
			current = *previous
		}
		if existing, exists := current[key]; !exists || next.rev > existing.rev {
			updated := make(snapshot, len(current)+1)
			maps.Copy(updated, current)
			updated[key] = next
			if rows.CompareAndSwap(previous, &updated) {
				return
			}
			continue
		}
		return
	}
}

func (r *Registry) forgetFeatures(chatID int64, features []string) {
	if len(features) == 0 {
		return
	}
	rows := &r.shards[r.shardIndex(chatID)].rows
	for {
		previous := rows.Load()
		if previous == nil {
			return
		}
		updated := maps.Clone(*previous)
		for _, feature := range features {
			delete(updated, rowKey{chatID, feature})
		}
		if rows.CompareAndSwap(previous, &updated) {
			return
		}
	}
}
