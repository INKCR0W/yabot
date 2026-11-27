package settings

import "sync/atomic"

const (
	deletionBuckets    = 256
	deletionActiveBits = 16
	deletionActiveMask = 1<<deletionActiveBits - 1
)

type deletionBucket struct {
	// Read the generation and active count together so a Patch sees the start of an erasure atomically.
	state atomic.Uint64
}

func (r *Registry) deletionBucket(userID int64) *deletionBucket {
	// Collisions can also anonymize overlapping public writes from other users.
	hash := uint64(userID)
	hash ^= hash >> 32
	hash ^= hash >> 16
	hash ^= hash >> 8
	return &r.deletions[hash%deletionBuckets]
}

func (b *deletionBucket) begin() bool {
	for {
		state := b.state.Load()
		if state&deletionActiveMask == deletionActiveMask {
			return false
		}
		if b.state.CompareAndSwap(state, state+(1<<deletionActiveBits)+1) {
			return true
		}
	}
}

func (b *deletionBucket) finish() {
	b.state.Add(^uint64(0))
}

func (b *deletionBucket) permitsIdentity(initial uint64) bool {
	return initial&deletionActiveMask == 0 &&
		initial>>deletionActiveBits == b.state.Load()>>deletionActiveBits
}
