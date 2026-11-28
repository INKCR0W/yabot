package chats

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/store"
)

const (
	erasureBuckets = 4096
	eraseBatchRows = 500
)

type erasureStamp struct {
	sequence uint64
	active   int
}

type erasureGate struct {
	mu       sync.Mutex
	sequence atomic.Uint64
	buckets  [erasureBuckets]atomic.Pointer[erasureStamp]
}

func (g *erasureGate) bucket(userID int64) *atomic.Pointer[erasureStamp] {
	return &g.buckets[uint64(userID)%erasureBuckets]
}

func (g *erasureGate) changedSince(userID int64, sequence uint64) bool {
	stamp := g.bucket(userID).Load()
	return stamp != nil && (stamp.active != 0 || stamp.sequence > sequence)
}

func (g *erasureGate) change(userID int64, delta int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	bucket := g.bucket(userID)
	stamp := erasureStamp{sequence: g.sequence.Load() + 1, active: delta}
	if previous := bucket.Load(); previous != nil {
		stamp.active += previous.active
	}
	// Readers sampling the new global sequence must see the corresponding bucket change.
	bucket.Store(&stamp)
	g.sequence.Store(stamp.sequence)
}

// ClaimToken binds a Telegram owner check to the chat membership and erasure sequence at its start.
type ClaimToken struct {
	userID      int64
	chatID      int64
	sequence    uint64
	memberEpoch uint64
}

// ClaimToken must be taken before verifying the user's creator status with Telegram.
func (c *Chats) ClaimToken(userID, chatID int64) (ClaimToken, error) {
	sequence := c.erasure.sequence.Load()
	snapshot, _ := c.State(chatID)
	if userID <= 0 {
		return ClaimToken{}, c.rejectClaim(chatID, userID, "user_mismatch")
	}
	if c.erasure.changedSince(userID, sequence) {
		return ClaimToken{}, c.rejectClaim(chatID, userID, "erasure")
	}
	return ClaimToken{userID: userID, chatID: chatID, sequence: sequence, memberEpoch: snapshot.MemberEpoch}, nil
}

func (c *Chats) invalidClaimReason(token ClaimToken, chatID, userID int64, memberEpoch uint64) string {
	switch {
	case userID <= 0 || token.userID != userID:
		return "user_mismatch"
	case token.chatID != chatID:
		return "chat_mismatch"
	case token.memberEpoch != memberEpoch:
		return "membership_changed"
	case c.erasure.changedSince(userID, token.sequence):
		return "erasure"
	default:
		return ""
	}
}

type erasureLease struct {
	refs   atomic.Int64
	gate   *erasureGate
	userID int64
}

func (g *erasureGate) begin(userID int64) *erasureLease {
	g.change(userID, 1)
	lease := &erasureLease{gate: g, userID: userID}
	lease.refs.Store(1)
	return lease
}

func (e *erasureLease) retain() bool {
	for {
		refs := e.refs.Load()
		if refs == 0 {
			return false
		}
		if e.refs.CompareAndSwap(refs, refs+1) {
			return true
		}
	}
}

func (e *erasureLease) release() {
	if e.refs.Add(-1) == 0 {
		e.gate.change(e.userID, -1)
	}
}

func (c *Chats) DeleteUser(ctx context.Context, userID int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if userID == 0 {
		return nil
	}
	lease := c.erasure.begin(userID)
	defer lease.release()
	err := c.dispatch.RunOnAll(ctx, func(_ context.Context, sh *dispatch.Shard, done func(error)) {
		if err := ctx.Err(); err != nil {
			done(err)
			return
		}
		if !lease.retain() {
			done(context.Canceled)
			return
		}
		defer lease.release()
		c.eraseShardOwner(c.local(sh), userID)
		done(nil)
	})
	if err == nil {
		err = c.eraseStoredOwner(ctx, lease)
	}
	if err == nil {
		return nil
	}
	if ctx.Err() == nil {
		if errors.Is(err, store.ErrBusy) || errors.Is(err, dispatch.ErrBusy) || errors.Is(err, ErrBusy) {
			c.log.Info("chat user erasure is busy")
			c.count(`yabot_chats_erase_failures_total{reason="busy"}`)
		} else {
			c.log.Error("chat user erasure failed", "operation", "erase_owner", "sqlite_code", sqliteErrorCode(err))
			c.count(`yabot_chats_erase_failures_total{reason="error"}`)
		}
	}
	return fmt.Errorf("erase chat user data: %w", err)
}

func (c *Chats) eraseShardOwner(local *shardState, userID int64) {
	// Preserve the erasure's version change when a pending decision replaces the current state.
	for _, pending := range local.pending {
		next := &pending.decision.Next
		erased := pending.entity.state.Record.OwnerID == userID
		if next.Record.OwnerID == userID {
			next.Record.OwnerID = 0
			erased = true
		}
		if erased {
			next.Version++
		}
	}
	// For a pending write, the recorded entity is also its rollback state.
	for _, entity := range local.records {
		if entity.state.Record.OwnerID == userID {
			entity.state.Record.OwnerID = 0
			entity.state.Version++
			c.publish(entity)
		}
	}
}

func (c *Chats) eraseStoredOwner(ctx context.Context, lease *erasureLease) (err error) {
	result := make(chan error, 1)
	// A canceled caller can return before the accepted batch settles.
	lease.refs.Add(1)
	accepted := false
	defer func() {
		if !accepted {
			lease.release()
			if recover() != nil {
				err = errors.New("chat owner erasure submission panicked")
			}
		}
	}()
	// The shard barrier puts every queued upsert carrying the old owner ahead of this sweep.
	if err := c.writeOwnerErasure(ctx, lease.userID, func(err error) {
		lease.release()
		result <- err
	}); err != nil {
		return err
	}
	accepted = true
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-result:
		return err
	}
}

func (c *Chats) writeOwnerErasure(ctx context.Context, userID int64, done func(error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var affected int64
	return c.store.Write(func(tx *sql.Tx) error {
		result, err := tx.Exec(`UPDATE chats SET owner_id = NULL
			WHERE chat_id IN (SELECT chat_id FROM chats WHERE owner_id = ? LIMIT ?)`, userID, eraseBatchRows)
		if err != nil {
			return err
		}
		affected, err = result.RowsAffected()
		return err
	}, func(err error) {
		returned := false
		defer func() {
			if !returned {
				done(errors.New("chat owner erasure callback panicked"))
			}
		}()
		if err == nil && affected > 0 {
			err = c.writeOwnerErasure(ctx, userID, done)
			if err == nil {
				returned = true
				return
			}
		}
		done(err)
		returned = true
	})
}
