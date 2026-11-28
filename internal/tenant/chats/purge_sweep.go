package chats

import (
	"context"
	"errors"
	"fmt"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/janitor"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/INKCR0W/yabot/internal/tenant/contract"
)

const purgeScanRows = 16

type purgeCandidate struct {
	id         int64
	purgeAfter int64
}

func purgeScanQuery(cutoff int64, after *purgeCandidate) (string, []any) {
	query := `SELECT chat_id, purge_after FROM chats WHERE state = ? AND purge_after <= ?`
	args := []any{Removed, cutoff}
	if after != nil {
		query += ` AND (purge_after, chat_id) > (?, ?)`
		args = append(args, after.purgeAfter, after.id)
	}
	return query + ` ORDER BY purge_after, chat_id LIMIT ?`, append(args, purgeScanRows)
}

func (c *Chats) purgeCandidates(ctx context.Context, cutoff int64) (candidates []purgeCandidate, err error) {
	query, args := purgeScanQuery(cutoff, c.purgeCursor)
	rows, err := c.store.Read().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		var row purgeCandidate
		if err := rows.Scan(&row.id, &row.purgeAfter); err != nil {
			return nil, err
		}
		candidates = append(candidates, row)
	}
	return candidates, rows.Err()
}

func (c *Chats) purgeSweep(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	cutoff := c.now().Unix()
	candidates, err := c.purgeCandidates(ctx, cutoff)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, fmt.Errorf("chats: purge scan failed (sqlite_code=%d)", sqliteErrorCode(err))
	}
	deleted := 0
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		cleared, err := c.purgeRemovedChat(ctx, candidate.id, cutoff)
		if cleared {
			deleted++
		}
		if err != nil {
			return deleted, err
		}
		// Move past failures as well so a full page cannot starve later chats.
		c.purgeCursor = &candidate
	}
	if len(candidates) == purgeScanRows {
		return deleted, janitor.ErrBacklog
	}
	c.purgeCursor = nil
	return deleted, nil
}

func (c *Chats) purgeRemovedChat(ctx context.Context, chatID, cutoff int64) (bool, error) {
	snapshot, ok := c.State(chatID)
	if !ok || snapshot.State != Removed || snapshot.PurgeAfter > cutoff {
		return false, nil
	}
	event := ClearFinished{RemovedAt: snapshot.RemovedAt, MemberEpoch: snapshot.MemberEpoch}
	valid := func() bool {
		current, _ := c.State(chatID)
		return event.matchesRemoval(current.State, current.RemovedAt, current.MemberEpoch)
	}
	for _, purger := range c.purgers {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if err := purger.PurgeChat(ctx, chatID, valid); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return false, ctxErr
			}
			if errors.Is(err, contract.ErrAborted) {
				c.count(`yabot_chats_purge_total{outcome="aborted"}`)
				c.log.Info("chat data purge aborted", "chat_id", chatID)
			} else {
				c.count(`yabot_chats_purge_total{outcome="error"}`)
				c.log.Error("chat data purge failed", "chat_id", chatID, "purger", fmt.Sprintf("%T", purger), "sqlite_code", sqliteErrorCode(err))
			}
			return false, nil
		}
	}
	return c.finishClear(ctx, chatID, event)
}

func (c *Chats) finishClear(ctx context.Context, chatID int64, event ClearFinished) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	k := keyOf(chatID)
	result := make(chan error, 1)
	work := &eventWork{event: event, reply: func(err error) { result <- err }}
	err := c.dispatch.Submit(k, dispatch.SettleOnPanic(func(_ context.Context, sh *dispatch.Shard) {
		c.enqueueInternalWork(sh, k, work)
	}, work.answer))
	if errors.Is(err, dispatch.ErrBusy) {
		c.count(`yabot_chats_maintenance_busy_total{task="chats_purge"}`)
		c.log.Info("chat maintenance inbox is full", "chat_id", chatID, "task", "chats_purge")
		return false, nil
	}
	if err != nil {
		return false, err
	}
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case err := <-result:
		var opErr *store.OpError
		if errors.Is(err, ErrMembershipChanged) || errors.As(err, &opErr) {
			return false, nil
		}
		return err == nil, err
	}
}
