package chats

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/janitor"
)

const idleScanRows = 50

type maintenance struct {
	periodicAfter int64
	idleAfter     idleCandidate
	erasure       erasureGate
	queued        sync.Map
}

type idleCandidate struct {
	id        int64
	state     ChatState
	updatedAt int64
}

type maintenanceEvent struct {
	id      int64
	trigger ProbeTrigger
}

func (c *Chats) periodicProbe(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	id, ok := c.nextPeriodic(c.periodicAfter, c.now(), c.probePeriodic)
	if !ok {
		return 0, nil
	}
	c.periodicAfter = id
	submitted, err := c.submitMaintenance(id, ProbePeriodic, "chats_probe")
	if err != nil {
		if errors.Is(err, dispatch.ErrBusy) {
			return 0, nil
		}
		return 0, err
	}
	if !submitted {
		return 0, nil
	}
	return 1, nil
}

func (c *Chats) idleSweep(ctx context.Context) (int, error) {
	candidates, err := c.idleCandidates(ctx)
	if err != nil {
		return 0, err
	}
	accepted := 0
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return accepted, err
		}
		submitted, err := c.submitMaintenance(candidate.id, ProbeIdle, "chats_idle")
		if err != nil && !errors.Is(err, dispatch.ErrBusy) {
			return accepted, err
		}
		if submitted {
			accepted++
		}
		c.idleAfter = candidate
	}
	if len(candidates) == idleScanRows {
		return accepted, janitor.ErrBacklog
	}
	c.idleAfter = idleCandidate{}
	return accepted, nil
}

func (c *Chats) idleCandidates(ctx context.Context) (candidates []idleCandidate, err error) {
	cutoff := c.now().Add(-c.policy.IdleLeaveAfter).Unix()
	after := c.idleAfter
	rows, err := c.store.Read().QueryContext(ctx, `SELECT chat_id, state, updated_at
		FROM chats WHERE state IN (?, ?) AND ever_active = 0 AND updated_at <= ?
		AND (state, updated_at, chat_id) > (?, ?, ?)
		ORDER BY state, updated_at, chat_id LIMIT ?`,
		Onboarding, Degraded, cutoff, after.state, after.updatedAt, after.id, idleScanRows)
	if err != nil {
		return nil, fmt.Errorf("query idle chats: %w", err)
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		var candidate idleCandidate
		if err := rows.Scan(&candidate.id, &candidate.state, &candidate.updatedAt); err != nil {
			return nil, fmt.Errorf("scan idle chat: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	return candidates, rows.Err()
}

func (c *Chats) submitMaintenance(id int64, trigger ProbeTrigger, task string) (bool, error) {
	candidate := maintenanceEvent{id: id, trigger: trigger}
	if _, alreadyQueued := c.queued.LoadOrStore(candidate, struct{}{}); alreadyQueued {
		return false, nil
	}
	accepted := false
	defer func() {
		if !accepted {
			c.queued.Delete(candidate)
		}
	}()
	key := keyOf(id)
	err := c.dispatch.Submit(key, func(_ context.Context, sh *dispatch.Shard) {
		work := &eventWork{event: ProbeTriggered{Trigger: trigger}, entered: func() { c.queued.Delete(candidate) }}
		c.enqueueInternalWork(sh, key, work)
	})
	if errors.Is(err, dispatch.ErrBusy) {
		c.count(`yabot_chats_maintenance_busy_total{task="` + task + `"}`)
		c.log.Info("chat maintenance inbox is full", "chat_id", id, "task", task)
	}
	accepted = err == nil
	return accepted, err
}
