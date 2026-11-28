package chats

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/metrics"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
)

type Reporter interface {
	ReportPermissionLost(chatID int64)
	ReportMigrated(oldID, newID int64)
	ReportUnknown(chatID int64)
	ReportOwnerStale(chatID int64)
}

const reportLimit = 10_000
const reportsPerTick = 64

type reportKind string

const (
	reportPermissionLost reportKind = "permission_lost"
	reportMigrated       reportKind = "migrated"
	reportUnknown        reportKind = "unknown"
	reportOwnerStale     reportKind = "owner_stale"
)

type reportKey struct {
	chatID int64
	kind   reportKind
}

type reportEntry struct {
	event  Event
	posted bool
}

type reportQueue struct {
	mu        sync.Mutex
	entries   map[reportKey]reportEntry
	dropped   *metrics.Counter
	overflows atomic.Uint64
}

func (c *Chats) ReportPermissionLost(chatID int64) {
	c.report(reportKey{chatID, reportPermissionLost}, ProbeTriggered{Trigger: ProbePermissionLost})
}

func (c *Chats) ReportMigrated(oldID, newID int64) {
	c.report(reportKey{oldID, reportMigrated}, MigrationReported{To: newID})
}

func (c *Chats) ReportUnknown(chatID int64) {
	c.report(reportKey{chatID, reportUnknown}, ProbeTriggered{Trigger: ProbeUnknown})
}

func (c *Chats) ReportOwnerStale(chatID int64) {
	c.report(reportKey{chatID, reportOwnerStale}, ProbeTriggered{Trigger: ProbeOwnerStale})
}

func (c *Chats) report(key reportKey, event Event) {
	queue := &c.reports
	queue.mu.Lock()
	if _, exists := queue.entries[key]; exists {
		queue.mu.Unlock()
		return
	}
	if len(queue.entries) == reportLimit {
		queue.mu.Unlock()
		queue.overflows.Add(1)
		if queue.dropped != nil {
			queue.dropped.Inc()
		}
		return
	}
	queue.entries[key] = reportEntry{event: event}
	queue.mu.Unlock()
}

func (c *Chats) tick(ctx context.Context, sh *dispatch.Shard) {
	if !c.ready.Load() || ctx.Err() != nil {
		return
	}
	now := c.now()
	local := c.local(sh)
	if dropped := c.reports.overflows.Swap(0); dropped != 0 {
		c.log.Warn("chat report queue is full", "dropped", dropped)
	}
	c.expireMemory(sh, local, now)
	for id, due := range local.idleDue {
		key := keyOf(id)
		if !now.Before(due) && !sh.Busy(key) {
			delete(local.idleDue, id)
			c.enqueueInternal(sh, key, ProbeTriggered{Trigger: ProbeIdle})
		}
	}
	c.flushReports()
}

func (c *Chats) flushReports() {
	type submission struct {
		key   reportKey
		event Event
	}
	var batch [reportsPerTick]submission
	n := 0
	queue := &c.reports
	queue.mu.Lock()
	for key, entry := range queue.entries {
		if entry.posted {
			continue
		}
		batch[n] = submission{key, entry.event}
		entry.posted = true
		queue.entries[key] = entry
		n++
		if n == len(batch) {
			break
		}
	}
	queue.mu.Unlock()

	for _, post := range batch[:n] {
		key := keyOf(post.key.chatID)
		err := c.dispatch.Submit(key, func(_ context.Context, sh *dispatch.Shard) {
			c.enqueueInternalWork(sh, key, &eventWork{
				event:   post.event,
				entered: func() { queue.remove(post.key) },
			})
		})
		if err != nil {
			busy := errors.Is(err, dispatch.ErrBusy)
			queue.mu.Lock()
			if busy {
				queue.entries[post.key] = reportEntry{event: post.event}
			} else {
				delete(queue.entries, post.key)
			}
			queue.mu.Unlock()
			if busy {
				c.count(`yabot_chats_overload_total{source="report_inbox"}`)
				c.log.Info("chat report deferred because shard inbox is full", "chat_id", post.key.chatID, "kind", string(post.key.kind))
			} else {
				c.count(`yabot_chats_reports_dropped_total{reason="dispatch_closed"}`)
				c.log.Info("chat report could not be submitted", "chat_id", post.key.chatID)
			}
		}
	}
}

func (q *reportQueue) remove(key reportKey) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.entries, key)
}
