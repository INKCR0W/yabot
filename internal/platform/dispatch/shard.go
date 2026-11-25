package dispatch

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"
)

type shardContextKey struct{}

// Shard mailbox methods require the owning goroutine and panic on keys from other shards.
type Shard struct {
	dispatcher  *Dispatcher
	index       int
	inbox       chan work
	completions chan Task
	ticketFreed chan struct{}
	inUse       atomic.Int64
	ready       []readyItem
	runCtx      context.Context
	mailboxes   map[Key]*mailbox
	retries     map[Key]time.Time
	exiting     bool
}

func newShard(d *Dispatcher, index int) *Shard {
	return &Shard{
		dispatcher:  d,
		index:       index,
		inbox:       make(chan work, d.deps.Inbox),
		completions: make(chan Task, d.deps.TicketsPerShard),
		ticketFreed: make(chan struct{}, 1),
		mailboxes:   make(map[Key]*mailbox),
		retries:     make(map[Key]time.Time),
	}
}

func (sh *Shard) Index() int { return sh.index }

func (sh *Shard) run(ctx context.Context) {
	ctx = context.WithValue(ctx, shardContextKey{}, true)
	sh.runCtx = ctx
	ticks, stop := sh.dispatcher.deps.NewTicker(sh.index, sh.dispatcher.deps.Tick)
	if stop != nil {
		defer stop()
	}
	defer sh.abortMailboxes()
	closing := sh.dispatcher.closing
	var tick time.Time
	var inboxItem work
	var tickReady, inboxReady bool
	for {
		if ctx.Err() != nil {
			// Submissions are already disabled when the run context is canceled.
			sh.drainCanceled(ctx, inboxReady, inboxItem)
			return
		}
		select {
		case <-closing:
			closing = nil
		default:
		}
		if closing == nil && !inboxReady && len(sh.inbox) == 0 && sh.inUse.Load() == 0 {
			return
		}
		select {
		case fn := <-sh.completions:
			sh.runCompletion(ctx, fn)
			continue
		default:
		}
		if !tickReady {
			select {
			case tick, tickReady = <-ticks:
				if !tickReady {
					ticks = nil
				}
			default:
			}
		}
		if tickReady {
			tickReady = false
			sh.runTick(ctx, tick)
			continue
		}
		if inboxReady {
			inboxReady = false
			sh.runWork(ctx, inboxItem)
			inboxItem = work{}
			continue
		}

		// Recheck higher priorities after the blocking select takes a lower-priority item.
		select {
		case fn := <-sh.completions:
			sh.runCompletion(ctx, fn)
		case tick, tickReady = <-ticks:
			if !tickReady {
				ticks = nil
			}
		case inboxItem = <-sh.inbox:
			inboxReady = true
		case <-sh.ticketFreed:
		case <-closing:
			closing = nil
		case <-ctx.Done():
		}
	}
}

// drainCanceled runs the completions already queued, then the accepted inbox work.
// Completions queued after this point are not awaited, so a completion chain cannot delay exit.
func (sh *Shard) drainCanceled(ctx context.Context, inboxReady bool, inboxItem work) {
	for range len(sh.completions) {
		sh.runCompletion(ctx, <-sh.completions)
	}
	if inboxReady {
		sh.runWork(ctx, inboxItem)
	}
	for len(sh.inbox) != 0 {
		sh.runWork(ctx, <-sh.inbox)
	}
}

func (sh *Shard) runCompletion(ctx context.Context, fn Task) {
	sh.inUse.Add(-1)
	sh.runWork(ctx, work{task: fn})
}

func (sh *Shard) runTick(ctx context.Context, _ time.Time) {
	for _, fn := range sh.dispatcher.tickFns {
		sh.execute(ctx, work{task: fn})
	}
	// Tick callbacks finish before due replays are released.
	sh.releaseDueReplays()
	sh.drainReady(ctx)
}

func (sh *Shard) runWork(ctx context.Context, item work) {
	sh.execute(ctx, item)
	sh.drainReady(ctx)
}

func (sh *Shard) drainReady(ctx context.Context) {
	for len(sh.ready) > 0 {
		item := sh.ready[0]
		sh.ready[0] = readyItem{}
		sh.ready = sh.ready[1:]
		sh.runReadyItem(ctx, item)
	}
}

func (sh *Shard) execute(ctx context.Context, item work) {
	returned := false
	defer func() {
		if !returned {
			// Goexit also reports a nil value here and still terminates the shard goroutine.
			err := &PanicError{Shard: sh.index, Value: recover(), Stack: debug.Stack()}
			sh.reportPanic(ctx, item, err)
			if item.onPanic != nil {
				sh.execute(ctx, work{key: item.key, route: item.route, task: func(context.Context, *Shard) {
					item.onPanic(err)
				}})
			}
		}
	}()
	item.task(ctx, sh)
	returned = true
}

type PanicError struct {
	// Shard is -1 for routing callbacks executed before work reaches a shard.
	Shard int
	Value any
	Stack []byte
}

const ingressPanicShard = -1

func (e *PanicError) Error() string {
	if e.Shard == ingressPanicShard {
		return fmt.Sprintf("dispatch: update routing panicked: %v", e.Value)
	}
	return fmt.Sprintf("dispatch: shard %d task panicked: %v", e.Shard, e.Value)
}

func (sh *Shard) reportPanic(ctx context.Context, item work, err *PanicError) {
	d := sh.dispatcher
	d.countPanic(item.route)
	route := item.route
	if route == "" {
		route = "none"
	}
	d.deps.Log.ErrorContext(ctx, "dispatch task panicked", "shard", sh.index, "key", item.key, "route", route,
		"panic", err.Value, "stack", string(err.Stack), "error", err)
}
