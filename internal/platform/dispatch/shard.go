package dispatch

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"
)

type shardContextKey struct{}

type Shard struct {
	dispatcher  *Dispatcher
	index       int
	inbox       chan work
	completions chan Task
	ticketFreed chan struct{}
	inUse       atomic.Int64
	ready       []work
}

func newShard(d *Dispatcher, index int) *Shard {
	return &Shard{
		dispatcher:  d,
		index:       index,
		inbox:       make(chan work, d.deps.Inbox),
		completions: make(chan Task, d.deps.TicketsPerShard),
		ticketFreed: make(chan struct{}, 1),
	}
}

func (sh *Shard) Index() int { return sh.index }

func (sh *Shard) run(ctx context.Context) {
	ctx = context.WithValue(ctx, shardContextKey{}, true)
	ticks, stop := sh.dispatcher.deps.NewTicker(sh.index, sh.dispatcher.deps.Tick)
	if stop != nil {
		defer stop()
	}
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

func (sh *Shard) runTick(ctx context.Context, now time.Time) {
	for _, fn := range sh.dispatcher.tickFns {
		sh.execute(ctx, work{task: fn})
	}
	sh.drainReady(ctx)
}

func (sh *Shard) runWork(ctx context.Context, item work) {
	sh.execute(ctx, item)
	sh.drainReady(ctx)
}

func (sh *Shard) drainReady(ctx context.Context) {
	for len(sh.ready) > 0 {
		item := sh.ready[0]
		sh.ready[0] = work{}
		sh.ready = sh.ready[1:]
		sh.execute(ctx, item)
	}
}

func (sh *Shard) execute(ctx context.Context, item work) {
	returned := false
	defer func() {
		if !returned {
			// Goexit also reports a nil value here and still terminates the shard goroutine.
			value := recover()
			sh.reportPanic(ctx, item, value)
		}
	}()
	item.task(ctx, sh)
	returned = true
}

func (sh *Shard) reportPanic(ctx context.Context, item work, value any) {
	d := sh.dispatcher
	route := item.route
	counter := d.panics
	if route == "" {
		route = "none"
	} else if d.deps.Metrics != nil {
		label := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(route)
		counter = d.deps.Metrics.GetOrCreateCounter(`yabot_dispatch_panics_total{route="` + label + `"}`)
	}
	if counter != nil {
		counter.Inc()
	}
	d.deps.Log.ErrorContext(ctx, "dispatch task panicked", "shard", sh.index, "key", item.key, "route", route,
		"panic", value, "stack", string(debug.Stack()))
	if item.onPanic != nil {
		item.onPanic(fmt.Errorf("dispatch: shard %d task panicked: %v", sh.index, value))
	}
}
