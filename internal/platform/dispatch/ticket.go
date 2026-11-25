package dispatch

import (
	"context"
	"sync/atomic"
)

type Ticket struct {
	state *ticketState
}

type ticketState struct {
	shard   *Shard
	claimed atomic.Bool
}

// Admit must be called on the shard goroutine. It returns false during exit cleanup.
func (sh *Shard) Admit() (Ticket, bool) {
	if sh.exiting || sh.inUse.Load() >= int64(cap(sh.completions)) {
		return Ticket{}, false
	}
	sh.inUse.Add(1)
	return Ticket{state: &ticketState{shard: sh}}, true
}

// Complete may run on any goroutine. It panics on a zero ticket or a nil callback.
// Wrap fn with SettleOnPanic when it settles a critical ack.
func (t Ticket) Complete(fn func(ctx context.Context, sh *Shard)) {
	if fn == nil {
		panic("dispatch: ticket completion callback must not be nil")
	}
	if t.state == nil {
		panic("dispatch: cannot complete a zero ticket")
	}
	sh := t.state.shard
	if !t.state.claimed.CompareAndSwap(false, true) {
		if counter := sh.dispatcher.lateComplete; counter != nil {
			counter.Inc()
		}
		return
	}
	// Each unreturned ticket reserves one slot until its completion is dequeued.
	sh.completions <- fn
}

// Cancel may run on any goroutine. It panics on a zero ticket.
func (t Ticket) Cancel() {
	if t.state == nil {
		panic("dispatch: cannot cancel a zero ticket")
	}
	sh := t.state.shard
	if !t.state.claimed.CompareAndSwap(false, true) {
		if counter := sh.dispatcher.lateCancel; counter != nil {
			counter.Inc()
		}
		return
	}
	if sh.inUse.Add(-1) == 0 {
		select {
		case sh.ticketFreed <- struct{}{}:
		default:
		}
	}
}
