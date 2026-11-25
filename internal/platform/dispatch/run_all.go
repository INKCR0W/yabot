package dispatch

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

var ErrInShard = errors.New("dispatch: RunOnAll cannot be called from a shard")

// AsyncTask may call done from another goroutine. Only its first call counts.
type AsyncTask func(ctx context.Context, sh *Shard, done func(error))

type asyncResult struct {
	called   atomic.Bool
	done     chan error
	returned chan error
}

// RunOnAll submits once to each shard and waits for their results or for ctx to end.
// Detecting a call from a shard requires passing the shard's context or a context derived from it.
func (d *Dispatcher) RunOnAll(ctx context.Context, t AsyncTask) error {
	if ctx.Value(shardContextKey{}) != nil {
		return ErrInShard
	}
	if !d.isRunning() {
		return ErrClosed
	}
	results := make([]*asyncResult, len(d.shards))
	for i, sh := range d.shards {
		result := &asyncResult{done: make(chan error, 1), returned: make(chan error, 1)}
		item := work{
			task: func(ctx context.Context, sh *Shard) {
				t(ctx, sh, func(err error) {
					if result.called.CompareAndSwap(false, true) {
						result.done <- err
					} else if d.lateDone != nil {
						d.lateDone.Inc()
					}
				})
				result.returned <- nil
			},
			onPanic: func(err error) { result.returned <- err },
		}
		if err := d.enqueueRunOnAll(ctx, sh, item); err != nil {
			return err
		}
		results[i] = result
	}
	errs := make([]error, len(results))
	for i, result := range results {
		errs[i] = result.wait(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return errors.Join(errs...)
}

func (d *Dispatcher) enqueueRunOnAll(ctx context.Context, sh *Shard, item work) error {
	const maxBackoff = 50 * time.Millisecond
	for delay := time.Millisecond; ; delay = min(2*delay, maxBackoff) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := d.enqueue(sh, item); err != ErrBusy {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.closing:
			return ErrClosed
		case <-time.After(delay):
		}
	}
}

func (r *asyncResult) wait(ctx context.Context) error {
	// A panic takes precedence even if the task called done before it panicked.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-r.returned:
		if err != nil {
			return err
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-r.done:
		return err
	}
}
