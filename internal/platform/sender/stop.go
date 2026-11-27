package sender

import (
	"container/heap"
	"context"
	"time"
)

const stopGrace = 5 * time.Second

type stopWaiter struct {
	ctx context.Context
	err error
}

// Stop waits for in-flight calls and their callbacks. A caller can wait again after ctx expires.
// Do and Done must not call Stop because it would wait for the calling callback itself.
func (s *Sender) Stop(ctx context.Context) error {
	s.mu.Lock()
	switch s.state {
	case senderCreated:
		s.state = senderStopped
		close(s.done)
	case senderRunning:
		now := time.Now()
		s.state = senderStopping
		s.stopAt = now.Add(stopGrace)
		s.shutdown(Interactive)
	}
	if s.state == senderStopped {
		s.mu.Unlock()
		return nil
	}
	waiter := &stopWaiter{ctx: ctx}
	s.stopWaiters[waiter] = struct{}{}
	if deadline, ok := ctx.Deadline(); ok {
		s.stopAt = earlierPause(s.stopAt, deadline)
	}
	s.advanceStop(time.Now())
	s.signal()
	s.mu.Unlock()

	select {
	case <-s.done:
		return waiter.err
	case <-ctx.Done():
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.stopWaiters, waiter)
		if s.state == senderStopped {
			return waiter.err
		}
		s.abortStop()
		s.signal()
		return ctx.Err()
	}
}

func (s *Sender) finishStop(now time.Time) {
	for waiter := range s.stopWaiters {
		waiter.err = waiter.ctx.Err()
		if deadline, ok := waiter.ctx.Deadline(); waiter.err == nil && ok && !now.Before(deadline) {
			waiter.err = context.DeadlineExceeded
		}
	}
	clear(s.stopWaiters)
	s.state = senderStopped
	s.base = nil
	close(s.done)
}

func (s *Sender) advanceStop(now time.Time) {
	if s.state != senderStopping || s.abort {
		return
	}
	if !now.Before(s.stopAt) {
		s.abortStop()
		return
	}
	for waiter := range s.stopWaiters {
		if waiter.ctx.Err() != nil {
			s.abortStop()
			return
		}
	}
}

func (s *Sender) abortStop() {
	if !s.abort {
		s.abort = true
		s.shutdown(Deadline)
	}
}

func (s *Sender) shutdown(first Class) {
	for class := first; class <= Background; class++ {
		for _, entry := range s.queue.drain(class) {
			s.queueDrop(entry, Shutdown)
		}
	}
	kept := s.retries[:0]
	for _, entry := range s.retries {
		if entry.request.Class >= first {
			s.queueDrop(entry, Shutdown)
		} else {
			kept = append(kept, entry)
		}
	}
	clear(s.retries[len(kept):])
	s.retries = kept
	heap.Init(&s.retries)

	for run := range s.calls {
		if run.entry.request.Class >= first && run.cancel != nil {
			run.cancel()
		}
	}
}

func (s *Sender) shuttingDown(class Class) bool {
	return s.state == senderStopping && (s.abort || class >= Interactive)
}
