package dispatch

import (
	"errors"
	"sync"
	"time"
)

var ErrSettleTimeout = errors.New("dispatch: timed out waiting for critical routes")

type Receipt struct {
	state *receiptState
}

// Wait settles the receipt with a timeout error if critical routes remain pending at the deadline.
// A nonpositive timeout checks immediately. All callers observe the same settled result.
func (rc Receipt) Wait(timeout time.Duration) error {
	s := rc.state
	if s == nil {
		return nil
	}
	select {
	case <-s.done:
		return s.err
	default:
	}
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-s.done:
			return s.err
		case <-timer.C:
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending != 0 {
		s.settle(ErrSettleTimeout)
	}
	return s.err
}

type receiptState struct {
	mu      sync.Mutex
	done    chan struct{}
	pending int
	acks    []*routeAck
	err     error
}

func newReceiptState() *receiptState {
	// Reserve one slot so fast handlers cannot settle the receipt while Accept is adding routes.
	return &receiptState{done: make(chan struct{}), pending: 1}
}

func (s *receiptState) add(route *registeredRoute, updateID int64) *routeAck {
	ack := &routeAck{state: s, route: route, updateID: updateID}
	s.mu.Lock()
	s.acks = append(s.acks, ack)
	s.pending++
	s.mu.Unlock()
	return ack
}

func (s *receiptState) finishRouting() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.complete()
}

func (s *receiptState) complete() {
	s.pending--
	if s.pending == 0 {
		s.settle(nil)
	}
}

func (s *receiptState) settle(timeoutErr error) {
	var failures []error
	for _, ack := range s.acks {
		err := ack.err
		if !ack.called {
			err = timeoutErr
		}
		if err != nil {
			failures = append(failures, &routeError{name: ack.route.Name, cause: err})
		}
		ack.err = nil
	}
	s.pending = 0
	s.acks = nil
	s.err = errors.Join(failures...)
	close(s.done)
}

type routeAck struct {
	state    *receiptState
	route    *registeredRoute
	updateID int64
	called   bool
	err      error
}

func (a *routeAck) Done(err error) {
	if a.complete(err) {
		if counter := a.route.duplicates; counter != nil {
			counter.Inc()
		}
		a.route.ackLog.Warn("dispatch ack already completed", "route", a.route.Name, "update_id", a.updateID)
	}
}

func (a *routeAck) complete(err error) (duplicate bool) {
	s := a.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if a.called || s.pending == 0 {
		return a.called
	}
	a.called = true
	a.err = err
	s.complete()
	return false
}

type routeError struct {
	name  string
	cause error
}

func (e *routeError) Error() string { return "dispatch: route " + e.name + ": " + e.cause.Error() }
func (e *routeError) Unwrap() error { return e.cause }
