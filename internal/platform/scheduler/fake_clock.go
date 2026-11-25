package scheduler

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"time"
)

// FakeClock must be created with NewFakeClock. It advances only through Advance, and its methods are safe for concurrent use.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	active map[*fakeTimer]struct{}
	armed  chan struct{}
	order  uint64
}

func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{
		now:    start,
		active: make(map[*fakeTimer]struct{}),
		armed:  make(chan struct{}),
	}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) NewTimer(d time.Duration) Timer {
	t := &fakeTimer{clock: c, tick: make(chan time.Time, 1)}
	c.mu.Lock()
	c.arm(t, d)
	c.mu.Unlock()
	return t
}

// Advance delivers due ticks in deadline order. Moving backward does not restore fired timers.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for _, t := range dueTimers(c.active, c.now) {
		delete(c.active, t)
		t.tick <- t.deadline
	}
}

func dueTimers(active map[*fakeTimer]struct{}, now time.Time) []*fakeTimer {
	var due []*fakeTimer
	for t := range active {
		if !t.deadline.After(now) {
			due = append(due, t)
		}
	}
	slices.SortFunc(due, func(a, b *fakeTimer) int {
		if order := a.deadline.Compare(b.deadline); order != 0 {
			return order
		}
		return cmp.Compare(a.order, b.order)
	})
	return due
}

// WaitForTimer waits for an active timer at deadline or for ctx to end. An already
// expired timer does not match, even if its tick has not been received.
func (c *FakeClock) WaitForTimer(ctx context.Context, deadline time.Time) error {
	for {
		c.mu.Lock()
		for t := range c.active {
			if t.deadline.Equal(deadline) {
				c.mu.Unlock()
				return nil
			}
		}
		armed := c.armed
		c.mu.Unlock()

		select {
		case <-armed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type fakeTimer struct {
	clock    *FakeClock
	tick     chan time.Time
	deadline time.Time
	order    uint64
}

func (t *fakeTimer) C() <-chan time.Time { return t.tick }

func (t *fakeTimer) Stop() bool {
	c := t.clock
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.disarm(t)
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	c := t.clock
	c.mu.Lock()
	defer c.mu.Unlock()
	active := c.disarm(t)
	c.arm(t, d)
	return active
}

func (c *FakeClock) arm(t *fakeTimer, d time.Duration) {
	t.deadline = c.now.Add(max(d, 0))
	if d <= 0 {
		t.tick <- t.deadline
		return
	}
	t.order = c.order
	c.order++
	c.active[t] = struct{}{}
	close(c.armed)
	c.armed = make(chan struct{})
}

func (c *FakeClock) disarm(t *fakeTimer) bool {
	if _, ok := c.active[t]; ok {
		delete(c.active, t)
		return true
	}
	// A channel timer can still retract an expired tick until it is received.
	select {
	case <-t.tick:
		return true
	default:
		return false
	}
}
