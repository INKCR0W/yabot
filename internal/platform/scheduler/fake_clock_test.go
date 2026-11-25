package scheduler

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

func TestFakeClockAdvanceDeliveryOrder(t *testing.T) {
	start := time.Unix(100, 0)
	clock := NewFakeClock(start)
	delays := []time.Duration{3 * time.Second, time.Second, 4 * time.Second, 2 * time.Second}
	// A shared FIFO records the send order without depending on receiver scheduling.
	ticks := make(chan time.Time, len(delays))
	for _, delay := range delays {
		timer := clock.NewTimer(delay).(*fakeTimer)
		timer.tick = ticks
	}

	clock.Advance(3 * time.Second)
	var delivered []time.Duration
	for range len(ticks) {
		delivered = append(delivered, (<-ticks).Sub(start))
	}
	if want := []time.Duration{time.Second, 2 * time.Second, 3 * time.Second}; !slices.Equal(delivered, want) {
		t.Fatalf("delivered deadlines = %v, want %v", delivered, want)
	}
}

func TestFakeClockAdvance(t *testing.T) {
	start := time.Unix(100, 0)
	clock := NewFakeClock(start)
	late := clock.NewTimer(3 * time.Second)
	early := clock.NewTimer(time.Second)
	middle := clock.NewTimer(2 * time.Second)

	clock.Advance(time.Second - time.Nanosecond)
	assertNoFakeTick(t, early)
	clock.Advance(time.Nanosecond)
	assertFakeTick(t, early, start.Add(time.Second))
	assertNoFakeTick(t, middle)
	assertNoFakeTick(t, late)

	clock.Advance(3 * time.Second)
	assertFakeTick(t, middle, start.Add(2*time.Second))
	assertFakeTick(t, late, start.Add(3*time.Second))
	if got, want := clock.Now(), start.Add(4*time.Second); !got.Equal(want) {
		t.Fatalf("Now() = %v, want %v", got, want)
	}
	stillPending := clock.NewTimer(2 * time.Second)
	clock.Advance(-4 * time.Second)
	if got := clock.Now(); !got.Equal(start) {
		t.Fatalf("Now() after moving backward = %v, want %v", got, start)
	}
	clock.Advance(4 * time.Second)
	assertNoFakeTick(t, early)
	assertNoFakeTick(t, middle)
	assertNoFakeTick(t, late)
	assertNoFakeTick(t, stillPending)
	clock.Advance(2 * time.Second)
	assertFakeTick(t, stillPending, start.Add(6*time.Second))
}

func TestFakeClockTimerStopAndReset(t *testing.T) {
	for _, tc := range []struct {
		name   string
		setup  func(*testing.T, *FakeClock, Timer)
		active bool
	}{
		{name: "armed", active: true},
		{name: "expired unread", active: true, setup: func(_ *testing.T, c *FakeClock, _ Timer) {
			c.Advance(time.Second)
		}},
		{name: "expired received", setup: func(t *testing.T, c *FakeClock, timer Timer) {
			c.Advance(time.Second)
			assertFakeTick(t, timer, c.Now())
		}},
		{name: "stopped", setup: func(t *testing.T, _ *FakeClock, timer Timer) {
			if !timer.Stop() {
				t.Fatal("first Stop() = false, want true")
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("stop", func(t *testing.T) {
				clock := NewFakeClock(time.Unix(100, 0))
				timer := clock.NewTimer(time.Second)
				if tc.setup != nil {
					tc.setup(t, clock, timer)
				}
				if got := timer.Stop(); got != tc.active {
					t.Fatalf("Stop() = %v, want %v", got, tc.active)
				}
				clock.Advance(3 * time.Second)
				assertNoFakeTick(t, timer)
			})
			t.Run("reset", func(t *testing.T) {
				clock := NewFakeClock(time.Unix(100, 0))
				timer := clock.NewTimer(time.Second)
				if tc.setup != nil {
					tc.setup(t, clock, timer)
				}
				if got := timer.Reset(2 * time.Second); got != tc.active {
					t.Fatalf("Reset() = %v, want %v", got, tc.active)
				}
				clock.Advance(time.Second)
				assertNoFakeTick(t, timer)
				clock.Advance(time.Second)
				assertFakeTick(t, timer, clock.Now())
			})
		})
	}
}

func TestFakeClockImmediateTimers(t *testing.T) {
	for _, delay := range []time.Duration{-time.Second, 0} {
		t.Run(delay.String(), func(t *testing.T) {
			start := time.Unix(100, 0)
			clock := NewFakeClock(start)
			timer := clock.NewTimer(delay)
			assertFakeTick(t, timer, start)
			if timer.Reset(delay) {
				t.Fatal("Reset returned true after the immediate tick was received")
			}
			if !timer.Reset(delay) {
				t.Fatal("Reset returned false with an unread immediate tick")
			}
			assertFakeTick(t, timer, start)
			assertNoFakeTick(t, timer)
		})
	}
}

func TestFakeClockWaitForTimer(t *testing.T) {
	for _, tc := range []struct {
		name string
		arm  func(*testing.T, *FakeClock, Timer)
	}{
		{"new timer", func(_ *testing.T, clock *FakeClock, _ Timer) {
			clock.NewTimer(2 * time.Second)
		}},
		{"reset timer", func(t *testing.T, _ *FakeClock, timer Timer) {
			if !timer.Reset(2 * time.Second) {
				t.Fatal("Reset returned false for an active timer")
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Unix(100, 0).In(time.FixedZone("test", 3600))
				clock := NewFakeClock(start)
				timer := clock.NewTimer(time.Second)
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				if err := clock.WaitForTimer(ctx, start.Add(time.Second).UTC()); err != nil {
					t.Fatalf("WaitForTimer for an active timer: %v", err)
				}

				done := make(chan error, 1)
				go func() { done <- clock.WaitForTimer(ctx, start.Add(2*time.Second)) }()
				synctest.Wait()
				select {
				case err := <-done:
					t.Fatalf("WaitForTimer returned before the matching timer was armed: %v", err)
				default:
				}
				tc.arm(t, clock, timer)
				if err := <-done; err != nil {
					t.Fatalf("WaitForTimer after arming the matching timer: %v", err)
				}
			})
		})
	}
}

func TestFakeClockWaitForTimerContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(100, 0)
		clock := NewFakeClock(start)
		clock.NewTimer(2 * time.Second)
		clock.Advance(2 * time.Second)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- clock.WaitForTimer(ctx, start.Add(2*time.Second)) }()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitForTimer for an expired timer = %v, want context.Canceled", err)
		}

		timer := clock.NewTimer(time.Second)
		if !timer.Stop() {
			t.Fatal("Stop returned false for an active timer")
		}
		ctx, cancelTimeout := context.WithTimeout(t.Context(), time.Second)
		defer cancelTimeout()
		if err := clock.WaitForTimer(ctx, start.Add(3*time.Second)); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("WaitForTimer for a stopped timer = %v, want context.DeadlineExceeded", err)
		}
	})
}

func assertFakeTick(t *testing.T, timer Timer, want time.Time) {
	t.Helper()
	select {
	case got := <-timer.C():
		if !got.Equal(want) {
			t.Fatalf("tick = %v, want %v", got, want)
		}
	default:
		t.Fatalf("no tick for %v", want)
	}
}

func assertNoFakeTick(t *testing.T, timer Timer) {
	t.Helper()
	select {
	case got := <-timer.C():
		t.Fatalf("unexpected tick for %v", got)
	default:
	}
}
