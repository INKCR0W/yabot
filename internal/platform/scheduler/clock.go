package scheduler

import "time"

// Clock implementations must be safe for concurrent use: At and the scheduler goroutine both call Now.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// Stop and Reset follow the return-value and stale-tick guarantees of time.NewTimer.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

func SystemClock() Clock { return systemClock{} }

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) NewTimer(d time.Duration) Timer {
	return systemTimer{Timer: time.NewTimer(d)}
}

type systemTimer struct{ *time.Timer }

func (t systemTimer) C() <-chan time.Time { return t.Timer.C }
