package scheduler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
)

// A persisted wall-only deadline must be compared on the monotonic clock once queued,
// so a later wall clock step cannot reorder it against in-process deadlines.
func TestAtAnchorsDeadlinesToMonotonicClock(t *testing.T) {
	s, err := New(Deps{Dispatcher: submitterFunc(func(dispatch.Key, dispatch.Task) error { return nil })})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	wallOnly := now.Add(time.Hour).Round(0)
	s.At(wallOnly, dispatch.Key{}, func(context.Context, *dispatch.Shard) {})
	got := s.pending[0].at
	if !strings.Contains(got.String(), " m=") {
		t.Fatalf("queued deadline %v has no monotonic reading", got)
	}
	if d := got.Round(0).Sub(wallOnly); d < -time.Millisecond || d > time.Millisecond {
		t.Fatalf("queued deadline moved by %v", d)
	}
}

type submitterFunc func(dispatch.Key, dispatch.Task) error

func (f submitterFunc) Submit(k dispatch.Key, t dispatch.Task) error { return f(k, t) }
