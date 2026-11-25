package dispatch

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"testing"
	"testing/synctest"
	"time"
	"weak"
)

func TestMailboxReadyItemsAcrossEntities(t *testing.T) {
	for _, tc := range []struct {
		name string
		hold bool
		want []string
	}{
		{name: "idle", want: []string{"A1", "B1", "A2"}},
		{name: "held by another entity", hold: true, want: []string{"A1", "B1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := startTestDispatcher(t, testDeps())
			a, b := Key{A: 1}, Key{A: 2}
			var trace mailboxTestTrace
			runMailboxTestTask(t, d, func(sh *Shard) {
				sh.DeferInternal(a, trace.item("A1", nil))
				sh.DeferInternal(b, trace.item("B1", func(_ context.Context, sh *Shard) {
					if tc.hold {
						sh.Hold(a)
					}
				}))
				sh.DeferInternal(a, trace.item("A2", nil))
			})
			trace.check(t, tc.want...)
			if tc.hold {
				runMailboxTestTask(t, d, func(sh *Shard) { sh.Release(a) })
				trace.check(t, "A1", "B1", "A2")
			}
		})
	}
}

func TestMailboxReleaseQueuesBehindReadyItems(t *testing.T) {
	for _, tc := range []struct {
		name   string
		replay bool
		want   []string
	}{
		{name: "hold", want: []string{"A1", "B1", "B2", "C1", "A2", "A3"}},
		{name: "requeue", replay: true, want: []string{"A1", "B1", "B2", "C1", "replay", "A2", "A3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := startTestDispatcher(t, testDeps())
			a, b, c := Key{A: 1}, Key{A: 2}, Key{A: 3}
			var trace mailboxTestTrace
			runMailboxTestTask(t, d, func(sh *Shard) {
				sh.DeferInternal(a, trace.item("A1", nil))
				sh.DeferInternal(b, trace.item("B1", func(_ context.Context, sh *Shard) {
					if tc.replay {
						sh.Requeue(a, trace.item("replay", nil), 0)
					} else {
						sh.Hold(a)
					}
				}))
				sh.DeferInternal(a, trace.item("A2", nil))
				sh.DeferInternal(b, trace.item("B2", func(_ context.Context, sh *Shard) { sh.Release(a) }))
				sh.DeferInternal(a, trace.item("A3", nil))
				sh.DeferInternal(c, trace.item("C1", nil))
			})
			trace.check(t, tc.want...)
		})
	}
}

func TestMailboxDropKeepsNewItemReadyPosition(t *testing.T) {
	d := startTestDispatcher(t, testDeps())
	a, b := Key{A: 1}, Key{A: 2}
	var trace mailboxTestTrace
	runMailboxTestTask(t, d, func(sh *Shard) {
		sh.DeferInternal(a, trace.item("A1", nil))
		sh.DeferInternal(b, trace.item("B1", nil))
		sh.DeferInternal(a, trace.item("A2", nil))
		sh.Drop(a, errors.New("entity deleted"))
		sh.DeferInternal(a, trace.item("A3", nil))
	})
	trace.check(t, "abort A1", "abort A2", "B1", "A3")
}

func TestMailboxReleaseKeepsReadyPositions(t *testing.T) {
	for _, tc := range []struct {
		name string
		hold bool
		want []string
	}{
		{name: "already ready", want: []string{"A1", "B1", "A2", "B2", "A3"}},
		{name: "released", hold: true, want: []string{"B1", "A1", "A2", "B2", "A3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := startTestDispatcher(t, testDeps())
			a, b := Key{A: 1}, Key{A: 2}
			var trace mailboxTestTrace
			runMailboxTestTask(t, d, func(sh *Shard) {
				if tc.hold {
					sh.Hold(a)
				}
				sh.DeferInternal(a, trace.item("A1", func(_ context.Context, sh *Shard) {
					sh.DeferInternal(a, trace.item("A3", nil))
				}))
				sh.DeferInternal(b, trace.item("B1", nil))
				sh.DeferInternal(a, trace.item("A2", nil))
				if got := sh.Busy(a); got != tc.hold {
					t.Errorf("Busy = %t with queued items, want %t", got, tc.hold)
				}
				sh.Release(a)
				sh.DeferInternal(b, trace.item("B2", nil))
			})
			trace.check(t, tc.want...)
		})
	}
}

func TestMailboxTickReleasesAllDueReplaysAfterCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := testDeps()
		clock := newMailboxLifecycleClock(&deps)
		var clockReads int
		deps.Now = func() time.Time {
			clockReads++
			return clock.now
		}
		d, err := New(deps)
		if err != nil {
			t.Fatal(err)
		}
		var allHeld bool
		var trace mailboxTestTrace
		a, b, c, tickKey := Key{A: 1}, Key{A: 2}, Key{A: 3}, Key{A: 4}
		d.OnTick(func(_ context.Context, sh *Shard) {
			allHeld = sh.Busy(a) && sh.Busy(b) && sh.Busy(c)
			sh.DeferInternal(tickKey, trace.item("tick", nil))
		})
		if err := d.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := d.Stop(ctx); err != nil {
				t.Error(err)
			}
		})
		runMailboxTestTask(t, d, func(sh *Shard) {
			sh.Requeue(a, trace.item("A", nil), 0)
			sh.Requeue(b, trace.item("B", nil), 0)
			sh.Requeue(c, trace.item("C", nil), 1)
		})
		clock.tick(100 * time.Millisecond)
		if !allHeld {
			t.Error("the tick callback found an entity released before the callback ran")
		}
		if len(trace) < 3 || trace[1] != "A" && trace[1] != "B" {
			t.Fatalf("first tick ran %q, want tick followed by both due replays", trace)
		}
		if trace[1] == "A" {
			trace.check(t, "tick", "A", "B")
		} else {
			trace.check(t, "tick", "B", "A")
		}
		clock.tick(100 * time.Millisecond)
		if len(trace) != 5 || trace[3] != "tick" || trace[4] != "C" {
			t.Errorf("second tick produced %q, want tick then C after the first batch", trace)
		}
		before := clockReads
		clock.tick(time.Second)
		if clockReads != before {
			t.Error("the dispatcher read the replay clock after all deadlines were cleared")
		}
		if len(trace) != 6 || trace[5] != "tick" {
			t.Errorf("idle tick produced %q, want only the tick item after both batches", trace)
		}
	})
}

func TestMailboxReleasesDequeuedStorage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := startTestDispatcher(t, testDeps())
		a, b := Key{A: 1}, Key{A: 2}
		var payload weak.Pointer[[128]byte]
		var idleBox weak.Pointer[mailbox]
		var trace mailboxTestTrace
		runMailboxTestTask(t, d, func(sh *Shard) {
			item := trace.item("A1", func(_ context.Context, sh *Shard) { sh.Hold(a) })
			value := new([128]byte)
			payload = weak.Make(value)
			item.Abort = func(error) { runtime.KeepAlive(value) }
			sh.DeferInternal(a, item)
			sh.DeferInternal(a, trace.item("A2", nil))
			sh.DeferInternal(b, trace.item("B1", nil))
			idleBox = weak.Make(sh.mailboxes[b])
		})
		synctest.Wait()
		trace.check(t, "A1", "B1")
		runtime.GC()
		if payload.Value() != nil {
			t.Error("the held mailbox retained a dequeued item's callback")
		}
		if idleBox.Value() != nil {
			t.Error("the ready queue retained a drained entity")
		}
		runMailboxTestTask(t, d, func(sh *Shard) { sh.Release(a) })
		trace.check(t, "A1", "B1", "A2")
	})
}

func TestMailboxStopAbortsItemsEnqueuedByAbort(t *testing.T) {
	d := startTestDispatcher(t, testDeps())
	key := Key{A: 1}
	var trace mailboxTestTrace
	var recreated weak.Pointer[mailbox]
	runMailboxTestTask(t, d, func(sh *Shard) {
		sh.Hold(key)
		item := trace.item("original", nil)
		abort := item.Abort
		item.Abort = func(reason error) {
			abort(reason)
			if reason != ErrClosed {
				t.Errorf("original Abort = %v, want %v", reason, ErrClosed)
			}
			sh.DeferInternal(key, trace.item("replacement", nil))
			recreated = weak.Make(sh.mailboxes[key])
		}
		sh.DeferInternal(key, item)
	})
	if err := d.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	trace.check(t, "abort original", "abort replacement")
	runtime.GC()
	if recreated.Value() != nil {
		t.Error("the stopped dispatcher retained the replacement mailbox")
	}
	runtime.KeepAlive(d)
}

func TestMailboxInterleavedEntities(t *testing.T) {
	a, b, c := Key{A: 1}, Key{A: 2}, Key{A: 3}
	type delivery struct {
		key  Key
		name string
	}
	var tail []delivery
	for entity, key := range []Key{a, b, c} {
		for id := 2; id <= 5; id++ {
			tail = append(tail, delivery{key: key, name: fmt.Sprintf("%c%d", 'A'+entity, id)})
		}
	}
	rng := rand.New(rand.NewPCG(11, 27))
	rng.Shuffle(len(tail), func(i, j int) { tail[i], tail[j] = tail[j], tail[i] })

	for _, tc := range []struct {
		name string
		hold bool
	}{
		{name: "idle"},
		{name: "hold self and another entity", hold: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := startTestDispatcher(t, testDeps())
			var trace mailboxTestTrace
			var holdA, holdB Task
			if tc.hold {
				holdA = func(_ context.Context, sh *Shard) { sh.Hold(a) }
				holdB = func(_ context.Context, sh *Shard) { sh.Hold(b) }
			}
			runMailboxTestTask(t, d, func(sh *Shard) {
				sh.DeferInternal(a, trace.item("A1", holdA))
				sh.DeferInternal(b, trace.item("B1", nil))
				sh.DeferInternal(c, trace.item("C1", holdB))
				for i, event := range tail {
					item := trace.item(event.name, nil)
					if i%2 == 0 {
						if !sh.Defer(event.key, item) {
							t.Errorf("Defer rejected %s with room in the mailbox", event.name)
						}
					} else {
						sh.DeferInternal(event.key, item)
					}
				}
			})

			want := []string{"A1", "B1", "C1"}
			for _, event := range tail {
				if !tc.hold || event.key == c {
					want = append(want, event.name)
				}
			}
			trace.check(t, want...)
			if tc.hold {
				for _, key := range []Key{b, a} {
					runMailboxTestTask(t, d, func(sh *Shard) { sh.Release(key) })
					for _, event := range tail {
						if event.key == key {
							want = append(want, event.name)
						}
					}
					trace.check(t, want...)
				}
			}
		})
	}
}
