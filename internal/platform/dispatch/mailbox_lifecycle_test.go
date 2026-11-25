package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestMailboxRequeueOrderAndCapacity(t *testing.T) {
	const capacity = 16
	for _, tc := range []struct {
		name       string
		before     int
		defaultNow bool
	}{
		{name: "held with full mailbox", before: capacity},
		{name: "idle with external capacity available"},
		{name: "default clock", defaultNow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				deps := testDeps()
				deps.MailboxPerEntity = capacity
				clock := newMailboxLifecycleClock(&deps)
				tick := clock.tick
				if tc.defaultNow {
					deps.Now = nil
					tick = func(advance time.Duration) {
						time.Sleep(advance)
						clock.tick(0)
					}
				}
				d := startTestDispatcher(t, deps)
				key := Key{A: 17}
				var order []int
				item := func(id int) Item {
					return mailboxLifecycleItem(t, func(context.Context, *Shard) { order = append(order, id) })
				}
				external := func(sh *Shard, id int) {
					if !sh.Defer(key, item(id)) {
						t.Errorf("Defer rejected external item %d", id)
					}
				}

				submitTicketTestTask(t, d, func(_ context.Context, sh *Shard) {
					if tc.before > 0 {
						sh.Hold(key)
						for i := 1; i <= tc.before; i++ {
							external(sh, i)
						}
					}
					sh.Requeue(key, item(0), 0)
					if !sh.Busy(key) {
						t.Error("Requeue left the entity idle")
					}
					for i := tc.before + 1; i <= capacity; i++ {
						external(sh, i)
					}
					sh.DeferInternal(key, item(capacity+1))
					if sh.Defer(key, item(capacity+2)) {
						t.Error("Defer accepted an external item beyond capacity")
					}
				})
				synctest.Wait()
				if len(order) != 0 {
					t.Fatalf("ran before a tick: %v", order)
				}
				tick(100*time.Millisecond - time.Nanosecond)
				if len(order) != 0 {
					t.Fatalf("ran before backoff elapsed: %v", order)
				}
				tick(time.Nanosecond)
				want := make([]int, capacity+2)
				for i := range want {
					want[i] = i
				}
				if !slices.Equal(order, want) {
					t.Errorf("execution order = %v, want %v", order, want)
				}
			})
		})
	}
}

func TestMailboxRequeueAllowsRetryAfterDequeuing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := testDeps()
		clock := newMailboxLifecycleClock(&deps)
		d := startTestDispatcher(t, deps)
		key := Key{A: 19}
		var order []string
		item := func(name string) Item {
			return mailboxLifecycleItem(t, func(context.Context, *Shard) { order = append(order, name) })
		}
		submitTicketTestTask(t, d, func(_ context.Context, sh *Shard) {
			sh.Requeue(key, mailboxLifecycleItem(t, func(_ context.Context, sh *Shard) {
				order = append(order, "first replay")
				sh.Requeue(key, item("second replay"), 1)
			}), 0)
			sh.DeferInternal(key, item("later event"))
			requireRequeuePanic(t, key, func() { sh.Requeue(key, item("duplicate during backoff"), 0) })
			sh.Release(key)
			requireRequeuePanic(t, key, func() { sh.Requeue(key, item("duplicate before dequeue"), 0) })
		})
		synctest.Wait()
		if !slices.Equal(order, []string{"first replay"}) {
			t.Fatalf("after manual Release ran %v, want [first replay]", order)
		}
		clock.tick(100 * time.Millisecond)
		if !slices.Equal(order, []string{"first replay"}) {
			t.Fatalf("at original deadline ran %v, want [first replay]", order)
		}
		clock.tick(100 * time.Millisecond)
		want := []string{"first replay", "second replay", "later event"}
		if !slices.Equal(order, want) {
			t.Errorf("execution order = %v, want %v", order, want)
		}
	})
}

func TestMailboxReleaseCancelsReplayDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := testDeps()
		clock := newMailboxLifecycleClock(&deps)
		d := startTestDispatcher(t, deps)
		key := Key{A: 21}
		var trace mailboxTestTrace
		submitTicketTestTask(t, d, func(_ context.Context, sh *Shard) {
			sh.Requeue(key, trace.item("replay", func(_ context.Context, sh *Shard) {
				sh.Hold(key)
				sh.DeferInternal(key, trace.item("new hold", nil))
			}), 0)
			sh.Release(key)
		})
		synctest.Wait()
		trace.check(t, "replay")
		clock.tick(100 * time.Millisecond)
		trace.check(t, "replay")
		submitTicketTestTask(t, d, func(_ context.Context, sh *Shard) { sh.Release(key) })
		synctest.Wait()
		trace.check(t, "replay", "new hold")
	})
}

func TestMailboxDropCancelsRequeueDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := testDeps()
		clock := newMailboxLifecycleClock(&deps)
		d := startTestDispatcher(t, deps)
		key := Key{A: 23}
		reason := errors.New("entity deleted")
		var aborted error
		var freshRan bool
		submitTicketTestTask(t, d, func(_ context.Context, sh *Shard) {
			sh.Requeue(key, Item{
				Run:   func(context.Context, *Shard) { t.Error("dropped replay ran") },
				Abort: func(err error) { aborted = err },
			}, 0)
			sh.Drop(key, reason)
			sh.Hold(key)
			sh.DeferInternal(key, mailboxLifecycleItem(t, func(context.Context, *Shard) { freshRan = true }))
		})
		synctest.Wait()
		if aborted != reason {
			t.Errorf("dropped replay received %v, want %v", aborted, reason)
		}
		clock.tick(time.Second)
		if freshRan {
			t.Error("dropped replay deadline released a new hold on the same key")
		}
		submitTicketTestTask(t, d, func(_ context.Context, sh *Shard) { sh.Release(key) })
		synctest.Wait()
		if !freshRan {
			t.Error("fresh item did not run after Release")
		}
	})
}

func TestMailboxCommitCallback(t *testing.T) {
	for _, tc := range []struct {
		name    string
		requeue bool
		stop    bool
	}{
		{name: "release"},
		{name: "requeue", requeue: true},
		{name: "release during stop", stop: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				deps := testDeps()
				clock := newMailboxLifecycleClock(&deps)
				d := startTestDispatcher(t, deps)
				key := Key{A: 29}
				var order []string
				item := func(name string) Item {
					return mailboxLifecycleItem(t, func(context.Context, *Shard) { order = append(order, name) })
				}
				ticket := requireTicket(t, d)
				submitTicketTestTask(t, d, func(_ context.Context, sh *Shard) {
					sh.Hold(key)
					sh.DeferInternal(key, item("first pending"))
					sh.DeferInternal(key, item("second pending"))
				})
				synctest.Wait()

				stopped := make(chan error, 1)
				if tc.stop {
					ctx, cancel := context.WithTimeout(t.Context(), time.Second)
					defer cancel()
					go func() { stopped <- d.Stop(ctx) }()
					await(t, d.closing)
					synctest.Wait()
					select {
					case err := <-stopped:
						t.Fatalf("Stop returned before commit: %v", err)
					default:
					}
				}
				if len(order) != 0 {
					t.Fatalf("before commit ran %v, want no events", order)
				}

				ticket.Complete(func(_ context.Context, sh *Shard) {
					if !sh.Busy(key) {
						t.Error("commit callback found the entity idle")
					}
					order = append(order, "commit")
					if tc.requeue {
						sh.Requeue(key, item("replay"), 0)
					} else {
						sh.Release(key)
					}
				})
				synctest.Wait()
				if tc.stop {
					if err := await(t, stopped); err != nil {
						t.Fatalf("Stop after commit: %v", err)
					}
				}
				want := []string{"commit"}
				if tc.requeue {
					if !slices.Equal(order, want) {
						t.Fatalf("before replay deadline ran %v, want %v", order, want)
					}
					clock.tick(100 * time.Millisecond)
					want = append(want, "replay")
				}
				want = append(want, "first pending", "second pending")
				if !slices.Equal(order, want) {
					t.Errorf("execution order = %v, want %v", order, want)
				}
			})
		})
	}
}

func TestMailboxRequeueBackoff(t *testing.T) {
	for _, tc := range []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: 100 * time.Millisecond},
		{attempt: 1, want: 200 * time.Millisecond},
		{attempt: 5, want: 3200 * time.Millisecond},
		{attempt: 6, want: 5 * time.Second},
		{attempt: math.MaxInt, want: 5 * time.Second},
		{attempt: -1, want: 100 * time.Millisecond},
		{attempt: math.MinInt, want: 100 * time.Millisecond},
	} {
		if got := requeueBackoff(tc.attempt); got != tc.want {
			t.Errorf("backoff at attempt %d = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

func TestMailboxStopAbortsPendingItems(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deadline bool
	}{
		{name: "after last completion"},
		{name: "deadline with ticket outstanding", deadline: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				deps := testDeps()
				clock := newMailboxLifecycleClock(&deps)
				logs := &mailboxPanicLog{Handler: slog.DiscardHandler}
				deps.Log = slog.New(logs)
				d, err := New(deps)
				if err != nil {
					t.Fatal(err)
				}
				ticked := make(chan struct{}, 1)
				d.OnTick(func(context.Context, *Shard) { ticked <- struct{}{} })
				if err := d.Start(t.Context()); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					if err := d.Stop(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
						t.Error(err)
					}
				})

				type outcome struct {
					name   string
					reason error
				}
				var orderMu sync.Mutex
				var order []outcome
				record := func(result outcome) {
					orderMu.Lock()
					defer orderMu.Unlock()
					order = append(order, result)
				}
				item := func(name string) Item {
					return Item{
						Run:   func(context.Context, *Shard) { record(outcome{name, nil}) },
						Abort: func(err error) { record(outcome{name, err}) },
					}
				}
				key := Key{A: 31}
				ticket := requireTicket(t, d)
				submitTicketTestTask(t, d, func(_ context.Context, sh *Shard) {
					sh.DeferInternal(key, item("finished item"))
				})
				synctest.Wait()
				submitTicketTestTask(t, d, func(_ context.Context, sh *Shard) {
					sh.Hold(key)
					if !sh.Defer(key, item("external")) {
						t.Error("Defer rejected an external item in an empty mailbox")
					}
					replay := item("replay")
					abort := replay.Abort
					replay.Abort = func(err error) {
						abort(err)
						if admission, ok := sh.Admit(); ok {
							admission.Cancel()
							t.Error("Admit succeeded during mailbox exit cleanup")
						}
						if !sh.Defer(key, item("external from Abort")) {
							t.Error("Defer rejected an external item in an empty mailbox")
						}
						sh.DeferInternal(key, item("internal from Abort"))
						if tc.deadline {
							panic("replay abort failed")
						}
					}
					sh.Requeue(key, replay, 0)
					sh.DeferInternal(key, item("internal"))
				})
				synctest.Wait()

				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				stopped := make(chan error, 1)
				go func() { stopped <- d.Stop(ctx) }()
				await(t, d.closing)
				clock.tick(100 * time.Millisecond)
				await(t, ticked)
				want := []outcome{{"finished item", nil}}
				orderMu.Lock()
				beforeExit := slices.Clone(order)
				orderMu.Unlock()
				if !slices.Equal(beforeExit, want) {
					t.Errorf("outcomes while waiting on ticket = %v, want %v", beforeExit, want)
				}
				if !tc.deadline {
					ticket.Complete(func(context.Context, *Shard) { record(outcome{"completion", nil}) })
					want = append(want, outcome{"completion", nil})
				}
				var wantErr error
				if tc.deadline {
					wantErr = context.DeadlineExceeded
				}
				if err := await(t, stopped); !errors.Is(err, wantErr) {
					t.Errorf("Stop = %v, want %v", err, wantErr)
				}
				want = append(want, outcome{"replay", ErrClosed}, outcome{"external", ErrClosed}, outcome{"internal", ErrClosed},
					outcome{"external from Abort", ErrClosed}, outcome{"internal from Abort", ErrClosed})
				if !slices.Equal(order, want) {
					t.Errorf("outcomes at Stop = %v, want %v", order, want)
				}
				if tc.deadline && !slices.Equal(logs.contextErrors, []error{context.Canceled}) {
					t.Errorf("Abort panic log context errors = %v, want [%v]", logs.contextErrors, context.Canceled)
				}
			})
		})
	}
}

func TestMailboxStopDrainsAcceptedInbox(t *testing.T) {
	for _, tc := range []struct {
		name        string
		blockOnTick bool
	}{
		{name: "before the inbox item is prefetched", blockOnTick: true},
		{name: "while the inbox item is executing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				deps := testDeps()
				clock := newMailboxLifecycleClock(&deps)
				d, err := New(deps)
				if err != nil {
					t.Fatal(err)
				}
				var trace mailboxTestTrace
				key := Key{A: 1}
				entered, gate := make(chan struct{}), make(chan struct{})
				unblock := sync.OnceFunc(func() { close(gate) })
				defer unblock()
				d.OnTick(func(_ context.Context, sh *Shard) {
					if tc.blockOnTick {
						close(entered)
						<-gate
					}
					sh.DeferInternal(key, trace.item("tick", nil))
				})
				if err := d.Start(t.Context()); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if tc.blockOnTick {
					clock.ticks <- time.Time{}
					await(t, entered)
				}
				submitTicketTestTask(t, d, func(_ context.Context, sh *Shard) {
					if !tc.blockOnTick {
						close(entered)
						<-gate
					}
					sh.DeferInternal(key, trace.item("accepted", nil))
				})
				if !tc.blockOnTick {
					await(t, entered)
					clock.ticks <- time.Time{}
				}
				stopped := make(chan error, 1)
				go func() { stopped <- d.Stop(ctx) }()
				await(t, d.closing)
				unblock()
				if err := await(t, stopped); err != nil {
					t.Fatal(err)
				}
				if tc.blockOnTick {
					trace.check(t, "tick", "accepted")
				} else {
					trace.check(t, "accepted")
				}
			})
		})
	}
}

func TestMailboxCanceledDrainRunsAcceptedItems(t *testing.T) {
	for _, prefetched := range []bool{false, true} {
		name := "queued"
		if prefetched {
			name = "prefetched and queued"
		}
		t.Run(name, func(t *testing.T) {
			d, err := New(testDeps())
			if err != nil {
				t.Fatal(err)
			}
			sh := d.shards[0]
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			var trace mailboxTestTrace
			key := Key{A: 1}
			task := func(name string) work {
				return work{key: key, task: func(_ context.Context, sh *Shard) {
					sh.DeferInternal(key, trace.item(name, nil))
				}}
			}
			sh.inbox <- task("queued")
			sh.drainCanceled(ctx, prefetched, task("prefetched"))
			if prefetched {
				trace.check(t, "prefetched", "queued")
			} else {
				trace.check(t, "queued")
			}
		})
	}
}

func mailboxLifecycleItem(t *testing.T, run Task) Item {
	t.Helper()
	return Item{Run: run, Abort: func(err error) { t.Errorf("unexpected mailbox abort: %v", err) }}
}

func requireRequeuePanic(t *testing.T, key Key, call func()) {
	t.Helper()
	defer func() {
		if got := recover(); !strings.Contains(fmt.Sprint(got), fmt.Sprint(key)) {
			t.Errorf("Requeue panic = %v, want the entity key %v", got, key)
		}
	}()
	call()
}

type mailboxPanicLog struct {
	slog.Handler
	contextErrors []error
}

func (*mailboxPanicLog) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelError
}

func (log *mailboxPanicLog) Handle(ctx context.Context, _ slog.Record) error {
	log.contextErrors = append(log.contextErrors, ctx.Err())
	return nil
}

type mailboxLifecycleClock struct {
	now   time.Time
	ticks chan time.Time
}

func newMailboxLifecycleClock(deps *Deps) *mailboxLifecycleClock {
	clock := &mailboxLifecycleClock{now: time.Unix(1234, 0), ticks: make(chan time.Time, 1)}
	deps.Now = func() time.Time { return clock.now }
	deps.NewTicker = func(int, time.Duration) (<-chan time.Time, func()) {
		return clock.ticks, func() {}
	}
	return clock
}

func (clock *mailboxLifecycleClock) tick(advance time.Duration) {
	clock.now = clock.now.Add(advance)
	clock.ticks <- time.Time{}
	synctest.Wait()
}
