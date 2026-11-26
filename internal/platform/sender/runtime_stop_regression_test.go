package sender

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"weak"

	"github.com/go-telegram/bot"

	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

func TestRuntimeStopHonorsBackground429(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := workerRuntimeDeps(t)
		deps.GlobalRPS, deps.GlobalBurst = 25, 5
		s, err := New(deps)
		if err != nil {
			t.Fatal(err)
		}
		ready, permit := make(chan struct{}), make(chan struct{})
		resume := sync.OnceFunc(func() { close(permit) })
		var held atomic.Bool
		s.beforeAttemptResultLock = func() {
			if held.CompareAndSwap(false, true) {
				close(ready)
				<-permit
			}
		}
		if err := s.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer workerRuntimeStop(t, s)
		defer resume()

		background, backgroundDone := gateRuntimeRequest(Background, 1, "getChat", time.Time{})
		background.Do = func(context.Context, *bot.Bot) error {
			return &bot.TooManyRequestsError{RetryAfter: 1}
		}
		gateRuntimeEnqueue(t, s, background)
		workerRuntimeReceive(t, ready)

		// Exhaust the burst so the queued Decision cannot race the 429 result for a token.
		for range deps.GlobalBurst - 1 {
			fill, done := gateRuntimeRequest(Decision, 2, "getChat", time.Time{})
			gateRuntimeEnqueue(t, s, fill)
			synctest.Wait()
			if got := gateRuntimeResult(t, done); got.Outcome.Class != telegram.OK {
				t.Fatalf("burst request = %+v, want OK", got)
			}
		}
		started := make(chan time.Time, 1)
		decision, decisionDone := gateRuntimeRequest(Decision, background.ChatID, "approveChatJoinRequest", time.Time{})
		decision.Do = func(context.Context, *bot.Bot) error {
			started <- time.Now()
			return nil
		}
		gateRuntimeEnqueue(t, s, decision)
		stopped := make(chan error, 1)
		go func() { stopped <- s.Stop(context.Background()) }()
		synctest.Wait()
		if err := s.Enqueue(decision); !errors.Is(err, ErrClosed) {
			t.Fatalf("Enqueue during Stop = %v, want ErrClosed", err)
		}

		pausedAt := time.Now()
		resume()
		synctest.Wait()
		workerRuntimeShutdown(t, gateRuntimeResult(t, backgroundDone), 1, false)
		if sentAt := workerRuntimeReceive(t, started); sentAt.Sub(pausedAt) != time.Second {
			t.Errorf("Decision sent after %v, want 1s", sentAt.Sub(pausedAt))
		}
		if got := workerRuntimeReceive(t, decisionDone); got.Outcome.Class != telegram.OK || got.Dropped != 0 || got.Attempts != 1 {
			t.Errorf("Decision result = %+v, want one successful attempt", got)
		}
		if err := workerRuntimeReceive(t, stopped); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
}

func TestRuntimeStopKeepsRetryOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := workerRuntimeDeps(t)
		workerRuntimeSingle(&deps)
		deps.MaxRetries = 1
		s, err := New(deps)
		if err != nil {
			t.Fatal(err)
		}
		samples := []time.Duration{0, 50 * time.Millisecond, 10 * time.Millisecond}
		s.retryRandom = func(int64) int64 {
			if len(samples) == 0 {
				t.Error("unexpected retry")
				return 0
			}
			sample := samples[0]
			samples = samples[1:]
			return int64(sample)
		}
		if err := s.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		began := time.Now()
		var resources []weak.Pointer[stopRuntimeProbeContext]
		for index, tc := range []struct {
			class   Class
			retryIn time.Duration
		}{
			{Background, 0},
			{Decision, 150 * time.Millisecond},
			{Decision, 110 * time.Millisecond},
		} {
			attempted := false
			resource := makeStopRuntimeProbe()
			resources = append(resources, weak.Make(resource))
			workerRuntimeEnqueue(t, s, workerRuntimeRequest(tc.class, int64(index+1), func(context.Context, *bot.Bot) error {
				if !attempted {
					attempted = true
					return context.DeadlineExceeded
				}
				if elapsed := time.Since(began); elapsed != tc.retryIn {
					t.Errorf("%s request %d retried at %v, want %v", tc.class, index, elapsed, tc.retryIn)
				}
				return nil
			}, func(result Result) {
				if tc.class == Background {
					if result.Dropped != Shutdown || result.Attempts != 1 || !time.Now().Equal(began) {
						t.Errorf("background request at %v = %+v, want immediate Shutdown", time.Since(began), result)
					}
				} else if result.Outcome.Class != telegram.OK || result.Attempts != 2 || result.Dropped != 0 {
					t.Errorf("decision request %d = %+v, want success after a retry", index, result)
				}
				runtime.KeepAlive(resource)
			}))
			synctest.Wait()
		}
		workerRuntimeStop(t, s)
		if elapsed := time.Since(began); elapsed != 150*time.Millisecond {
			t.Errorf("Stop finished at %v, want 150 ms", elapsed)
		}
		synctest.Wait()
		runtime.GC()
		for i, resource := range resources {
			if resource.Value() != nil {
				t.Errorf("stopped sender retained request %d after delivering its result", i)
			}
		}
		runtime.KeepAlive(s)
	})
}

type stopRuntimeDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c stopRuntimeDeadlineContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func TestRuntimeStopDeadlineBeforeCancellationNotice(t *testing.T) {
	for _, delay := range []time.Duration{0, time.Nanosecond} {
		t.Run(delay.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := workerRuntimeStart(t, workerRuntimeDeps(t))
				started := make(chan struct{})
				done := make(chan Result, 1)
				workerRuntimeEnqueue(t, s, workerRuntimeRequest(Decision, 1, func(ctx context.Context, _ *bot.Bot) error {
					close(started)
					<-ctx.Done()
					time.Sleep(delay)
					return ctx.Err()
				}, func(result Result) { done <- result }))
				workerRuntimeReceive(t, started)
				began := time.Now()
				const timeout = time.Second
				deadline := began.Add(timeout)
				// Keep the notification timer separate from the deadline seen by Stop.
				parent, cancel := context.WithDeadline(t.Context(), deadline.Add(2*time.Nanosecond))
				defer cancel()
				ctx := stopRuntimeDeadlineContext{Context: parent, deadline: deadline}
				if err := s.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("Stop = %v, want DeadlineExceeded", err)
				}
				if elapsed := time.Since(began); elapsed != timeout+delay {
					t.Errorf("Stop returned after %v, want %v", elapsed, timeout+delay)
				}
				workerRuntimeShutdown(t, workerRuntimeReceive(t, done), 1, true)
			})
		})
	}
}

type stopRuntimeDelayedDoneContext struct {
	context.Context
	enters chan struct{}
	resume chan struct{}
}

func (c stopRuntimeDelayedDoneContext) Done() <-chan struct{} {
	close(c.enters)
	<-c.resume
	return c.Context.Done()
}

func TestRuntimeStopCancellationAtAttemptBoundary(t *testing.T) {
	const timeout = time.Second
	for _, tc := range []struct {
		name     string
		delay    time.Duration
		inFlight bool
	}{
		{name: "before Do"},
		{name: "cancel retained after deadline", delay: timeout},
		{name: "after Do starts", inFlight: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				deps := workerRuntimeDeps(t)
				workerRuntimeSingle(&deps)
				s, err := New(deps)
				if err != nil {
					t.Fatal(err)
				}
				paused, release := make(chan struct{}), make(chan struct{})
				if !tc.inFlight {
					s.beforeAttempt = func(*item) {
						close(paused)
						<-release
					}
				}
				if err := s.Start(t.Context()); err != nil {
					t.Fatal(err)
				}
				done := make(chan Result, 1)
				workerRuntimeEnqueue(t, s, workerRuntimeRequest(Decision, 1, func(context.Context, *bot.Bot) error {
					if tc.inFlight {
						close(paused)
						<-release
					} else {
						t.Error("selected request reached Do after Stop cancellation")
					}
					return nil
				}, func(result Result) { done <- result }))
				workerRuntimeReceive(t, paused)

				parent, cancel := context.WithTimeout(t.Context(), timeout)
				defer cancel()
				ctx := stopRuntimeDelayedDoneContext{Context: parent, enters: make(chan struct{}), resume: make(chan struct{})}
				stopped := make(chan error, 1)
				go func() { stopped <- s.Stop(ctx) }()
				workerRuntimeReceive(t, ctx.enters)
				synctest.Wait()
				cancel()
				time.Sleep(tc.delay)
				close(release)
				synctest.Wait()
				close(ctx.resume)
				if err := workerRuntimeReceive(t, stopped); !errors.Is(err, context.Canceled) {
					t.Errorf("Stop = %v, want context canceled", err)
				}
				attempts := 0
				if tc.inFlight {
					attempts = 1
				}
				workerRuntimeShutdown(t, workerRuntimeReceive(t, done), attempts, false)
			})
		})
	}
}

type stopRuntimeSnapshotContext struct {
	context.Context
	registered bool
	snapshot   chan struct{}
	resume     chan struct{}
}

func (c *stopRuntimeSnapshotContext) Deadline() (time.Time, bool) {
	if c.registered {
		close(c.snapshot)
		<-c.resume
	}
	c.registered = true
	return time.Now().Add(-time.Second), false
}

func TestRuntimeStopKeepsDecidedResultAfterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := workerRuntimeStart(t, workerRuntimeDeps(t))
		started, release := make(chan struct{}), make(chan struct{})
		done := make(chan Result, 1)
		workerRuntimeEnqueue(t, s, workerRuntimeRequest(Decision, 1, func(context.Context, *bot.Bot) error {
			close(started)
			<-release
			return nil
		}, func(result Result) { done <- result }))
		workerRuntimeReceive(t, started)

		parent, cancel := context.WithCancel(t.Context())
		defer cancel()
		ctx := &stopRuntimeSnapshotContext{Context: parent, snapshot: make(chan struct{}), resume: make(chan struct{})}
		stopped := make(chan error, 1)
		go func() { stopped <- s.Stop(ctx) }()
		synctest.Wait()
		close(release)
		workerRuntimeReceive(t, ctx.snapshot)
		cancel()
		close(ctx.resume)
		if err := workerRuntimeReceive(t, stopped); err != nil {
			t.Errorf("Stop = %v, want the successful result chosen before cancellation", err)
		}
		if result := workerRuntimeReceive(t, done); result.Dropped != 0 || result.Outcome.Class != telegram.OK {
			t.Errorf("drained result = %+v, want success without a stop deadline", result)
		}
	})
}

func TestRuntimeStopReleasesContexts(t *testing.T) {
	t.Run("start", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			s, err := New(workerRuntimeDeps(t))
			if err != nil {
				t.Fatal(err)
			}
			startContext := makeStopRuntimeProbe()
			startedWith := weak.Make(startContext)
			if err := s.Start(startContext); err != nil {
				t.Fatal(err)
			}
			workerRuntimeStop(t, s)
			repeatedContext := makeStopRuntimeProbe()
			repeatedWith := weak.Make(repeatedContext)
			if err := s.Stop(repeatedContext); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			runtime.GC()
			if startedWith.Value() != nil {
				t.Error("stopped sender retained its Start context")
			}
			if repeatedWith.Value() != nil {
				t.Error("repeating Stop retained its context after the sender finished")
			}
			runtime.KeepAlive(s)
		})
	})
	synctest.Test(t, func(t *testing.T) {
		deps := workerRuntimeDeps(t)
		workerRuntimeSingle(&deps)
		s := workerRuntimeStart(t, deps)
		started, release := make(chan struct{}), make(chan struct{})
		workerCanceled := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer workerRuntimeStop(t, s)
		defer unblock()
		workerRuntimeEnqueue(t, s, workerRuntimeRequest(Decision, 1, func(ctx context.Context, _ *bot.Bot) error {
			close(started)
			<-ctx.Done()
			close(workerCanceled)
			<-release
			return ctx.Err()
		}, func(Result) {}))
		workerRuntimeReceive(t, started)
		queued, queuedDone := gateRuntimeRequest(Decision, 2, "getChat", time.Time{})
		queued.Do = func(context.Context, *bot.Bot) error {
			t.Error("queued request reached Do after Stop cancellation")
			return nil
		}
		workerRuntimeEnqueue(t, s, queued)

		canceledContext := makeStopRuntimeProbe()
		canceledWith := weak.Make(canceledContext)
		canceled, cancel := context.WithCancel(canceledContext)
		canceledStop := make(chan error, 1)
		go func() { canceledStop <- s.Stop(canceled) }()
		synctest.Wait()
		cancel()
		if err := workerRuntimeReceive(t, canceledStop); !errors.Is(err, context.Canceled) {
			t.Fatalf("Stop with canceled context = %v, want Canceled", err)
		}
		synctest.Wait()
		select {
		case <-workerCanceled:
		default:
			t.Error("Stop returned without canceling the in-flight request")
		}
		workerRuntimeShutdown(t, gateRuntimeResult(t, queuedDone), 0, false)
		runtime.GC()
		if canceledWith.Value() != nil {
			t.Error("canceled Stop retained its context while a worker was still running")
		}

		finishContext := makeStopRuntimeProbe()
		finishedWith := weak.Make(finishContext)
		finished := make(chan error, 1)
		go func() { finished <- s.Stop(finishContext) }()
		synctest.Wait()
		unblock()
		if err := workerRuntimeReceive(t, finished); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		runtime.GC()
		if finishedWith.Value() != nil {
			t.Error("completed Stop retained its context")
		}
		runtime.KeepAlive(s)
	})
}

type stopRuntimeProbeContext struct{ context.Context }

func makeStopRuntimeProbe() *stopRuntimeProbeContext {
	return &stopRuntimeProbeContext{Context: context.Background()}
}
