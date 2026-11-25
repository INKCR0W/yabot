package scheduler_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/scheduler"
	"github.com/VictoriaMetrics/metrics"
)

func TestSchedulerDeadlineOrder(t *testing.T) {
	for _, tc := range []struct {
		name      string
		deadlines []int
		afterOne  []int
		want      []int
	}{
		{"out of order", []int{3, 1, 2}, []int{1}, []int{1, 2, 0}},
		{"same deadline and key", []int{1, 1, 1, 1}, []int{0, 1, 2, 3}, []int{0, 1, 2, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Unix(0, 0)
				clock := scheduler.NewFakeClock(start)
				s := newScheduler(t, scheduler.Deps{Dispatcher: submitFunc(runTask), Clock: clock})
				var submitted []int
				for id, seconds := range tc.deadlines {
					s.At(start.Add(time.Duration(seconds)*time.Second), dispatch.Key{}, func(context.Context, *dispatch.Shard) {
						submitted = append(submitted, id)
					})
				}
				checkOrder(t, submitted, nil)
				defer startScheduler(t, s)()
				checkArmed(t, clock, start.Add(time.Second))
				synctest.Wait()
				checkOrder(t, submitted, nil)
				clock.Advance(time.Second)
				synctest.Wait()
				checkOrder(t, submitted, tc.afterOne)
				clock.Advance(2 * time.Second)
				synctest.Wait()
				checkOrder(t, submitted, tc.want)
			})
		})
	}
}

func TestSchedulerEarlierDeadlineRearmsTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(0, 0)
		clock := scheduler.NewFakeClock(start)
		var submitted []int
		s := newScheduler(t, scheduler.Deps{
			Clock: clock,
			Dispatcher: submitFunc(func(k dispatch.Key, _ dispatch.Task) error {
				submitted = append(submitted, int(k.A))
				return nil
			}),
		})
		defer startScheduler(t, s)()
		s.At(start.Add(2*time.Second), dispatch.Key{A: 2}, noopTask)
		checkArmed(t, clock, start.Add(2*time.Second))
		s.At(start.Add(time.Second), dispatch.Key{A: 1}, noopTask)
		checkArmed(t, clock, start.Add(time.Second))
		// The single timer moved, so nothing still waits on the later deadline.
		probe, cancel := context.WithCancel(t.Context())
		cancel()
		if err := clock.WaitForTimer(probe, start.Add(2*time.Second)); !errors.Is(err, context.Canceled) {
			t.Fatalf("timer at the later deadline is still armed: %v", err)
		}
		clock.Advance(time.Second)
		synctest.Wait()
		checkOrder(t, submitted, []int{1})
		checkArmed(t, clock, start.Add(2*time.Second))
		clock.Advance(time.Second)
		synctest.Wait()
		checkOrder(t, submitted, []int{1, 2})
	})
}

func TestSchedulerRetries(t *testing.T) {
	const (
		busyMetric   = "yabot_scheduler_busy_retries_total"
		errorsMetric = "yabot_scheduler_submit_errors_total"
		retryDelay   = 50 * time.Millisecond
	)
	for _, tc := range []struct {
		name     string
		failures []error
		metric   string
		logs     int
		infoLogs int
	}{
		{"busy", []error{dispatch.ErrBusy, fmt.Errorf("full: %w", dispatch.ErrBusy)}, busyMetric, 0, 2},
		{"other errors", []error{dispatch.ErrClosed, errors.New("unexpected failure")}, errorsMetric, 2, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Unix(0, 0)
				clock := scheduler.NewFakeClock(start)
				set := metrics.NewSet()
				var log bytes.Buffer
				var attempts []time.Time
				var successful []int
				const taskCount = 2
				s := newScheduler(t, scheduler.Deps{
					Clock:   clock,
					Metrics: set,
					Log:     slog.New(slog.NewJSONHandler(&log, nil)),
					Dispatcher: submitFunc(func(k dispatch.Key, task dispatch.Task) error {
						round := len(attempts) / taskCount
						attempts = append(attempts, clock.Now())
						if round < len(tc.failures) {
							return tc.failures[round]
						}
						return runTask(k, task)
					}),
				})
				for id := range taskCount {
					s.At(start, dispatch.Key{}, func(context.Context, *dispatch.Shard) {
						successful = append(successful, id)
					})
				}
				defer startScheduler(t, s)()
				for round := range len(tc.failures) + 1 {
					synctest.Wait()
					wantAttempts := (round + 1) * taskCount
					if got := len(attempts); got != wantAttempts {
						t.Fatalf("round %d: %d submit attempts, want %d", round, got, wantAttempts)
					}
					for _, when := range attempts[round*taskCount:] {
						if want := start.Add(time.Duration(round) * retryDelay); !when.Equal(want) {
							t.Fatalf("round %d: submitted at %v, want %v", round, when, want)
						}
					}
					checkDropped(t, set, 0)
					if round < len(tc.failures) {
						checkOrder(t, successful, nil)
						checkPending(t, set, taskCount)
						checkArmed(t, clock, clock.Now().Add(retryDelay))
						clock.Advance(retryDelay - time.Nanosecond)
						synctest.Wait()
						if got := len(attempts); got != wantAttempts {
							t.Fatalf("retried before deadline: %d attempts, want %d", got, wantAttempts)
						}
						clock.Advance(time.Nanosecond)
					}
				}
				clock.Advance(retryDelay)
				synctest.Wait()
				checkOrder(t, successful, []int{0, 1})
				checkPending(t, set, 0)
				checkDropped(t, set, 0)
				for _, metric := range []string{busyMetric, errorsMetric} {
					var want uint64
					if metric == tc.metric {
						want = uint64(len(tc.failures) * taskCount)
					}
					if got := set.GetOrCreateCounter(metric).Get(); got != want {
						t.Errorf("%s = %d, want %d", metric, got, want)
					}
				}
				errorLogs, infoLogs := 0, 0
				for line := range strings.SplitSeq(log.String(), "\n") {
					if strings.Contains(line, `"level":"INFO"`) {
						infoLogs++
					}
					if !strings.Contains(line, `"level":"ERROR"`) {
						continue
					}
					errorLogs++
					if !strings.Contains(line, dispatch.ErrClosed.Error()) {
						t.Errorf("want an Error log for the first failure of a task, got %s", line)
					}
				}
				if errorLogs != tc.logs {
					t.Fatalf("got %d Error logs, want %d: %s", errorLogs, tc.logs, log.String())
				}
				// One Info per task on its first busy retry.
				if infoLogs != tc.infoLogs {
					t.Fatalf("got %d Info logs, want %d: %s", infoLogs, tc.infoLogs, log.String())
				}
			})
		})
	}
}

func TestSchedulerRetryPreservesInsertionOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(0, 0)
		clock := scheduler.NewFakeClock(start)
		var submitted []int
		wasBusy := false
		s := newScheduler(t, scheduler.Deps{
			Clock: clock,
			Dispatcher: submitFunc(func(k dispatch.Key, _ dispatch.Task) error {
				if k.A == 1 && !wasBusy {
					wasBusy = true
					return dispatch.ErrBusy
				}
				submitted = append(submitted, int(k.A))
				return nil
			}),
		})
		deadline := start.Add(scheduler.RetryDelay)
		s.At(start, dispatch.Key{A: 1}, noopTask)
		s.At(deadline, dispatch.Key{A: 2}, noopTask)
		defer startScheduler(t, s)()
		checkArmed(t, clock, deadline)
		clock.Advance(scheduler.RetryDelay)
		synctest.Wait()
		checkOrder(t, submitted, []int{1, 2})
	})
}

func TestSchedulerLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(0, 0)
		clock := scheduler.NewFakeClock(start)
		set := metrics.NewSet()
		var submitted []int
		s := newScheduler(t, scheduler.Deps{
			Clock: clock, Metrics: set,
			Dispatcher: submitFunc(func(k dispatch.Key, _ dispatch.Task) error {
				submitted = append(submitted, int(k.A))
				return nil
			}),
		})
		s.At(start.Add(-time.Second), dispatch.Key{A: 1}, noopTask)
		stopScheduler(t, s)
		checkPending(t, set, 1)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := s.Start(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Start with canceled context = %v, want context.Canceled", err)
		}
		ctx, cancel = context.WithCancel(t.Context())
		if err := s.Start(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
		defer stopScheduler(t, s)
		if err := s.Start(t.Context()); err == nil {
			t.Fatal("second Start succeeded")
		}
		synctest.Wait()
		checkOrder(t, submitted, []int{1})

		s.At(start.Add(time.Second), dispatch.Key{A: 2}, noopTask)
		s.At(start.Add(2*time.Second), dispatch.Key{A: 3}, noopTask)
		checkArmed(t, clock, start.Add(time.Second))
		clock.Advance(time.Second)
		synctest.Wait()
		checkOrder(t, submitted, []int{1, 2})
		checkPending(t, set, 1)
		stopScheduler(t, s)
		checkPending(t, set, 0)
		checkDropped(t, set, 1)
		probe, cancelProbe := context.WithCancel(t.Context())
		cancelProbe()
		if err := clock.WaitForTimer(probe, start.Add(2*time.Second)); !errors.Is(err, context.Canceled) {
			t.Fatalf("timer is still armed after Stop: %v", err)
		}

		s.At(clock.Now(), dispatch.Key{A: 4}, noopTask)
		checkDropped(t, set, 2)
		clock.Advance(time.Second)
		synctest.Wait()
		checkOrder(t, submitted, []int{1, 2})
		if err := s.Start(t.Context()); err == nil {
			t.Fatal("Start after Stop succeeded")
		}
	})
}

func TestSchedulerStopDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := scheduler.NewFakeClock(time.Unix(0, 0))
		set := metrics.NewSet()
		entered := make(chan struct{})
		release := make(chan struct{})
		s := newScheduler(t, scheduler.Deps{
			Clock: clock, Metrics: set,
			Dispatcher: submitFunc(func(k dispatch.Key, _ dispatch.Task) error {
				if k.A != 1 {
					t.Errorf("unexpected Submit for key %v", k)
					return nil
				}
				close(entered)
				<-release
				return dispatch.ErrBusy
			}),
		})
		var releaseOnce sync.Once
		releaseSubmit := func() { releaseOnce.Do(func() { close(release) }) }
		defer startScheduler(t, s)()
		defer releaseSubmit()
		s.At(clock.Now(), dispatch.Key{A: 1}, noopTask)
		<-entered
		s.At(clock.Now(), dispatch.Key{A: 2}, noopTask)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := s.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Stop with Submit in flight = %v, want context.DeadlineExceeded", err)
		}
		s.At(clock.Now(), dispatch.Key{A: 3}, noopTask)
		checkDropped(t, set, 1)
		releaseSubmit()
		stopScheduler(t, s)
		checkPending(t, set, 0)
		checkDropped(t, set, 3)
	})
}

func TestSchedulerConcurrentAt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(0, 0)
		clock := scheduler.NewFakeClock(start)
		var submitted []int
		s := newScheduler(t, scheduler.Deps{
			Clock: clock,
			Dispatcher: submitFunc(func(k dispatch.Key, task dispatch.Task) error {
				submitted = append(submitted, int(k.A))
				return runTask(k, task)
			}),
		})
		defer startScheduler(t, s)()
		const count = 1024
		want := make([]int, count)
		var producers sync.WaitGroup
		for i := range count {
			want[i] = i
			producers.Go(func() {
				id := count - i - 1
				s.At(start.Add(time.Duration(id+1)*time.Millisecond), dispatch.Key{A: int64(id)}, noopTask)
			})
		}
		producers.Wait()
		checkArmed(t, clock, start.Add(time.Millisecond))
		synctest.Wait()
		checkOrder(t, submitted, nil)
		clock.Advance(count * time.Millisecond)
		synctest.Wait()
		checkOrder(t, submitted, want)
	})
}

func TestSchedulerRequiresNonNilTask(t *testing.T) {
	s := newScheduler(t, scheduler.Deps{Dispatcher: submitFunc(runTask)})
	defer func() {
		if recover() == nil {
			t.Error("At accepted a nil task")
		}
	}()
	s.At(time.Time{}, dispatch.Key{}, nil)
}

func TestSchedulerWithDispatcher(t *testing.T) {
	d, err := dispatch.New(dispatch.Deps{Shards: 2, Inbox: 8, TicketsPerShard: 1, MailboxPerEntity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := d.Stop(ctx); err != nil {
			t.Error(err)
		}
	}()
	clock := scheduler.SystemClock()
	s := newScheduler(t, scheduler.Deps{Dispatcher: d, Clock: clock})
	key := dispatch.Key{A: 42}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	probe := make(chan int, 1)
	if err := d.Submit(key, func(_ context.Context, sh *dispatch.Shard) { probe <- sh.Index() }); err != nil {
		t.Fatal(err)
	}
	shard := receive(t, ctx, probe)
	type execution struct{ id, shard int }
	executed := make(chan execution, 3)
	start := clock.Now()
	for _, id := range []int{3, 1, 2} {
		s.At(start.Add(time.Duration(id)*3*time.Millisecond), key, func(_ context.Context, sh *dispatch.Shard) {
			executed <- execution{id, sh.Index()}
		})
	}
	defer startScheduler(t, s)()
	for _, id := range []int{1, 2, 3} {
		if got, want := receive(t, ctx, executed), (execution{id, shard}); got != want {
			t.Fatalf("execution = %+v, want %+v", got, want)
		}
	}
}

type submitFunc func(dispatch.Key, dispatch.Task) error

func (f submitFunc) Submit(k dispatch.Key, task dispatch.Task) error { return f(k, task) }

func runTask(_ dispatch.Key, task dispatch.Task) error {
	task(context.Background(), nil)
	return nil
}

func noopTask(context.Context, *dispatch.Shard) {}

func newScheduler(t *testing.T, deps scheduler.Deps) *scheduler.Scheduler {
	t.Helper()
	s, err := scheduler.New(deps)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func startScheduler(t *testing.T, s *scheduler.Scheduler) func() {
	t.Helper()
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	return func() { stopScheduler(t, s) }
}

func stopScheduler(t *testing.T, s *scheduler.Scheduler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Error(err)
	}
}

func checkArmed(t *testing.T, clock *scheduler.FakeClock, deadline time.Time) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := clock.WaitForTimer(ctx, deadline); err != nil {
		t.Fatalf("waiting for timer at %v: %v", deadline, err)
	}
}

func checkOrder(t *testing.T, got, want []int) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("submitted %v, want %v", got, want)
	}
}

func checkPending(t *testing.T, set *metrics.Set, want float64) {
	t.Helper()
	if got := set.GetOrCreateGauge("yabot_scheduler_pending", nil).Get(); got != want {
		t.Errorf("pending = %v, want %v", got, want)
	}
}

func checkDropped(t *testing.T, set *metrics.Set, want uint64) {
	t.Helper()
	if got := set.GetOrCreateCounter("yabot_scheduler_dropped_total").Get(); got != want {
		t.Errorf("dropped = %d, want %d", got, want)
	}
}

func receive[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatal("timed out waiting for task execution")
		var zero T
		return zero
	}
}

func TestSchedulerRetryWaitsFromFailureForOverdueTask(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Unix(0, 0)
		clock := scheduler.NewFakeClock(start)
		attempts := 0
		s := newScheduler(t, scheduler.Deps{
			Clock: clock,
			Dispatcher: submitFunc(func(dispatch.Key, dispatch.Task) error {
				attempts++
				if attempts == 1 {
					return dispatch.ErrBusy
				}
				return nil
			}),
		})
		s.At(start.Add(-time.Second), dispatch.Key{}, noopTask)
		defer startScheduler(t, s)()
		synctest.Wait()
		if attempts != 1 {
			t.Fatalf("attempts = %d before the retry delay, want 1", attempts)
		}
		checkArmed(t, clock, start.Add(50*time.Millisecond))
		clock.Advance(50*time.Millisecond - time.Nanosecond)
		synctest.Wait()
		if attempts != 1 {
			t.Fatalf("attempts = %d before the retry delay, want 1", attempts)
		}
		clock.Advance(time.Nanosecond)
		synctest.Wait()
		if attempts != 2 {
			t.Fatalf("attempts = %d after the retry delay, want 2", attempts)
		}
	})
}
