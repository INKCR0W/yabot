package dispatch_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/VictoriaMetrics/metrics"
)

func TestRunOnAllCollectsAsyncResults(t *testing.T) {
	firstErr := errors.New("shard one failed")
	secondErr := errors.New("shard two failed")
	ignoredErr := errors.New("duplicate result")
	for _, tc := range []struct {
		name           string
		errs           [3]error
		duplicates     bool
		withoutMetrics bool
	}{
		{name: "success"},
		{name: "joined errors and duplicate results", errs: [3]error{nil, firstErr, secondErr}, duplicates: true},
		{name: "duplicate results without metrics", errs: [3]error{nil, firstErr, secondErr}, duplicates: true, withoutMetrics: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var set *metrics.Set
				if !tc.withoutMetrics {
					set = metrics.NewSet()
				}
				d := newRunOnAllDispatcher(t, len(tc.errs), 1, set)
				startRunOnAllDispatcher(t, d)
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				visited := make(chan int, len(tc.errs))
				result := make(chan error, 1)
				go func() {
					result <- d.RunOnAll(t.Context(), func(_ context.Context, sh *dispatch.Shard, done func(error)) {
						index := sh.Index()
						visited <- index
						go func() {
							<-release
							done(tc.errs[index])
							if tc.duplicates {
								done(ignoredErr)
							}
						}()
					})
				}()
				synctest.Wait()
				if n := len(visited); n != len(tc.errs) {
					t.Fatalf("visited %d shards, want %d", n, len(tc.errs))
				}
				var seen [3]int
				for range tc.errs {
					index := <-visited
					if index < 0 || index >= len(seen) {
						t.Fatalf("invalid shard index %d", index)
					}
					seen[index]++
				}
				if seen != [3]int{1, 1, 1} {
					t.Fatalf("visits by shard = %v, want [1 1 1]", seen)
				}
				select {
				case err := <-result:
					t.Fatalf("RunOnAll returned before done: %v", err)
				default:
				}
				unblock()
				err := <-result
				if !tc.duplicates && err != nil {
					t.Fatalf("RunOnAll: %v", err)
				}
				if tc.duplicates && (!errors.Is(err, firstErr) || !errors.Is(err, secondErr) || errors.Is(err, ignoredErr)) {
					t.Errorf("RunOnAll = %v, want only first results from each shard", err)
				}
				synctest.Wait()
				if set == nil {
					return
				}
				var output strings.Builder
				set.WritePrometheus(&output)
				var want uint64
				if tc.duplicates {
					want = uint64(len(tc.errs))
				}
				line := fmt.Sprintf("yabot_dispatch_run_on_all_done_late_total %d\n", want)
				if !strings.Contains(output.String(), line) {
					t.Errorf("missing metric %q in %q", line, output.String())
				}
			})
		})
	}
}

func TestRunOnAllDeadline(t *testing.T) {
	for _, tc := range []struct {
		name      string
		fullInbox bool
	}{
		{name: "waiting for done"},
		{name: "retrying full inbox", fullInbox: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := newRunOnAllDispatcher(t, 1, 1, nil)
				startRunOnAllDispatcher(t, d)
				if tc.fullInbox {
					blockRunOnAllInbox(t, d)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
				defer cancel()
				calls := make(chan func(error), 1)
				err := d.RunOnAll(ctx, func(_ context.Context, _ *dispatch.Shard, done func(error)) {
					calls <- done
				})
				if err != context.DeadlineExceeded {
					t.Fatalf("RunOnAll = %v, want %v", err, context.DeadlineExceeded)
				}
				if tc.fullInbox {
					if len(calls) != 0 {
						t.Fatal("RunOnAll ran with the inbox blocked")
					}
					return
				}
				select {
				case done := <-calls:
					done(nil)
				default:
					t.Fatal("RunOnAll task did not run before the deadline")
				}
			})
		})
	}
}

func TestRunOnAllRejectsShardContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newRunOnAllDispatcher(t, 1, 1, nil)
		startRunOnAllDispatcher(t, d)
		err := d.RunOnAll(t.Context(), func(ctx context.Context, _ *dispatch.Shard, done func(error)) {
			derived, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			done(d.RunOnAll(derived, func(_ context.Context, _ *dispatch.Shard, done func(error)) { done(nil) }))
		})
		if !errors.Is(err, dispatch.ErrInShard) {
			t.Fatalf("nested RunOnAll = %v, want ErrInShard", err)
		}
	})
}

func TestRunOnAllRetriesFullInbox(t *testing.T) {
	for _, tc := range []struct {
		fullThrough time.Duration
		retryAt     time.Duration
	}{
		{0, time.Millisecond},
		{time.Millisecond, 3 * time.Millisecond},
		{3 * time.Millisecond, 7 * time.Millisecond},
		{7 * time.Millisecond, 15 * time.Millisecond},
		{15 * time.Millisecond, 31 * time.Millisecond},
		{31 * time.Millisecond, 63 * time.Millisecond},
		{63 * time.Millisecond, 113 * time.Millisecond},
		{113 * time.Millisecond, 163 * time.Millisecond},
		{163 * time.Millisecond, 213 * time.Millisecond},
	} {
		t.Run(tc.retryAt.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := newRunOnAllDispatcher(t, 1, 1, nil)
				startRunOnAllDispatcher(t, d)
				unblock := blockRunOnAllInbox(t, d)
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				started := time.Now()
				result := make(chan error, 1)
				go func() {
					result <- d.RunOnAll(ctx, func(_ context.Context, _ *dispatch.Shard, done func(error)) { done(nil) })
				}()
				// Leave the inbox full through the preceding retry before making room.
				time.Sleep(tc.fullThrough + time.Nanosecond)
				unblock()
				if err := <-result; err != nil {
					t.Fatalf("RunOnAll after inbox drained: %v", err)
				}
				if elapsed := time.Since(started); elapsed != tc.retryAt {
					t.Errorf("RunOnAll returned after %v, want %v", elapsed, tc.retryAt)
				}
			})
		})
	}
}

func TestRunOnAllPanic(t *testing.T) {
	for _, tc := range []struct {
		name      string
		callsDone bool
	}{
		{name: "before done"},
		{name: "after done", callsDone: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := newRunOnAllDispatcher(t, 1, 1, nil)
				startRunOnAllDispatcher(t, d)
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				err := d.RunOnAll(ctx, func(_ context.Context, _ *dispatch.Shard, done func(error)) {
					if tc.callsDone {
						done(nil)
					}
					panic("async task panicked")
				})
				if err == nil || !strings.Contains(err.Error(), "async task panicked") {
					t.Fatalf("RunOnAll = %v, want the panic error", err)
				}
			})
		})
	}
}

func TestRunOnAllClosed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newRunOnAllDispatcher(t, 1, 1, nil)
		checkClosed := func(phase string) {
			err := d.RunOnAll(t.Context(), func(_ context.Context, _ *dispatch.Shard, done func(error)) { done(nil) })
			if !errors.Is(err, dispatch.ErrClosed) {
				t.Fatalf("RunOnAll %s = %v, want ErrClosed", phase, err)
			}
		}
		checkClosed("before Start")
		startRunOnAllDispatcher(t, d)
		unblock := blockRunOnAllInbox(t, d)
		pending := make(chan error, 1)
		go func() {
			pending <- d.RunOnAll(t.Context(), func(_ context.Context, _ *dispatch.Shard, done func(error)) { done(nil) })
		}()
		synctest.Wait()
		stopped := make(chan error, 1)
		go func() { stopped <- d.Stop(t.Context()) }()
		synctest.Wait()
		checkClosed("during Stop")
		if err := <-pending; !errors.Is(err, dispatch.ErrClosed) {
			t.Errorf("RunOnAll waiting for inbox space = %v, want ErrClosed", err)
		}
		unblock()
		if err := <-stopped; err != nil {
			t.Fatalf("Stop: %v", err)
		}
		checkClosed("after Stop")
	})
}

func newRunOnAllDispatcher(t *testing.T, shards, inbox int, set *metrics.Set) *dispatch.Dispatcher {
	t.Helper()
	d, err := dispatch.New(dispatch.Deps{
		Shards: shards, Inbox: inbox, TicketsPerShard: 1, MailboxPerEntity: 1, Metrics: set,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return d
}

func startRunOnAllDispatcher(t *testing.T, d *dispatch.Dispatcher) {
	t.Helper()
	if err := d.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := d.Stop(ctx); err != nil {
			t.Errorf("Stop in cleanup: %v", err)
		}
	})
}

func blockRunOnAllInbox(t *testing.T, d *dispatch.Dispatcher) func() {
	t.Helper()
	started := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	if err := d.Submit(dispatch.Key{}, func(context.Context, *dispatch.Shard) {
		close(started)
		<-release
	}); err != nil {
		t.Fatalf("Submit blocker: %v", err)
	}
	<-started
	if err := d.Submit(dispatch.Key{}, func(context.Context, *dispatch.Shard) {}); err != nil {
		t.Fatalf("Submit filler: %v", err)
	}
	return unblock
}

func TestRunOnAllConcurrentDoneCallsAdoptOneResult(t *testing.T) {
	const callers = 64
	set := metrics.NewSet()
	d := newRunOnAllDispatcher(t, 1, 1, set)
	startRunOnAllDispatcher(t, d)
	errs := make([]error, callers)
	for i := range errs {
		errs[i] = fmt.Errorf("result %d", i)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var callersDone sync.WaitGroup
	err := d.RunOnAll(ctx, func(_ context.Context, _ *dispatch.Shard, done func(error)) {
		start := make(chan struct{})
		for i := range callers {
			callersDone.Go(func() {
				<-start
				done(errs[i])
			})
		}
		close(start)
	})
	callersDone.Wait()
	adopted := 0
	for _, candidate := range errs {
		if errors.Is(err, candidate) {
			adopted++
		}
	}
	if adopted != 1 {
		t.Fatalf("RunOnAll = %v, want exactly one adopted result", err)
	}
	var output strings.Builder
	set.WritePrometheus(&output)
	if line := fmt.Sprintf("yabot_dispatch_run_on_all_done_late_total %d\n", callers-1); !strings.Contains(output.String(), line) {
		t.Fatalf("metrics missing %q:\n%s", line, output.String())
	}
}
