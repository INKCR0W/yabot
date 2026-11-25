package janitor_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/janitor"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/VictoriaMetrics/metrics"
)

func TestJanitorFirstRunAndPeriod(t *testing.T) {
	for _, tc := range []struct {
		name  string
		every time.Duration
		first time.Duration
	}{
		{name: "short period", every: 5 * time.Millisecond, first: 5 * time.Millisecond},
		{name: "long period", every: time.Hour, first: time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				j := janitor.New(janitor.Deps{Metrics: metrics.NewSet()})
				start := time.Now()
				runs := make(chan time.Duration, 4)
				j.Register("timed", tc.every, func(ctx context.Context) (int, error) {
					select {
					case runs <- time.Since(start):
					case <-ctx.Done():
						return 0, ctx.Err()
					}
					return 0, nil
				})
				startJanitor(t, j, t.Context())

				time.Sleep(tc.first - time.Nanosecond)
				synctest.Wait()
				if got := len(runs); got != 0 {
					t.Fatalf("runs before first interval = %d, want 0", got)
				}
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				if got := len(runs); got != 1 {
					t.Fatalf("runs at first interval = %d, want 1", got)
				}
				time.Sleep(2 * tc.every)
				synctest.Wait()
				want := []time.Duration{tc.first, tc.first + tc.every, tc.first + 2*tc.every}
				if got := len(runs); got != len(want) {
					t.Fatalf("run count = %d, want %d", got, len(want))
				}
				for i, stamp := range want {
					if got := <-runs; got != stamp {
						t.Errorf("run %d time = %v, want %v", i+1, got, stamp)
					}
				}
			})
		})
	}
}

func TestJanitorTasksRunIndependently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const slowEvery = 10 * time.Millisecond
		const fastEvery = 5 * time.Millisecond
		j := janitor.New(janitor.Deps{Metrics: metrics.NewSet()})
		releaseSlow := make(chan struct{})
		var slowRuns, fastRuns, activeSlow, overlap atomic.Int32
		j.Register("slow", slowEvery, func(ctx context.Context) (int, error) {
			if activeSlow.Add(1) > 1 {
				overlap.Add(1)
			}
			defer activeSlow.Add(-1)
			if slowRuns.Add(1) == 1 {
				select {
				case <-releaseSlow:
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}
			return 0, nil
		})
		j.Register("fast", fastEvery, func(context.Context) (int, error) {
			fastRuns.Add(1)
			return 0, nil
		})
		startJanitor(t, j, t.Context())

		time.Sleep(slowEvery)
		synctest.Wait()
		if got := activeSlow.Load(); got != 1 {
			t.Fatalf("active slow runs at first tick = %d, want 1", got)
		}
		fastBefore := fastRuns.Load()
		time.Sleep(5 * fastEvery)
		synctest.Wait()
		if got := fastRuns.Load() - fastBefore; got < 2 {
			t.Errorf("fast runs while slow task was blocked = %d, want at least 2", got)
		}
		if got := slowRuns.Load(); got != 1 || overlap.Load() != 0 {
			t.Errorf("slow runs while blocked = %d, overlap count = %d, want 1 and 0", got, overlap.Load())
		}

		close(releaseSlow)
		synctest.Wait()
		if got := slowRuns.Load(); got != 1 {
			t.Errorf("slow runs immediately after release = %d, want 1 with missed ticks skipped", got)
		}
		time.Sleep(fastEvery)
		synctest.Wait()
		if got := slowRuns.Load(); got != 2 {
			t.Errorf("slow runs at next scheduled tick = %d, want 2", got)
		}
	})
}

func TestJanitorStop(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{name: "waits for task"},
		{name: "deadline expires and retry waits", timeout: 3 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const every = 5 * time.Millisecond
				j := janitor.New(janitor.Deps{Metrics: metrics.NewSet()})
				if err := j.Stop(t.Context()); err != nil {
					t.Fatalf("Stop before Start: %v", err)
				}
				taskCanceled := make(chan error, 1)
				releaseTask := make(chan struct{}, 1)
				defer close(releaseTask)
				finished := make(chan struct{})
				j.Register("wait", every, func(ctx context.Context) (int, error) {
					defer close(finished)
					<-ctx.Done()
					taskCanceled <- ctx.Err()
					<-releaseTask
					return 0, ctx.Err()
				})
				startJanitor(t, j, t.Context())
				time.Sleep(every)
				synctest.Wait()

				stopCtx := t.Context()
				if tc.timeout > 0 {
					ctx, cancel := context.WithTimeout(stopCtx, tc.timeout)
					defer cancel()
					stopCtx = ctx
				}
				stopped := make(chan error, 1)
				go func() { stopped <- j.Stop(stopCtx) }()
				synctest.Wait()
				select {
				case err := <-taskCanceled:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("task context error = %v, want context.Canceled", err)
					}
				default:
					t.Fatal("Stop did not cancel the running task")
				}
				select {
				case err := <-stopped:
					t.Fatalf("Stop returned %v while the task was running and its context was live", err)
				default:
				}

				if tc.timeout > 0 {
					time.Sleep(tc.timeout)
					synctest.Wait()
					checkStopResult(t, stopped, context.DeadlineExceeded)

					retryCtx, cancelRetry := context.WithCancel(t.Context())
					defer cancelRetry()
					go func() { stopped <- j.Stop(retryCtx) }()
					synctest.Wait()
					select {
					case err := <-stopped:
						t.Fatalf("retry Stop returned %v before the task was released", err)
					default:
					}
				}
				releaseTask <- struct{}{}
				synctest.Wait()
				checkStopResult(t, stopped, nil)
				if err := j.Stop(t.Context()); err != nil {
					t.Fatalf("repeat Stop: %v", err)
				}
				select {
				case <-finished:
				default:
					t.Fatal("Stop returned before the task exited")
				}
			})
		})
	}
}

func TestJanitorStartContextDoesNotOwnTasks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const every = 5 * time.Millisecond
		j := janitor.New(janitor.Deps{Metrics: metrics.NewSet()})
		startCtx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var runs atomic.Int32
		j.Register("continue", every, func(ctx context.Context) (int, error) {
			if err := ctx.Err(); err != nil {
				t.Errorf("task context error before Stop = %v, want nil", err)
			}
			runs.Add(1)
			return 0, nil
		})
		startJanitor(t, j, startCtx)
		cancel()
		time.Sleep(2 * every)
		synctest.Wait()
		if got := runs.Load(); got != 2 {
			t.Fatalf("runs after canceling Start context = %d, want 2", got)
		}
		if err := j.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * every)
		if got := runs.Load(); got != 2 {
			t.Errorf("runs after Stop = %d, want 2", got)
		}
	})
}

func TestJanitorRegisterRejectsInvalidTasks(t *testing.T) {
	fn := func(context.Context) (int, error) { return 0, nil }
	for _, tc := range []struct {
		caseName string
		name     string
		every    time.Duration
		fn       func(context.Context) (int, error)
		setup    func(*testing.T, *janitor.Janitor)
	}{
		{caseName: "empty name", every: time.Minute, fn: fn},
		{caseName: "zero period", name: "task", fn: fn},
		{caseName: "negative period", name: "task", every: -time.Second, fn: fn},
		{caseName: "nil task", name: "task", every: time.Minute},
		{caseName: "duplicate", name: "task", every: time.Minute, fn: fn, setup: func(_ *testing.T, j *janitor.Janitor) {
			j.Register("task", time.Hour, fn)
		}},
		{caseName: "already started", name: "task", every: time.Minute, fn: fn, setup: func(t *testing.T, j *janitor.Janitor) {
			startJanitor(t, j, t.Context())
		}},
	} {
		t.Run(tc.caseName, func(t *testing.T) {
			j := janitor.New(janitor.Deps{Metrics: metrics.NewSet()})
			if tc.setup != nil {
				tc.setup(t, j)
			}
			defer func() {
				if recover() == nil {
					t.Error("Register did not panic")
				}
			}()
			j.Register(tc.name, tc.every, tc.fn)
		})
	}
}

func TestJanitorTaskLogsAndMetrics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const every = 5 * time.Millisecond
		const cleanupDuration = time.Millisecond
		const panicValue = "cleanup panic"
		var logOutput bytes.Buffer
		log := slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug}))
		set := metrics.NewSet()
		j := janitor.New(janitor.Deps{Log: log, Metrics: set})
		var runs atomic.Int32
		j.Register("cleanup", every, func(ctx context.Context) (int, error) {
			time.Sleep(cleanupDuration)
			switch runs.Add(1) {
			case 1:
				panic(panicValue)
			case 2:
				return 0, fmt.Errorf("delete batch: %w", store.ErrBusy)
			case 3:
				return 2, nil
			case 4:
				return 3, errors.New("cleanup failed")
			default:
				<-ctx.Done()
				return 0, fmt.Errorf("cleanup stopped: %w", ctx.Err())
			}
		})
		j.Register("backup", 2*every, func(context.Context) (int, error) {
			time.Sleep(2 * time.Millisecond)
			return 7, io.ErrUnexpectedEOF
		})
		startJanitor(t, j, t.Context())
		time.Sleep(5*every + cleanupDuration)
		synctest.Wait()
		if got := runs.Load(); got != 5 {
			t.Fatalf("runs after panic and returned errors = %d, want 5", got)
		}
		if err := j.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}

		levels := make(map[string][]string)
		wantLevels := map[string][]string{
			"cleanup": {"ERROR", "INFO", "DEBUG", "ERROR"},
			"backup":  {"ERROR", "ERROR"},
		}
		decoder := json.NewDecoder(&logOutput)
		for {
			var entry struct {
				Level string `json:"level"`
				Task  string `json:"task"`
				Panic string `json:"panic"`
			}
			if err := decoder.Decode(&entry); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				t.Fatal(err)
			}
			if _, ok := wantLevels[entry.Task]; !ok {
				t.Errorf("unexpected log task %q", entry.Task)
			}
			if entry.Task == "cleanup" && len(levels[entry.Task]) == 0 && entry.Panic != panicValue {
				t.Errorf("logged panic value = %q, want %q", entry.Panic, panicValue)
			}
			levels[entry.Task] = append(levels[entry.Task], entry.Level)
		}
		for task, want := range wantLevels {
			if got := levels[task]; !slices.Equal(got, want) {
				t.Errorf("log levels for %s = %v, want %v", task, got, want)
			}
		}

		var metricsOutput strings.Builder
		set.WritePrometheus(&metricsOutput)
		lines := strings.Split(metricsOutput.String(), "\n")
		for _, want := range []string{
			`yabot_janitor_runs_total{task="cleanup"} 5`,
			`yabot_janitor_errors_total{task="cleanup"} 4`,
			`yabot_janitor_rows_total{task="cleanup"} 5`,
			`yabot_janitor_duration_seconds_count{task="cleanup"} 5`,
			`yabot_janitor_duration_seconds_sum{task="cleanup"} 0.005`,
			`yabot_janitor_runs_total{task="backup"} 2`,
			`yabot_janitor_errors_total{task="backup"} 2`,
			`yabot_janitor_rows_total{task="backup"} 14`,
			`yabot_janitor_duration_seconds_count{task="backup"} 2`,
			`yabot_janitor_duration_seconds_sum{task="backup"} 0.004`,
		} {
			if !slices.Contains(lines, want) {
				t.Errorf("missing metric %q in %q", want, metricsOutput.String())
			}
		}
	})
}

func TestJanitorLogsBusyErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		level string
	}{
		{name: "busy", err: store.ErrBusy, level: "INFO"},
		{name: "wrapped busy", err: &store.OpError{Err: store.ErrBusy}, level: "INFO"},
		{name: "joined busy", err: errors.Join(store.ErrBusy, &store.OpError{Err: store.ErrBusy}), level: "INFO"},
		{name: "busy and IO failure", err: errors.Join(store.ErrBusy, io.ErrUnexpectedEOF), level: "ERROR"},
		{name: "wrapped mixed errors", err: &store.OpError{Err: errors.Join(store.ErrBusy, io.ErrUnexpectedEOF)}, level: "ERROR"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const every = 5 * time.Millisecond
				var output bytes.Buffer
				log := slog.New(slog.NewJSONHandler(&output, nil))
				j := janitor.New(janitor.Deps{Log: log, Metrics: metrics.NewSet()})
				j.Register("cleanup", every, func(context.Context) (int, error) { return 0, tc.err })
				startJanitor(t, j, t.Context())
				time.Sleep(every)
				synctest.Wait()
				if err := j.Stop(t.Context()); err != nil {
					t.Fatal(err)
				}

				var entry struct {
					Level string `json:"level"`
					Error string `json:"error"`
				}
				if err := json.NewDecoder(&output).Decode(&entry); err != nil {
					t.Fatal(err)
				}
				if entry.Level != tc.level || entry.Error != tc.err.Error() {
					t.Errorf("log = (%q, %q), want (%q, %q)", entry.Level, entry.Error, tc.level, tc.err)
				}
			})
		})
	}
}

func TestJanitorLogsCleanupFailureAfterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const every = 5 * time.Millisecond
		var output bytes.Buffer
		log := slog.New(slog.NewJSONHandler(&output, nil))
		j := janitor.New(janitor.Deps{Log: log, Metrics: metrics.NewSet()})
		j.Register("backup", every, func(ctx context.Context) (int, error) {
			<-ctx.Done()
			err := errors.Join(ctx.Err(), errors.New("remove temporary snapshot failed"))
			return 0, fmt.Errorf("backup failed: %w", err)
		})
		startJanitor(t, j, t.Context())
		time.Sleep(every)
		synctest.Wait()
		if err := j.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}

		var entry struct {
			Level string `json:"level"`
			Task  string `json:"task"`
			Error string `json:"error"`
		}
		if err := json.NewDecoder(&output).Decode(&entry); err != nil {
			t.Fatal(err)
		}
		if entry.Level != "ERROR" || entry.Task != "backup" || !strings.Contains(entry.Error, "remove temporary snapshot failed") {
			t.Errorf("cleanup log = %+v, want the backup task's cleanup error at ERROR level", entry)
		}
	})
}

func startJanitor(t *testing.T, j *janitor.Janitor, ctx context.Context) {
	t.Helper()
	if err := j.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := j.Stop(ctx); err != nil {
			t.Errorf("Stop in cleanup: %v", err)
		}
	})
}

func checkStopResult(t *testing.T, result <-chan error, want error) {
	t.Helper()
	select {
	case err := <-result:
		if !errors.Is(err, want) {
			t.Errorf("Stop = %v, want %v", err, want)
		}
	default:
		t.Error("Stop did not return")
	}
}
