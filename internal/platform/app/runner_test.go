package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestRunShutdown(t *testing.T) {
	type contextKey struct{}
	for _, trigger := range []string{"cancel", "fatal"} {
		t.Run(trigger, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.WithValue(t.Context(), contextKey{}, "value"))
				defer cancel()
				var log bytes.Buffer
				runner := testRunner(&log)
				fatal := runner.Fatal()
				cause := &fs.PathError{Op: "read", Path: "db", Err: errors.New("storage lost")}
				report := make(chan struct{})
				calls := make(callLog, 6)
				last := calls.component("C", func(startCtx context.Context) error {
					if startCtx.Value(contextKey{}) != "value" {
						t.Error("Start context did not inherit the Run context value")
					}
					if trigger == "fatal" {
						go func() {
							<-report
							fatal("C", cause)
						}()
					}
					return nil
				}, nil)
				result := runAsync(runner, ctx, calls.component("A", nil, nil), calls.component("B", nil, nil), last)
				synctest.Wait()
				select {
				case err := <-result:
					t.Fatalf("Run returned before shutdown: %v", err)
				default:
				}
				if trigger == "fatal" {
					close(report)
					err := <-result
					assertComponentError(t, err, "C", cause)
					var pathError *fs.PathError
					if !errors.As(err, &pathError) || pathError != cause {
						t.Errorf("Run = %v, want original *fs.PathError", err)
					}
				} else {
					cancel()
					if err := <-result; err != nil {
						t.Errorf("Run after cancellation = %v, want nil", err)
					}
				}
				calls.check(t, "start A", "start B", "start C", "stop C", "stop B", "stop A")
				synctest.Wait()
				logged := log.String()
				for _, component := range []string{"A", "B", "C"} {
					for _, message := range []string{"component start finished", "component stop attempt finished"} {
						if !strings.Contains(logged, `msg="`+message+`" component=`+component+" duration=") {
							t.Errorf("missing %q record with duration for %s in %s", message, component, logged)
						}
					}
				}
			})
		})
	}
}

func TestRunRollsBackStartFailure(t *testing.T) {
	cause := errors.New("broken setup")
	for _, tc := range []struct {
		name    string
		start   func(context.Context) error
		wantErr error
	}{
		{"error", func(context.Context) error { return cause }, cause},
		{"panic", func(context.Context) error { panic(cause) }, cause},
		{"cancellation error with active context", func(context.Context) error { return context.Canceled }, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var log bytes.Buffer
			calls := make(callLog, 8)
			runner := NewRunner(slog.New(slog.NewJSONHandler(&log, nil)))
			var contexts []context.Context
			recordStart := func(ctx context.Context) error {
				contexts = append(contexts, ctx)
				return nil
			}
			checkContexts := func(context.Context) error {
				for i, ctx := range contexts {
					if ctx.Err() != context.Canceled {
						t.Errorf("Start context for component at index %d has error %v when rollback begins, want context.Canceled", i, ctx.Err())
					}
				}
				return nil
			}
			err := runner.Run(t.Context(),
				calls.component("A", recordStart, nil), calls.component("B", recordStart, checkContexts),
				calls.component("C", tc.start, nil), calls.component("D", nil, nil),
			)
			assertComponentError(t, err, "C", tc.wantErr)
			loggedError := tc.wantErr.Error()
			if tc.name == "panic" {
				const wantError = `start component "C": panic: broken setup`
				if err.Error() != wantError {
					t.Errorf("Run = %q, want %q", err, wantError)
				}
				loggedError = "panic: " + loggedError
			}
			record := assertLogRecord(t, &log, map[string]string{
				"level": "ERROR", "msg": "component start failed", "component": "C", "error": loggedError,
			})
			if tc.name == "panic" {
				assertPanicStack(t, record, "TestRunRollsBackStartFailure")
			}
			calls.check(t, "start A", "start B", "start C", "stop B", "stop A")
		})
	}
}

func TestRunKeepsStartFailureWhenCanceledDuringLog(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runner := testRunner(writerFunc(func(p []byte) (int, error) {
		if bytes.Contains(p, []byte("component start finished")) && bytes.Contains(p, []byte("component=C ")) {
			cancel()
		}
		return len(p), nil
	}))
	calls := make(callLog, 8)
	start := func(context.Context) error { return context.Canceled }
	err := runner.Run(ctx,
		calls.component("A", nil, nil), calls.component("B", nil, nil),
		calls.component("C", start, nil), calls.component("D", nil, nil),
	)
	if ctx.Err() != context.Canceled {
		t.Fatal("Start log did not trigger cancellation")
	}
	assertComponentError(t, err, "C", context.Canceled)
	calls.check(t, "start A", "start B", "start C", "stop B", "stop A")
}

func TestRunFatalCancelsBlockedStart(t *testing.T) {
	cause := errors.New("storage lost during startup")
	startErr := errors.New("startup failed")
	for _, tc := range []struct {
		name         string
		start        func(context.Context) error
		wantStartErr error
	}{
		{name: "context error", start: context.Context.Err},
		{name: "context cause", start: context.Cause},
		{name: "startup error", start: func(context.Context) error { return startErr }, wantStartErr: startErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				runner := testRunner(io.Discard)
				report := runner.Fatal()
				entered := make(chan struct{})
				calls := make(callLog, 6)
				a := calls.component("A", func(context.Context) error {
					go func() {
						<-entered
						report("A", cause)
					}()
					return nil
				}, nil)
				b := calls.component("B", func(startCtx context.Context) error {
					close(entered)
					<-startCtx.Done()
					if got := context.Cause(startCtx); !errors.Is(got, cause) {
						t.Errorf("Start context cause = %v, want %v", got, cause)
					}
					return tc.start(startCtx)
				}, nil)
				result := runAsync(runner, ctx, a, b, calls.component("C", nil, nil))
				synctest.Wait()
				var err error
				select {
				case err = <-result:
				default:
					t.Fatal("Run did not return after Fatal was reported during Start")
				}
				assertComponentError(t, err, "A", cause)
				if errors.Is(err, context.Canceled) {
					t.Errorf("Run included a cancellation error: %v", err)
				}
				if tc.wantStartErr != nil {
					assertComponentError(t, err, "B", tc.wantStartErr)
				} else if strings.Contains(err.Error(), `start component "B"`) {
					t.Errorf("Run included a startup error: %v", err)
				}
				calls.check(t, "start A", "start B", "stop A")
			})
		})
	}
}

func TestRunInterruptedDuringStart(t *testing.T) {
	startErr := errors.New("startup failed")
	cancelCause := errors.New("shutdown requested")
	returnStartError := func(context.Context) error { return startErr }
	for _, tc := range []struct {
		name            string
		fatal           bool
		timeout         time.Duration
		start           func(context.Context) error
		wantStartErr    error
		wantLoggedError string
	}{
		{name: "fatal", fatal: true},
		{name: "fatal with start error", fatal: true, start: returnStartError, wantStartErr: startErr},
		{name: "cancel with wrapped error", start: func(ctx context.Context) error {
			return fmt.Errorf("interrupted Start: %w", ctx.Err())
		}},
		{name: "cancel with joined error", start: func(ctx context.Context) error { return errors.Join(ctx.Err()) }},
		{name: "cancel joined with setup error", start: func(ctx context.Context) error {
			return errors.Join(ctx.Err(), errors.New("migration failed"))
		}, wantLoggedError: "migration failed"},
		{name: "deadline joined with init error", timeout: time.Second, start: func(context.Context) error {
			return errors.Join(context.DeadlineExceeded, errors.New("init failed"))
		}},
		{name: "cancel with cause", start: context.Cause},
		{name: "cancel with unrelated error", start: returnStartError, wantStartErr: startErr},
		{name: "cancel with panic", start: func(ctx context.Context) error {
			panic(context.Cause(ctx))
		}, wantStartErr: cancelCause},
		{name: "cancel with success"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(t.Context())
				defer cancel(nil)
				if tc.timeout > 0 {
					timeoutCtx, stopTimer := context.WithTimeout(ctx, tc.timeout)
					defer stopTimer()
					ctx = timeoutCtx
				}
				calls := make(callLog, 8)
				entered := make(chan struct{})
				release := make(chan struct{})
				c := calls.component("C", func(ctx context.Context) error {
					close(entered)
					<-release
					if tc.start == nil {
						return nil
					}
					return tc.start(ctx)
				}, nil)
				cause := errors.New("storage lost during startup")
				var log bytes.Buffer
				runner := testRunner(&log)
				result := runAsync(runner, ctx,
					calls.component("A", nil, nil), calls.component("B", nil, nil), c, calls.component("D", nil, nil),
				)
				<-entered
				switch {
				case tc.timeout > 0:
					<-ctx.Done()
				case tc.fatal:
					runner.Fatal()("A", cause)
				default:
					cancel(cancelCause)
				}
				synctest.Wait()
				select {
				case err := <-result:
					t.Errorf("Run returned before Start finished: %v", err)
					close(release)
					return
				default:
				}
				close(release)
				err := <-result
				if tc.fatal {
					assertComponentError(t, err, "A", cause)
				}
				if tc.wantStartErr != nil {
					assertComponentError(t, err, "C", tc.wantStartErr)
				} else if !tc.fatal && err != nil {
					t.Errorf("Run after cancellation = %v, want nil", err)
				}
				if tc.wantLoggedError != "" {
					_, entry, _ := strings.Cut(log.String(), `level=INFO msg="component start canceled" component=C error=`)
					entry, _, _ = strings.Cut(entry, "\n")
					if !strings.Contains(entry, tc.wantLoggedError) {
						t.Errorf("missing cancellation record with %q in %s", tc.wantLoggedError, &log)
					}
				}
				if tc.start == nil {
					calls.check(t, "start A", "start B", "start C", "stop C", "stop B", "stop A")
				} else {
					calls.check(t, "start A", "start B", "start C", "stop B", "stop A")
				}
			})
		})
	}
}

func TestRunSkipsNextComponentAfterFatalFromStart(t *testing.T) {
	runner := testRunner(io.Discard)
	report := runner.Fatal()
	cause := errors.New("fatal from Start")
	calls := make(callLog, 4)
	a := calls.component("A", func(context.Context) error {
		report("A", cause)
		return nil
	}, nil)
	err := runner.Run(t.Context(), a, calls.component("B", nil, nil))
	assertComponentError(t, err, "A", cause)
	calls.check(t, "start A", "stop A")
}

func TestFatalBeforeRun(t *testing.T) {
	runner := testRunner(io.Discard)
	report := runner.Fatal()
	cause := errors.New("fatal construction error")
	later := errors.New("later error")
	report("store", cause)
	report("ingress", later)
	calls := make(callLog, 2)
	err := runner.Run(t.Context(), calls.component("A", nil, nil))
	assertComponentError(t, err, "store", cause)
	if errors.Is(err, later) {
		t.Errorf("Run included a later Fatal: %v", err)
	}
	calls.check(t)
}

func TestConcurrentFirstFatal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const reports = 32
		calls := make(callLog, 6)
		runner := testRunner(io.Discard)
		report := runner.Fatal()
		result := runAsync(runner, t.Context(),
			calls.component("A", nil, nil), calls.component("B", nil, nil), calls.component("C", nil, nil),
		)
		release := make(chan struct{})
		var wg sync.WaitGroup
		causes := make([]error, reports)
		for i := range causes {
			cause := fmt.Errorf("fatal report %d", i)
			causes[i] = cause
			wg.Go(func() {
				defer func() {
					if value := recover(); value != nil {
						t.Errorf("Fatal %d panicked: %v", i, value)
					}
				}()
				<-release
				report("worker", cause)
			})
		}
		synctest.Wait()
		close(release)
		wg.Wait()

		err := <-result
		var matches int
		for _, cause := range causes {
			if errors.Is(err, cause) {
				matches++
			}
		}
		if matches != 1 {
			t.Errorf("Run = %v, matched %d Fatal causes, want exactly one", err, matches)
		}
		calls.check(t, "start A", "start B", "start C", "stop C", "stop B", "stop A")
	})
}

func TestConcurrentFatalDoesNotWaitForLogger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const reports = 100
		var log bytes.Buffer
		releaseLog := make(chan struct{})
		fatalLogged := make(chan string, 1)
		writer := writerFunc(func(p []byte) (int, error) {
			if bytes.Contains(p, []byte("level=ERROR")) {
				<-releaseLog
			}
			if bytes.Contains(p, []byte(`msg="component reported a fatal error"`)) {
				fatalLogged <- string(p)
			}
			return log.Write(p)
		})
		runner := testRunner(writer)
		report := runner.Fatal()
		stopping := make(chan struct{})
		releaseStop := make(chan struct{})
		store := make(callLog, 2).component("store", nil, func(context.Context) error {
			close(stopping)
			<-releaseStop
			return nil
		})
		result := runAsync(runner, t.Context(), store)
		synctest.Wait()
		first := errors.New("first fatal error")
		report("store", first)
		<-stopping

		ready := make(chan struct{})
		var wg sync.WaitGroup
		causes := make([]error, reports)
		for i := range causes {
			causes[i] = errors.New("concurrent fatal error")
			wg.Go(func() {
				<-ready
				report("worker", causes[i])
			})
		}
		close(ready)
		wg.Wait()
		close(releaseLog)
		synctest.Wait()
		select {
		case entry := <-fatalLogged:
			if !strings.Contains(entry, "level=ERROR") || !strings.Contains(entry, "component=store") ||
				!strings.Contains(entry, `error="`+first.Error()+`"`) {
				t.Errorf("accepted Fatal log = %s, want Error with component store and error %q", entry, first)
			}
		default:
			t.Error("accepted Fatal was not logged during shutdown")
		}
		close(releaseStop)
		err := <-result
		assertComponentError(t, err, "store", first)
		for i, cause := range causes {
			if errors.Is(err, cause) {
				t.Errorf("Run included later Fatal %d: %v", i, err)
			}
		}
		synctest.Wait()
		if !strings.Contains(log.String(), `level=ERROR msg="ignored fatal reports"`) {
			t.Errorf("ignored Fatal reports missing from Error logs: %s", &log)
		}
	})
}

func TestFatalDuringAndAfterShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var log bytes.Buffer
		runner := NewRunner(slog.New(slog.NewJSONHandler(&log, nil)))
		report := runner.Fatal()
		started := make(chan struct{})
		stopping := make(chan struct{})
		releaseStop := make(chan struct{})
		calls := make(callLog, 2)
		a := calls.component("A", func(context.Context) error {
			close(started)
			return nil
		}, func(context.Context) error {
			close(stopping)
			<-releaseStop
			return nil
		})
		result := runAsync(runner, ctx, a)
		<-started
		cancel()
		<-stopping
		during := errors.New("during shutdown")
		report("worker", during)
		close(releaseStop)
		assertComponentError(t, <-result, "worker", during)
		report("worker", errors.New("after Run"))
		synctest.Wait()
		assertLogRecord(t, &log, map[string]string{
			"level": "ERROR", "msg": "ignored fatal reports", "example_component": "worker", "example_error": "after Run",
		})
	})
}

func TestFatalFromStop(t *testing.T) {
	cause := errors.New("storage lost while stopping")
	startErr := errors.New("startup failed")
	for _, tc := range []struct {
		name      string
		component string
		async     bool
		startErr  error
		wantCalls []string
	}{
		{"synchronous", "C", false, nil, []string{"start A", "start B", "start C", "stop C", "stop B", "stop A"}},
		{"goroutine", "A", true, nil, []string{"start A", "start B", "start C", "stop C", "stop B", "stop A"}},
		{"rollback", "B", false, startErr, []string{"start A", "start B", "start C", "stop B", "stop A"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := make(callLog, 6)
			runner := testRunner(io.Discard)
			report := runner.Fatal()
			stop := func(name string) func(context.Context) error {
				if name != tc.component {
					return nil
				}
				return func(context.Context) error {
					if tc.async {
						reported := make(chan struct{})
						go func() {
							report(name, cause)
							close(reported)
						}()
						<-reported
					} else {
						report(name, cause)
					}
					return nil
				}
			}
			last := calls.component("C", func(context.Context) error {
				if tc.startErr == nil {
					cancel()
				}
				return tc.startErr
			}, stop("C"))
			err := runner.Run(ctx, calls.component("A", nil, stop("A")), calls.component("B", nil, stop("B")), last)
			assertComponentError(t, err, tc.component, cause)
			if tc.startErr != nil {
				assertComponentError(t, err, "C", tc.startErr)
			}
			calls.check(t, tc.wantCalls...)
		})
	}
}

func TestFatalFromStopAfterTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		releaseIngress := make(chan struct{})
		defer close(releaseIngress)
		const timeout = 25 * time.Millisecond
		runner := testRunner(io.Discard)
		runner.stopTimeout = timeout
		report := runner.Fatal()
		cause := errors.New("storage lost while stopping")
		calls := make(callLog, 4)
		begin := time.Now()
		store := calls.component("store", nil, func(context.Context) error {
			if elapsed := time.Since(begin); elapsed != timeout {
				t.Errorf("store Stop began after %s, want %s", elapsed, timeout)
			}
			report("store", cause)
			return nil
		})
		ingress := calls.component("ingress", func(context.Context) error {
			cancel()
			return nil
		}, func(context.Context) error {
			<-releaseIngress
			return nil
		})
		assertComponentError(t, runner.Run(ctx, store, ingress), "store", cause)
		calls.check(t, "start store", "start ingress", "stop ingress", "stop store")
	})
}

func TestRunContinuesAfterStopFailure(t *testing.T) {
	cause := errors.New("broken teardown")
	for _, tc := range []struct {
		name string
		stop func(context.Context) error
	}{
		{"error", func(context.Context) error { return cause }},
		{"panic", func(context.Context) error { panic(cause) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var log bytes.Buffer
				runner := NewRunner(slog.New(slog.NewJSONHandler(&log, nil)))
				fatal := runner.Fatal()
				primary := errors.New("primary failure")
				calls := make(callLog, 4)
				b := calls.component("B", func(context.Context) error {
					fatal("B", primary)
					return nil
				}, tc.stop)
				err := runner.Run(t.Context(), calls.component("A", nil, nil), b)
				assertComponentError(t, err, "B", primary)
				if errors.Is(err, cause) {
					t.Errorf("Run included the Stop error: %v", err)
				}
				calls.check(t, "start A", "start B", "stop B", "stop A")
				synctest.Wait()
				loggedError := cause.Error()
				if tc.name == "panic" {
					loggedError = "panic: " + loggedError
				}
				record := assertLogRecord(t, &log, map[string]string{
					"level": "ERROR", "msg": "component stop failed", "component": "B", "error": loggedError,
				})
				if tc.name == "panic" {
					assertPanicStack(t, record, "TestRunContinuesAfterStopFailure")
				}
			})
		})
	}
}

func TestRunAbandonsTimedOutStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type contextKey struct{}
		valueCtx := context.WithValue(t.Context(), contextKey{}, "value")
		ctx, cancel := context.WithCancel(valueCtx)
		defer cancel()
		unblock := make(chan struct{})
		var log bytes.Buffer
		runner := NewRunner(slog.New(slog.NewJSONHandler(&log, nil)))
		report := runner.Fatal()
		late := errors.New("fatal from abandoned Stop")
		const timeout = 25 * time.Millisecond
		runner.stopTimeout = timeout
		calls := make(callLog, 4)
		deadlines := make(chan time.Time, 2)
		checkContext := func(stopCtx context.Context) {
			if stopCtx.Err() != nil || stopCtx.Value(contextKey{}) != "value" {
				t.Errorf("Stop context = %v, want active context with Run's value", stopCtx)
			}
			deadline, ok := stopCtx.Deadline()
			if !ok {
				t.Error("Stop context has no deadline")
			}
			deadlines <- deadline
		}
		a := calls.component("A", nil, func(stopCtx context.Context) error {
			checkContext(stopCtx)
			return nil
		})
		b := calls.component("B", func(context.Context) error {
			cancel()
			return nil
		}, func(stopCtx context.Context) error {
			checkContext(stopCtx)
			<-unblock
			report("B", late)
			return nil
		})
		begin := time.Now()
		if err := runner.Run(ctx, a, b); err != nil {
			t.Errorf("Run with stuck Stop = %v, want nil", err)
		}
		elapsed := time.Since(begin)
		close(unblock)
		synctest.Wait()
		if elapsed >= 2*timeout+10*time.Millisecond {
			t.Errorf("Run took %s, want less than %s", elapsed, 2*timeout+10*time.Millisecond)
		}
		first, second := <-deadlines, <-deadlines
		if !second.After(first) {
			t.Errorf("second deadline %v must be after first deadline %v", second, first)
		}
		calls.check(t, "start A", "start B", "stop B", "stop A")
		assertLogRecord(t, &log, map[string]string{
			"level": "ERROR", "msg": "component stop timed out", "component": "B", "error": context.DeadlineExceeded.Error(),
		})
		assertLogRecord(t, &log, map[string]string{
			"level": "ERROR", "msg": "ignored fatal reports", "example_component": "B", "example_error": late.Error(),
		})
	})
}

func TestRunUsesDefaultStopTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const timeout = 10 * time.Second
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		unblock := make(chan struct{})
		defer close(unblock)
		runner := NewRunner(nil)
		calls := make(callLog, 4)
		a := calls.component("A", nil, nil)
		b := calls.component("B", func(context.Context) error {
			cancel()
			return nil
		}, func(stopCtx context.Context) error {
			deadline, ok := stopCtx.Deadline()
			if !ok {
				t.Error("Stop context has no deadline")
			} else if remaining := time.Until(deadline); remaining != timeout {
				t.Errorf("Stop context deadline is in %s, want %s", remaining, timeout)
			}
			<-unblock
			return nil
		})
		begin := time.Now()
		if err := runner.Run(ctx, a, b); err != nil {
			t.Errorf("Run with stuck Stop = %v, want nil", err)
		}
		if elapsed := time.Since(begin); elapsed != timeout {
			t.Errorf("Run took %s, want %s", elapsed, timeout)
		}
		calls.check(t, "start A", "start B", "stop B", "stop A")
	})
}

func TestRunnerRejectsInvalidRun(t *testing.T) {
	for _, scenario := range []string{"duplicate name", "second run"} {
		t.Run(scenario, func(t *testing.T) {
			calls := make(callLog, 4)
			runner := testRunner(io.Discard)
			if scenario == "duplicate name" {
				start := func(context.Context) error { return errors.New("unexpected Start") }
				a, b := calls.component("A", start, nil), calls.component("A", start, nil)
				if err := runner.Run(t.Context(), a, b); err == nil || !strings.Contains(err.Error(), `duplicate component name "A"`) {
					t.Errorf("Run with duplicate names = %v, want duplicate name error for A", err)
				}
			} else {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				a := calls.component("A", nil, nil)
				if err := runner.Run(ctx, a); err != nil {
					t.Fatalf("first Run = %v, want nil", err)
				}
				if err := runner.Run(ctx, a); err == nil || !strings.Contains(err.Error(), "only run once") {
					t.Errorf("second Run = %v, want run-once error", err)
				}
			}
			calls.check(t)
		})
	}
}

func TestRunRejectsNilComponents(t *testing.T) {
	for _, tc := range []struct {
		name      string
		component Component
	}{
		{"nil interface", nil},
		{"nil pointer", (*testComponent)(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := make(callLog, 2)
			runner := testRunner(io.Discard)
			err := runner.Run(t.Context(), calls.component("A", nil, nil), tc.component)
			const want = "component at index 1 is nil"
			if err == nil || err.Error() != want {
				t.Errorf("Run = %v, want %q", err, want)
			}
			calls.check(t)
		})
	}
}

type callLog chan string

func (calls callLog) component(name string, start, stop func(context.Context) error) Component {
	return &testComponent{name, calls, start, stop}
}

func (calls callLog) check(t *testing.T, want ...string) {
	t.Helper()
	got := make([]string, len(calls))
	for i := range got {
		got[i] = <-calls
	}
	if !slices.Equal(got, want) {
		t.Errorf("component calls = %v, want %v", got, want)
	}
}

type testComponent struct {
	name  string
	calls callLog
	start func(context.Context) error
	stop  func(context.Context) error
}

func (c *testComponent) Name() string { return c.name }

func (c *testComponent) Start(ctx context.Context) error {
	c.calls <- "start " + c.name
	if c.start != nil {
		return c.start(ctx)
	}
	return nil
}

func (c *testComponent) Stop(ctx context.Context) error {
	c.calls <- "stop " + c.name
	if c.stop != nil {
		return c.stop(ctx)
	}
	return nil
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func testRunner(w io.Writer) *Runner {
	return NewRunner(slog.New(slog.NewTextHandler(w, nil)))
}

func runAsync(runner *Runner, ctx context.Context, components ...Component) <-chan error {
	result := make(chan error, 1)
	go func() { result <- runner.Run(ctx, components...) }()
	return result
}

func assertComponentError(t *testing.T, err error, name string, cause error) {
	t.Helper()
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), `"`+name+`"`) {
		t.Fatalf("Run = %v, want component %q and wrapped error %v", err, name, cause)
	}
}

func assertLogRecord(t *testing.T, log *bytes.Buffer, want map[string]string) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(log.Bytes()))
	for {
		var record map[string]any
		err := decoder.Decode(&record)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decode log record: %v", err)
		}
		matches := true
		for key, value := range want {
			if record[key] != value {
				matches = false
				break
			}
		}
		if matches {
			return record
		}
	}
	t.Fatalf("no log record has fields %v in %s", want, log)
	return nil
}

func assertPanicStack(t *testing.T, record map[string]any, function string) {
	t.Helper()
	stack, _ := record["stack"].(string)
	if !strings.Contains(stack, function) {
		t.Errorf("panic stack missing %s in log record %v", function, record)
	}
}
