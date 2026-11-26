package sender

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
	"weak"

	"github.com/go-telegram/bot"

	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

func TestRuntimeCanceledStartReleasesContext(t *testing.T) {
	s, err := New(gateRuntimeDeps(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	startContext := &stopRuntimeProbeContext{Context: ctx}
	ref := weak.Make(startContext)
	if err := s.Start(startContext); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start = %v, want context.Canceled", err)
	}
	runtime.GC()
	if ref.Value() != nil {
		t.Error("sender retained context after unsuccessful Start")
	}
	runtime.KeepAlive(s)
}

func TestRuntimeCompletedCallbackReleasedWhileOthersRemain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := gateRuntimeDeps(t)
		deps.Workers = 2
		s := gateRuntimeStart(t, deps)
		defer gateRuntimeStop(t, s)
		respond := make(chan struct{})
		resumeResponse := sync.OnceFunc(func() { close(respond) })
		defer resumeResponse()
		trigger, _ := gateRuntimeRequest(Decision, 1, "getChat", time.Time{})
		trigger.Do = func(ctx context.Context, _ *bot.Bot) error {
			select {
			case <-respond:
				return &bot.TooManyRequestsError{RetryAfter: 1}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		gateRuntimeEnqueue(t, s, trigger)
		later, _ := gateRuntimeRequest(Decision, 2, "getChat", time.Time{})
		later.Do = func(ctx context.Context, _ *bot.Bot) error {
			select {
			case <-time.After(200 * time.Millisecond):
				return &bot.TooManyRequestsError{RetryAfter: 1}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		gateRuntimeEnqueue(t, s, later)
		synctest.Wait()

		entered, proceed := make(chan struct{}, 1), make(chan struct{})
		resumeCallbacks := sync.OnceFunc(func() { close(proceed) })
		defer resumeCallbacks()
		marker := makeStopRuntimeProbe()
		ref := weak.Make(marker)
		for _, done := range []func(Result){
			func(Result) { runtime.KeepAlive(marker) },
			func(Result) { entered <- struct{}{}; <-proceed },
			func(Result) {},
		} {
			r, _ := gateRuntimeRequest(Decision, 1, "getChat", time.Now().Add(time.Second))
			r.Done = done
			gateRuntimeEnqueue(t, s, r)
		}
		other, otherDone := gateRuntimeRequest(Decision, 2, "getChat", time.Now().Add(time.Second))
		gateRuntimeEnqueue(t, s, other)
		resumeResponse()
		workerRuntimeReceive(t, entered)
		runtime.GC()
		if ref.Value() != nil {
			t.Error("sender retained a callback after it returned while other completions were pending")
		}
		time.Sleep(200 * time.Millisecond)
		synctest.Wait()
		gateRuntimePending(t, otherDone)
		resumeCallbacks()
		synctest.Wait()
		gateRuntimeExpired(t, otherDone, 0)
		runtime.KeepAlive(s)
	})
}

func TestRuntimeReservedWorkerReleasedBeforeDoneReturns(t *testing.T) {
	for _, class := range []Class{Deadline, Decision} {
		t.Run(class.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				deps := workerRuntimeDeps(t)
				deps.Workers, deps.ReservedDeadline, deps.ReservedDecision = 3, 1, 1
				deps.CapInteractive, deps.CapBackground = 1, 1
				s := workerRuntimeStart(t, deps)
				defer workerRuntimeStop(t, s)
				blockShared := make(chan struct{})
				defer close(blockShared)
				sharedStarted := make(chan struct{}, 1)
				workerRuntimeEnqueue(t, s, workerRuntimeRequest(Background, 1, func(ctx context.Context, _ *bot.Bot) error {
					sharedStarted <- struct{}{}
					select {
					case <-blockShared:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}, func(Result) {}))
				workerRuntimeReceive(t, sharedStarted)

				allowReturn := make(chan struct{})
				endCall := sync.OnceFunc(func() { close(allowReturn) })
				defer endCall()
				allowDone := make(chan struct{})
				defer close(allowDone)
				reservedStarted, callbackEntered := make(chan struct{}, 1), make(chan struct{}, 1)
				workerRuntimeEnqueue(t, s, workerRuntimeRequest(class, 2, func(ctx context.Context, _ *bot.Bot) error {
					reservedStarted <- struct{}{}
					select {
					case <-allowReturn:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}, func(Result) {
					callbackEntered <- struct{}{}
					<-allowDone
				}))
				workerRuntimeReceive(t, reservedStarted)
				next, nextDone := gateRuntimeRequest(class, 3, "approveChatJoinRequest", time.Now().Add(time.Minute))
				workerRuntimeEnqueue(t, s, next)
				synctest.Wait()
				gateRuntimePending(t, nextDone)
				endCall()
				workerRuntimeReceive(t, callbackEntered)
				synctest.Wait()
				if got := gateRuntimeResult(t, nextDone); got.Outcome.Class != telegram.OK || got.Attempts != 1 {
					t.Errorf("result while previous callback is outstanding = %+v, want one successful attempt", got)
				}
			})
		})
	}
}

func TestRuntimeQueuedDeadlineAndWorkerLimitWhileDoneReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := gateRuntimeStart(t, gateRuntimeDeps(t))
		defer gateRuntimeStop(t, s)
		entered := make(chan struct{}, 1)
		allowDone := make(chan struct{})
		resume := sync.OnceFunc(func() { close(allowDone) })
		defer resume()
		active, _ := gateRuntimeRequest(Decision, 1, "getChat", time.Time{})
		active.Done = func(Result) {
			entered <- struct{}{}
			<-allowDone
		}
		gateRuntimeEnqueue(t, s, active)
		workerRuntimeReceive(t, entered)
		const budget = 100 * time.Millisecond
		short, shortDone := gateRuntimeRequest(Decision, 2, "getChat", time.Now().Add(budget))
		next, nextDone := gateRuntimeRequest(Decision, 3, "getChat", time.Time{})
		gateRuntimeEnqueue(t, s, short)
		gateRuntimeEnqueue(t, s, next)
		synctest.Wait()
		gateRuntimePending(t, shortDone)
		gateRuntimePending(t, nextDone)
		time.Sleep(budget)
		synctest.Wait()
		gateRuntimeExpired(t, shortDone, 0)
		gateRuntimePending(t, nextDone)
		resume()
		synctest.Wait()
		if got := gateRuntimeResult(t, nextDone); got.Outcome.Class != telegram.OK || got.Attempts != 1 {
			t.Errorf("next request = %+v, want one successful attempt", got)
		}
	})
}

func TestRuntimeAttemptCleanupAndLogs(t *testing.T) {
	for _, tc := range []struct {
		name       string
		attemptErr error
		panicDone  bool
		wantLog    string
		wantCause  string
	}{
		{"fatal attempt", errors.New("attempt failure sentinel"), false, `"level":"ERROR","msg":"sender request failed"`, `"error":"attempt failure sentinel"`},
		{"panicking callback", nil, true, `"level":"ERROR","msg":"sender Done callback panicked"`, `"panic":"callback failure sentinel"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var logs bytes.Buffer
				deps := workerRuntimeDeps(t)
				deps.Log = slog.New(slog.NewJSONHandler(&logs, nil))
				s := workerRuntimeStart(t, deps)
				defer workerRuntimeStop(t, s)
				var attempt context.Context
				workerRuntimeEnqueue(t, s, workerRuntimeRequest(Decision, 42, func(ctx context.Context, _ *bot.Bot) error {
					attempt = ctx
					return tc.attemptErr
				}, func(Result) {
					if err := attempt.Err(); !errors.Is(err, context.Canceled) {
						t.Errorf("attempt context at Done = %v, want context.Canceled", err)
					}
					if tc.panicDone {
						panic("callback failure sentinel")
					}
				}))
				synctest.Wait()
				got := logs.String()
				if strings.Count(got, "\n") != 1 || !strings.Contains(got, tc.wantLog) || !strings.Contains(got, `"method":"getChat","chat_id":42`) ||
					!strings.Contains(got, tc.wantCause) {
					t.Errorf("logs = %s, want one diagnostic for getChat in chat 42", got)
				}
			})
		})
	}
}

func TestRuntime429PauseStartsWhenResultIsProcessed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(gateRuntimeDeps(t))
		if err != nil {
			t.Fatal(err)
		}
		ready := make(chan struct{}, 1)
		permit := make(chan struct{})
		resume := sync.OnceFunc(func() { close(permit) })
		s.beforeAttemptResultLock = sync.OnceFunc(func() {
			ready <- struct{}{}
			<-permit
		})
		if err := s.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer gateRuntimeStop(t, s)
		defer resume()
		limit, limitDone := gateRuntimeRequest(Decision, 1, "getChat", time.Time{})
		limit.Do = func(context.Context, *bot.Bot) error { return &bot.TooManyRequestsError{RetryAfter: 1} }
		gateRuntimeEnqueue(t, s, limit)
		workerRuntimeReceive(t, ready)
		time.Sleep(500 * time.Millisecond)
		resume()
		synctest.Wait()
		if got := gateRuntimeResult(t, limitDone); got.Outcome.RetryAfter != time.Second {
			t.Fatalf("trigger = %+v, want 1s RetryAfter", got)
		}
		queued, done := gateRuntimeRequest(Decision, 1, "getChat", time.Time{})
		gateRuntimeEnqueue(t, s, queued)
		time.Sleep(999 * time.Millisecond)
		synctest.Wait()
		gateRuntimePending(t, done)
		time.Sleep(time.Millisecond)
		synctest.Wait()
		if got := gateRuntimeResult(t, done); got.Outcome.Class != telegram.OK || got.Attempts != 1 {
			t.Errorf("request after pause = %+v, want one successful attempt", got)
		}
	})
}
