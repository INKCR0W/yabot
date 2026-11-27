package sender

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot"

	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

type recoveryStopContext struct {
	context.Context
	checks int
}

func (ctx *recoveryStopContext) Err() error {
	ctx.checks++
	if ctx.checks == 2 {
		// Let the pause expire after the scheduler has sampled its clock.
		time.Sleep(time.Second)
	}
	return ctx.Context.Err()
}

func TestSenderRuntimeChatRecoveryAcrossClockSample(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := gateRuntimeStart(t, gateRuntimeDeps(t))
		defer gateRuntimeStop(t, s)
		r, done := gateRuntimeRequest(Decision, 1, "getChat", time.Time{})
		r.Do = func(context.Context, *bot.Bot) error { return &bot.TooManyRequestsError{RetryAfter: 1} }
		gateRuntimeEnqueue(t, s, r)
		synctest.Wait()
		gateRuntimeResult(t, done)
		queued, queuedDone := gateRuntimeRequest(Decision, 1, "getChat", time.Time{})
		gateRuntimeEnqueue(t, s, queued)
		synctest.Wait()
		gateRuntimePending(t, queuedDone)
		started := time.Now()
		if err := s.Stop(&recoveryStopContext{Context: t.Context()}); err != nil {
			t.Fatal(err)
		}
		if got := gateRuntimeResult(t, queuedDone); got.Dropped != 0 || got.Outcome.Class != telegram.OK || time.Since(started) != time.Second {
			t.Errorf("paused request = %+v after %v, want success at recovery", got, time.Since(started))
		}
	})
}

type pauseLogWriter struct {
	bytes.Buffer
	entered chan struct{}
	resume  chan struct{}
}

func (w *pauseLogWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("sender entered global pause")) {
		select {
		case <-w.entered:
		default:
			close(w.entered)
			<-w.resume
		}
	}
	return w.Buffer.Write(p)
}

func TestSenderRuntimeLogsRecoveryBeforeRenewedGlobalPause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writer := &pauseLogWriter{entered: make(chan struct{}), resume: make(chan struct{})}
		deps := gateRuntimeDeps(t)
		deps.Workers = 2
		deps.Log = slog.New(slog.NewJSONHandler(writer, nil))
		deps.Metrics = metrics.NewSet()
		s := gateRuntimeStart(t, deps)
		defer gateRuntimeStop(t, s)
		resumeLogs := sync.OnceFunc(func() { close(writer.resume) })
		defer resumeLogs()
		pending, pendingDone := gateRuntimeRequest(Decision, 1, "getChat", time.Time{})
		pending.Do = func(ctx context.Context, _ *bot.Bot) error {
			select {
			case <-time.After(time.Second):
				return &bot.TooManyRequestsError{RetryAfter: 31}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		gateRuntimeEnqueue(t, s, pending)
		synctest.Wait()
		for _, method := range []string{"getChat", "sendMessage", "approveChatJoinRequest"} {
			r, done := gateRuntimeRequest(Decision, 0, method, time.Time{})
			r.Do = func(context.Context, *bot.Bot) error { return &bot.TooManyRequestsError{RetryAfter: 1} }
			gateRuntimeEnqueue(t, s, r)
			synctest.Wait()
			if got := gateRuntimeResult(t, done); got.Outcome.RetryAfter != time.Second {
				t.Fatalf("%s trigger = %+v, want 1s RetryAfter", method, got)
			}
		}
		workerRuntimeReceive(t, writer.entered)
		time.Sleep(time.Second)
		synctest.Wait()
		if got := gateRuntimeResult(t, pendingDone); got.Outcome.RetryAfter != 31*time.Second {
			t.Fatalf("renewing 429 = %+v, want 31s RetryAfter", got)
		}
		if got := deps.Metrics.GetOrCreateGauge(`yabot_sender_gate_paused{scope="global"}`, nil).Get(); got != 1 {
			t.Errorf("global pause gauge = %v while scheduler is in the logger, want 1", got)
		}
		resumeLogs()
		synctest.Wait()
		entered := `"msg":"sender entered global pause"`
		left := `"msg":"sender left global pause"`
		logs := writer.String()
		if strings.Count(logs, entered) != 2 || strings.Count(logs, left) != 1 || strings.Index(logs, left) > strings.LastIndex(logs, entered) {
			t.Errorf("global pause logs = %s, want enter, leave, enter", logs)
		}
	})
}

func TestSenderRuntimeStopAppliesCutoffWithSchedulerInLogger(t *testing.T) {
	for _, name := range []string{"canceled context", "elapsed deadline"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				writer := &pauseLogWriter{entered: make(chan struct{}), resume: make(chan struct{})}
				deps := gateRuntimeDeps(t)
				deps.Workers = 2
				deps.Log = slog.New(slog.NewJSONHandler(writer, nil))
				s := gateRuntimeStart(t, deps)
				defer gateRuntimeStop(t, s)
				resumeLogs := sync.OnceFunc(func() { close(writer.resume) })
				defer resumeLogs()
				started := make(chan context.Context, 1)
				r, done := gateRuntimeRequest(Decision, 1, "getChat", time.Time{})
				r.Do = func(ctx context.Context, _ *bot.Bot) error {
					started <- ctx
					<-ctx.Done()
					return ctx.Err()
				}
				gateRuntimeEnqueue(t, s, r)
				attempt := workerRuntimeReceive(t, started)
				trigger, _ := gateRuntimeRequest(Decision, 2, "getChat", time.Time{})
				trigger.Do = func(context.Context, *bot.Bot) error { return &bot.TooManyRequestsError{RetryAfter: 31} }
				gateRuntimeEnqueue(t, s, trigger)
				workerRuntimeReceive(t, writer.entered)
				parent, cancel := context.WithCancel(t.Context())
				defer cancel()
				var stopContext context.Context = parent
				var wantErr error = context.Canceled
				if name == "elapsed deadline" {
					stopContext = stopRuntimeDeadlineContext{Context: parent, deadline: time.Now()}
					wantErr = context.DeadlineExceeded
				} else {
					cancel()
				}
				stop := stopRuntimeDelayedDoneContext{Context: stopContext, enters: make(chan struct{}), resume: make(chan struct{})}
				stopped := make(chan error, 1)
				go func() { stopped <- s.Stop(stop) }()
				workerRuntimeReceive(t, stop.enters)
				if err := attempt.Err(); !errors.Is(err, context.Canceled) {
					t.Errorf("in-flight context = %v at Stop cutoff, want context.Canceled", err)
				}
				close(stop.resume)
				resumeLogs()
				if err := workerRuntimeReceive(t, stopped); !errors.Is(err, wantErr) {
					t.Errorf("Stop = %v, want %v", err, wantErr)
				}
				workerRuntimeShutdown(t, workerRuntimeReceive(t, done), 1, true)
			})
		})
	}
}

func TestSenderRuntimeDoesNotWakeForTokensWithNoBacklog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := gateRuntimeDeps(t)
		deps.GlobalBurst, deps.GlobalRPS = 1, 1
		s := gateRuntimeStart(t, deps)
		defer gateRuntimeStop(t, s)
		r, done := gateRuntimeRequest(Decision, 1, "getChat", time.Time{})
		gateRuntimeEnqueue(t, s, r)
		synctest.Wait()
		gateRuntimeResult(t, done)
		s.mu.Lock()
		loops := s.loops
		s.mu.Unlock()
		time.Sleep(time.Second)
		synctest.Wait()
		s.mu.Lock()
		after := s.loops
		s.mu.Unlock()
		if after != loops {
			t.Errorf("idle scheduler woke %d times while the bucket refilled", after-loops)
		}
	})
}

func TestSenderRuntimeOrdinaryRetryExpiresDuringNewPause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := gateRuntimeDeps(t)
		deps.MaxRetries = 1
		s := gateRuntimeStart(t, deps)
		defer gateRuntimeStop(t, s)
		first, firstDone := gateRuntimeRequest(Decision, 1, "getChat", time.Now().Add(500*time.Millisecond))
		first.Do = func(context.Context, *bot.Bot) error { return context.DeadlineExceeded }
		gateRuntimeEnqueue(t, s, first)
		synctest.Wait()
		gateRuntimePending(t, firstDone)

		trigger, triggerDone := gateRuntimeRequest(Decision, 1, "getChat", time.Time{})
		trigger.Do = func(context.Context, *bot.Bot) error { return &bot.TooManyRequestsError{RetryAfter: 1} }
		gateRuntimeEnqueue(t, s, trigger)
		synctest.Wait()
		gateRuntimePending(t, triggerDone)
		time.Sleep(200 * time.Millisecond)
		synctest.Wait()
		got := gateRuntimeResult(t, firstDone)
		if got.Dropped != Expired || got.Attempts != 1 || got.Outcome.Class != telegram.Retry || !got.Ambiguous {
			t.Errorf("backing-off request = %+v, want ambiguous expiry after one attempt", got)
		}
	})
}

func TestSenderRuntimeOverdueRetryExpiresWithoutAddingToBacklog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := gateRuntimeDeps(t)
		deps.MaxRetries, deps.GlobalBurst, deps.GlobalRPS = 1, 2, 0.1
		s := gateRuntimeStart(t, deps)
		defer gateRuntimeStop(t, s)
		first, firstDone := gateRuntimeRequest(Decision, 1, "approveChatJoinRequest", time.Now().Add(500*time.Millisecond))
		first.Do = func(context.Context, *bot.Bot) error { return context.DeadlineExceeded }
		gateRuntimeEnqueue(t, s, first)
		synctest.Wait()
		gateRuntimePending(t, firstDone)

		trigger, _ := gateRuntimeRequest(Decision, 0, "getChat", time.Time{})
		trigger.Do = func(context.Context, *bot.Bot) error { return &bot.TooManyRequestsError{RetryAfter: 1} }
		gateRuntimeEnqueue(t, s, trigger)
		synctest.Wait()
		entered := make(chan struct{}, 1)
		allowDone := make(chan struct{})
		resume := sync.OnceFunc(func() { close(allowDone) })
		defer resume()
		admitted, _ := gateRuntimeRequest(Interactive, 2, "getChat", time.Now().Add(time.Second))
		admitted.Done = func(result Result) {
			if result.Dropped != Expired || result.Attempts != 0 {
				t.Errorf("request blocked on admission = %+v, want expiry", result)
			}
			entered <- struct{}{}
			<-allowDone
		}
		gateRuntimeEnqueue(t, s, admitted)
		workerRuntimeReceive(t, entered)
		time.Sleep(500 * time.Millisecond)
		resume()
		synctest.Wait()
		got := gateRuntimeResult(t, firstDone)
		if got.Dropped != Expired || got.Attempts != 1 || got.Outcome.Class != telegram.Retry || !got.Ambiguous {
			t.Errorf("overdue retry = %+v, want ambiguous expiry without waiting for a token", got)
		}
	})
}

func TestSenderRuntimeOnlyScansNewOrExtendedPauseScopes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := gateRuntimeDeps(t)
		deps.Workers = 2
		s := gateRuntimeStart(t, deps)
		defer gateRuntimeStop(t, s)
		respond := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
		for i, delay := range []int{2, 1} {
			r, _ := gateRuntimeRequest(Decision, 1, "getChat", time.Time{})
			r.Do = func(ctx context.Context, _ *bot.Bot) error {
				select {
				case <-respond[i]:
					return &bot.TooManyRequestsError{RetryAfter: delay}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			gateRuntimeEnqueue(t, s, r)
		}
		synctest.Wait()
		r, done := gateRuntimeRequest(Background, 2, "getChat", time.Time{})
		gateRuntimeEnqueue(t, s, r)
		close(respond[0])
		synctest.Wait()
		gateRuntimeResult(t, done)
		s.mu.Lock()
		first, full := s.pauseScans, s.fullPauseScans
		s.mu.Unlock()
		if first == 0 || full != 0 {
			t.Errorf("chat-only pause used %d scans, %d class-wide, want only chat scans", first, full)
		}
		close(respond[1])
		synctest.Wait()
		s.mu.Lock()
		all, full := s.pauseScans, s.fullPauseScans
		s.mu.Unlock()
		if all != first || full != 0 {
			t.Errorf("non-extending 429 used %d additional scans, %d class-wide", all-first, full)
		}
	})
}

func TestSenderRuntimeDoesNotRescanUnchangedMethodPause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := gateRuntimeDeps(t)
		deps.Workers = 2
		s := gateRuntimeStart(t, deps)
		defer gateRuntimeStop(t, s)

		respond := make(chan struct{})
		pending, pendingDone := gateRuntimeRequest(Decision, 1, "getChat", time.Time{})
		pending.Do = func(ctx context.Context, _ *bot.Bot) error {
			select {
			case <-respond:
				return &bot.TooManyRequestsError{RetryAfter: 1}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		gateRuntimeEnqueue(t, s, pending)
		synctest.Wait()
		trigger, triggerDone := gateRuntimeRequest(Decision, 0, "getChat", time.Time{})
		trigger.Do = func(context.Context, *bot.Bot) error { return &bot.TooManyRequestsError{RetryAfter: 2} }
		gateRuntimeEnqueue(t, s, trigger)
		synctest.Wait()
		gateRuntimeResult(t, triggerDone)
		s.mu.Lock()
		before := s.fullPauseScans
		s.mu.Unlock()
		if before == 0 {
			t.Fatal("new method pause did not scan all affected queues")
		}

		close(respond)
		synctest.Wait()
		if got := gateRuntimeResult(t, pendingDone); got.Outcome.RetryAfter != time.Second {
			t.Fatalf("in-flight result = %+v, want 1s RetryAfter", got)
		}
		if got := s.EstimateWait(Decision, 2); got != 2*time.Second {
			t.Errorf("method pause after shorter 429 = %v, want 2s", got)
		}
		s.mu.Lock()
		after := s.fullPauseScans
		s.mu.Unlock()
		if after != before {
			t.Errorf("unchanged method pause triggered %d additional class-wide scans", after-before)
		}
	})
}

func TestSenderRuntimeGlobalOnlyExtensionExpiresQueuedWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := gateRuntimeDeps(t)
		deps.Workers = 4
		s := gateRuntimeStart(t, deps)
		defer gateRuntimeStop(t, s)
		began := time.Now()
		var respond []chan struct{}
		for i, delay := range []int{40, 1, 1, 38} {
			ch := make(chan struct{})
			respond = append(respond, ch)
			r, _ := gateRuntimeRequest(Decision, int64((i+1)%4), "sendMessage", time.Time{})
			r.Do = func(ctx context.Context, _ *bot.Bot) error {
				select {
				case <-ch:
					return &bot.TooManyRequestsError{RetryAfter: delay}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			gateRuntimeEnqueue(t, s, r)
		}
		synctest.Wait()
		for _, ch := range respond[:3] {
			close(ch)
			synctest.Wait()
			time.Sleep(time.Second)
		}
		// The method pause lasts until 42s and the global pause until 40s.
		r, done := gateRuntimeRequest(Background, 9, "getChat", began.Add(40*time.Second+500*time.Millisecond))
		gateRuntimeEnqueue(t, s, r)
		synctest.Wait()
		gateRuntimePending(t, done)
		close(respond[3])
		synctest.Wait()
		gateRuntimeExpired(t, done, 0)
		s.mu.Lock()
		full := s.fullPauseScans
		s.mu.Unlock()
		if full == 0 {
			t.Error("global pause did not scan all affected queues")
		}
	})
}
