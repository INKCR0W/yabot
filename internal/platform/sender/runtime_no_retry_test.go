package sender

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot"

	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

func TestRuntimeNoRetry429PreservesChatPause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const retryAfter = 2 * time.Second
		const chatID = -42
		deps := gateRuntimeDeps(t)
		deps.MaxRetries = 2
		deps.Metrics = metrics.NewSet()
		var logs bytes.Buffer
		deps.Log = slog.New(slog.NewJSONHandler(&logs, nil))
		s := gateRuntimeStart(t, deps)
		defer gateRuntimeStop(t, s)

		first, firstDone := gateRuntimeRequest(Background, chatID, "leaveChat", time.Time{})
		first.NoRetry = true
		limit := &bot.TooManyRequestsError{RetryAfter: int(retryAfter / time.Second)}
		firstCalls := make(chan struct{}, deps.MaxRetries+1)
		first.Do = func(context.Context, *bot.Bot) error {
			firstCalls <- struct{}{}
			return limit
		}
		started := time.Now()
		gateRuntimeEnqueue(t, s, first)
		synctest.Wait()
		want := Result{Outcome: telegram.Outcome{Class: telegram.Retry, RetryAfter: retryAfter}, Err: limit, Attempts: 1}
		if got := gateRuntimeResult(t, firstDone); got != want {
			t.Fatalf("first result = %+v, want %+v", got, want)
		}
		assertRuntimeFinalRetryObserved(t, &logs, deps.Metrics, first, retryAfter)

		second, secondDone := gateRuntimeRequest(Background, chatID, "getChat", time.Time{})
		secondCalls := make(chan time.Time, deps.MaxRetries+1)
		second.Do = func(context.Context, *bot.Bot) error {
			secondCalls <- time.Now()
			return nil
		}
		gateRuntimeEnqueue(t, s, second)
		<-time.After(retryAfter - time.Nanosecond)
		synctest.Wait()
		if len(secondCalls) != 0 {
			t.Fatalf("same-chat Do started after %v, before the %v pause ended", (<-secondCalls).Sub(started), retryAfter)
		}

		<-time.After(time.Nanosecond)
		synctest.Wait()
		if got := gateRuntimeResult(t, secondDone); got.Outcome.Class != telegram.OK || got.Dropped != 0 || got.Attempts != 1 {
			t.Errorf("same-chat result = %+v, want success on the first attempt", got)
		}
		if len(secondCalls) != 1 {
			t.Fatalf("same-chat Do calls = %d, want 1", len(secondCalls))
		}
		if elapsed := (<-secondCalls).Sub(started); elapsed != retryAfter {
			t.Errorf("same-chat Do started after %v, want %v", elapsed, retryAfter)
		}
		if len(firstCalls) != 1 || len(firstDone) != 0 {
			t.Errorf("first request called Do %d times with %d extra results", len(firstCalls), len(firstDone))
		}
	})
}

func TestRuntimeNoRetryShutdownTakesPriorityOverPauseExpiry(t *testing.T) {
	for _, class := range []Class{Interactive, Background} {
		t.Run(class.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				deps := gateRuntimeDeps(t)
				deps.MaxRetries = 2
				s := gateRuntimeStart(t, deps)
				defer gateRuntimeStop(t, s)

				r, done := gateRuntimeRequest(class, -42, "leaveChat", time.Now().Add(time.Second))
				r.NoRetry = true
				limit := &bot.TooManyRequestsError{RetryAfter: 2}
				calls := make(chan struct{}, deps.MaxRetries+1)
				r.Do = func(ctx context.Context, _ *bot.Bot) error {
					calls <- struct{}{}
					<-ctx.Done()
					return limit
				}
				gateRuntimeEnqueue(t, s, r)
				synctest.Wait()
				if len(calls) != 1 {
					t.Fatalf("Do calls before Stop = %d, want 1 in flight", len(calls))
				}
				gateRuntimePending(t, done)
				gateRuntimeStop(t, s)
				got := gateRuntimeResult(t, done)
				workerRuntimeShutdown(t, got, 1, false)
				if got.Outcome.RetryAfter != 2*time.Second || got.Err != limit {
					t.Errorf("shutdown result = %+v, want the original 429 error and retry delay", got)
				}
			})
		})
	}
}

func TestRuntimeNoRetryWaitsForPauseBeforeFirstAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const pause = 2 * time.Second
		deps := gateRuntimeDeps(t)
		deps.MaxRetries = 2
		s, err := New(deps)
		if err != nil {
			t.Fatal(err)
		}
		s.onDispatch = func(now time.Time, entry *item) {
			s.onDispatch = nil
			s.record429(now, entry.request.ChatID, entry.method, pause)
		}
		if err := s.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer gateRuntimeStop(t, s)

		r, done := gateRuntimeRequest(Background, -42, "leaveChat", time.Time{})
		r.NoRetry = true
		calls := make(chan time.Time, deps.MaxRetries+1)
		r.Do = func(context.Context, *bot.Bot) error {
			calls <- time.Now()
			return io.ErrUnexpectedEOF
		}
		started := time.Now()
		gateRuntimeEnqueue(t, s, r)
		synctest.Wait()
		gateRuntimePending(t, done)
		if len(calls) != 0 {
			t.Fatalf("Do called %d times before pause recovery", len(calls))
		}

		<-time.After(pause)
		synctest.Wait()
		want := Result{Outcome: telegram.Outcome{Class: telegram.Retry}, Err: io.ErrUnexpectedEOF, Attempts: 1, Ambiguous: true}
		if got := gateRuntimeResult(t, done); got != want {
			t.Errorf("result = %+v, want %+v", got, want)
		}
		if len(calls) != 1 {
			t.Fatalf("Do calls = %d, want 1", len(calls))
		}
		if elapsed := (<-calls).Sub(started); elapsed != pause {
			t.Errorf("Do started after %v, want %v", elapsed, pause)
		}
	})
}

func assertRuntimeFinalRetryObserved(t *testing.T, logs *bytes.Buffer, set *metrics.Set, r Request, retryAfter time.Duration) {
	t.Helper()
	const message = "sender request completed with retryable outcome"
	records := workerRuntimeLogs(t, bytes.NewReader(logs.Bytes()), message)
	if len(records) != 1 {
		t.Errorf("got %d %q logs, want 1", len(records), message)
	} else if record := records[0]; record.Level != "INFO" || record.Method != r.Method || record.ChatID != r.ChatID ||
		record.RetryAfter == nil || *record.RetryAfter != retryAfter {
		t.Errorf("log = %+v, want INFO for %s in chat %d with retry_after %v", record, r.Method, r.ChatID, retryAfter)
	}
	metric := fmt.Sprintf(`yabot_sender_requests_total{method=%q,class=%q,outcome="retry"}`, r.Method, r.Class.String())
	if got := set.GetOrCreateCounter(metric).Get(); got != 1 {
		t.Errorf("%s = %d, want 1", metric, got)
	}
}
