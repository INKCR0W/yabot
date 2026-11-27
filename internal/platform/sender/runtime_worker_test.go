package sender

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot"

	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

func workerRuntimeDeps(t *testing.T) Deps {
	t.Helper()
	b, err := bot.New("1:test", bot.WithSkipGetMe())
	if err != nil {
		t.Fatal(err)
	}
	return Deps{
		Bot: b, Workers: 16, ReservedDeadline: 6, ReservedDecision: 4,
		CapInteractive: 4, CapBackground: 4, GlobalRPS: 1_000, GlobalBurst: 32,
		ClassWeights: [4]int{8, 4, 2, 1}, ChatQueue: 32, MaxRetries: 4,
	}
}

func workerRuntimeSingle(deps *Deps) {
	deps.Workers = 1
	deps.ReservedDeadline, deps.ReservedDecision = 0, 0
	deps.CapInteractive, deps.CapBackground = 1, 1
}

func workerRuntimeStart(t *testing.T, deps Deps) *Sender {
	t.Helper()
	s, err := New(deps)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	err = s.Start(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func workerRuntimeStop(t *testing.T, s *Sender) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func workerRuntimeRequest(class Class, chatID int64, do func(context.Context, *bot.Bot) error, done func(Result)) Request {
	request := Request{Class: class, ChatID: chatID, Method: "getChat", Do: do, Done: done}
	if class == Deadline || class == Interactive {
		request.Deadline = time.Now().Add(time.Minute)
	}
	if class == Deadline {
		request.Method = "approveChatJoinRequest"
	}
	return request
}

func workerRuntimeEnqueue(t *testing.T, s *Sender, request Request) {
	t.Helper()
	if err := s.Enqueue(request); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
}

func workerRuntimeReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(time.Minute):
		t.Fatal("timed out waiting for event")
		var zero T
		return zero
	}
}

func workerRuntimeShutdown(t *testing.T, got Result, attempts int, ambiguous bool) {
	t.Helper()
	if got.Dropped != Shutdown || got.Outcome.Class != telegram.Retry || got.Attempts != attempts || got.Ambiguous != ambiguous {
		t.Fatalf("result = %+v, want Shutdown/Retry with %d attempts and ambiguity %t", got, attempts, ambiguous)
	}
}

type workerAttemptLog struct {
	Message    string         `json:"msg"`
	Level      string         `json:"level"`
	Method     string         `json:"method"`
	ChatID     int64          `json:"chat_id"`
	Attempt    int            `json:"attempt"`
	Error      string         `json:"error"`
	RetryAfter *time.Duration `json:"retry_after"`
}

func workerRuntimeLogs(t *testing.T, logs io.Reader, message string) []workerAttemptLog {
	t.Helper()
	var records []workerAttemptLog
	decoder := json.NewDecoder(logs)
	for {
		var record workerAttemptLog
		if err := decoder.Decode(&record); err == io.EOF {
			return records
		} else if err != nil {
			t.Fatalf("decode sender log: %v", err)
		}
		if record.Message == message {
			records = append(records, record)
		}
	}
}

func TestRuntimeAttemptOutcomes(t *testing.T) {
	done := fmt.Errorf("%w, HIDE_REQUESTER_MISSING", bot.ErrorBadRequest)
	forbidden := fmt.Errorf("%w, bot was blocked by the user", bot.ErrorForbidden)
	permission := fmt.Errorf("%w, CHAT_ADMIN_REQUIRED", bot.ErrorBadRequest)
	migrated := &bot.MigrateError{Message: "group migrated", MigrateToChatID: -123}
	fatal := errors.New("unexpected response")
	limit := &bot.TooManyRequestsError{Message: "rate limited", RetryAfter: 1}
	serverFailure := errors.New("error response from telegram for method getChat, 503 unavailable")
	for _, tc := range []struct {
		name      string
		errors    []error
		want      telegram.Outcome
		ambiguous bool
		retryLogs int
		noRetry   bool
	}{
		{name: "ok", errors: []error{nil}, want: telegram.Outcome{Class: telegram.OK}},
		{name: "done", errors: []error{done}, want: telegram.Outcome{Class: telegram.Done, Reason: telegram.RequestMissing}},
		{name: "forbidden", errors: []error{forbidden}, want: telegram.Outcome{Class: telegram.Forbidden, Reason: telegram.BotBlocked}},
		{name: "permission", errors: []error{permission}, want: telegram.Outcome{Class: telegram.Permission}},
		{name: "migrated", errors: []error{migrated}, want: telegram.Outcome{Class: telegram.Migrated, MigrateTo: -123}},
		{name: "fatal", errors: []error{fatal}, want: telegram.Outcome{Class: telegram.Fatal}},
		{name: "rate limited", errors: []error{limit, limit, limit}, want: telegram.Outcome{Class: telegram.Retry, RetryAfter: time.Second}},
		{name: "429 then success", errors: []error{limit, nil}, want: telegram.Outcome{Class: telegram.OK}},
		{name: "timeout then success", errors: []error{context.DeadlineExceeded, nil}, want: telegram.Outcome{Class: telegram.OK}, ambiguous: true, retryLogs: 1},
		{name: "timeout then 429 then success", errors: []error{context.DeadlineExceeded, limit, nil}, want: telegram.Outcome{Class: telegram.OK}, ambiguous: true, retryLogs: 1},
		{name: "network failure", errors: []error{io.ErrUnexpectedEOF, io.ErrUnexpectedEOF, io.ErrUnexpectedEOF}, want: telegram.Outcome{Class: telegram.Retry}, ambiguous: true, retryLogs: 2},
		{name: "server failure", errors: []error{serverFailure, serverFailure, serverFailure}, want: telegram.Outcome{Class: telegram.Retry}, ambiguous: true, retryLogs: 2},
		{name: "no retry ok", errors: []error{nil}, want: telegram.Outcome{Class: telegram.OK}, noRetry: true},
		{name: "no retry fatal", errors: []error{fatal}, want: telegram.Outcome{Class: telegram.Fatal}, noRetry: true},
		{name: "no retry network failure", errors: []error{io.ErrUnexpectedEOF}, want: telegram.Outcome{Class: telegram.Retry}, ambiguous: true, noRetry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				deps := workerRuntimeDeps(t)
				deps.MaxRetries = 2
				checkFinalRetry := tc.noRetry && tc.want.Class == telegram.Retry
				if checkFinalRetry {
					deps.Metrics = metrics.NewSet()
				}
				var logs bytes.Buffer
				deps.Log = slog.New(slog.NewJSONHandler(&logs, nil))
				s := workerRuntimeStart(t, deps)
				defer workerRuntimeStop(t, s)
				results := make(chan Result, 8)
				var calls atomic.Int32
				r := workerRuntimeRequest(Background, 1, func(_ context.Context, b *bot.Bot) error {
					if b != deps.Bot {
						t.Error("Do received a different bot")
					}
					attempt := int(calls.Add(1)) - 1
					if attempt >= len(tc.errors) {
						t.Errorf("unexpected attempt %d", attempt+1)
						return fatal
					}
					return tc.errors[attempt]
				}, func(result Result) { results <- result })
				r.NoRetry = tc.noRetry
				workerRuntimeEnqueue(t, s, r)
				got := workerRuntimeReceive(t, results)
				if got.Outcome != tc.want || got.Dropped != 0 || got.Attempts != len(tc.errors) || got.Ambiguous != tc.ambiguous || got.Err != tc.errors[len(tc.errors)-1] {
					t.Errorf("result = %+v, want outcome %+v, %d attempts, ambiguity %t and original error", got, tc.want, len(tc.errors), tc.ambiguous)
				}
				if got := int(calls.Load()); got != len(tc.errors) {
					t.Errorf("Do calls = %d, want %d", got, len(tc.errors))
				}
				synctest.Wait()
				if len(results) != 0 {
					t.Error("Done was called more than once")
				}
				if tc.want.Class == telegram.Fatal && !strings.Contains(logs.String(), `"level":"ERROR","msg":"sender request failed"`) {
					t.Errorf("fatal attempt did not log an error: %s", &logs)
				}
				if checkFinalRetry {
					assertRuntimeFinalRetryObserved(t, &logs, deps.Metrics, r, tc.want.RetryAfter)
				}
				retryLogs := workerRuntimeLogs(t, &logs, "sender request retrying")
				if len(retryLogs) != tc.retryLogs {
					t.Fatalf("got %d retry logs, want %d: %+v", len(retryLogs), tc.retryLogs, retryLogs)
				}
				for i, record := range retryLogs {
					if record.Level != "INFO" || record.Method != r.Method || record.ChatID != r.ChatID ||
						record.Attempt != i+1 || record.Error != tc.errors[i].Error() {
						t.Errorf("unexpected retry log fields: %+v", record)
					}
				}
			})
		})
	}
}

func TestRuntimeRetryBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := workerRuntimeDeps(t)
		workerRuntimeSingle(&deps)
		deps.MaxRetries = 7
		s := workerRuntimeStart(t, deps)
		defer workerRuntimeStop(t, s)
		started := make(chan time.Time, 16)
		results := make(chan Result, 2)
		workerRuntimeEnqueue(t, s, workerRuntimeRequest(Background, 1, func(context.Context, *bot.Bot) error {
			started <- time.Now()
			return context.DeadlineExceeded
		}, func(result Result) { results <- result }))
		synctest.Wait()
		first := workerRuntimeReceive(t, started)
		workerRuntimeEnqueue(t, s, workerRuntimeRequest(Background, 2, func(context.Context, *bot.Bot) error {
			started <- time.Now()
			return nil
		}, func(result Result) { results <- result }))
		if got := workerRuntimeReceive(t, started); !got.Equal(first) {
			t.Errorf("request waited for a worker during another request's backoff: %v", got.Sub(first))
		}
		if got := workerRuntimeReceive(t, results); got.Outcome.Class != telegram.OK {
			t.Fatalf("unrelated request result = %+v", got)
		}
		prev := first
		for retry, ceiling := range []time.Duration{200, 400, 800, 1_600, 3_200, 5_000, 5_000} {
			ceiling *= time.Millisecond
			next := workerRuntimeReceive(t, started)
			if got := next.Sub(prev); got < ceiling/2 || got > ceiling {
				t.Errorf("retry %d backoff = %v, want [%v, %v]", retry+1, got, ceiling/2, ceiling)
			}
			prev = next
		}
		if got := workerRuntimeReceive(t, results); got.Attempts != 1+deps.MaxRetries || got.Outcome.Class != telegram.Retry || got.Dropped != 0 {
			t.Errorf("exhausted retry result = %+v", got)
		}
	})
}

func TestRuntimeRetryStopsAtDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := workerRuntimeDeps(t)
		var logs bytes.Buffer
		deps.Log = slog.New(slog.NewJSONHandler(&logs, nil))
		s, err := New(deps)
		if err != nil {
			t.Fatal(err)
		}
		s.retryRandom = func(int64) int64 { return 0 }
		if err := s.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer workerRuntimeStop(t, s)
		results := make(chan Result, 2)
		start := time.Now()
		r := workerRuntimeRequest(Deadline, 1, func(context.Context, *bot.Bot) error {
			return context.DeadlineExceeded
		}, func(result Result) { results <- result })
		r.Deadline = start.Add(100 * time.Millisecond)
		workerRuntimeEnqueue(t, s, r)
		got := workerRuntimeReceive(t, results)
		if !time.Now().Equal(start) || got.Attempts != 1 || got.Dropped != 0 || got.Outcome.Class != telegram.Retry || !errors.Is(got.Err, context.DeadlineExceeded) {
			t.Errorf("at %v got %+v, want the first attempt's result immediately", time.Since(start), got)
		}
		synctest.Wait()
		if retryLogs := workerRuntimeLogs(t, &logs, "sender request retrying"); len(retryLogs) != 0 {
			t.Errorf("unexpected retry logs after final attempt: %+v", retryLogs)
		}
	})
}

func TestRuntimeRetryExpiresOnFinal429(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := workerRuntimeDeps(t)
		s, err := New(deps)
		if err != nil {
			t.Fatal(err)
		}
		s.retryRandom = func(int64) int64 { return 0 }
		if err := s.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer workerRuntimeStop(t, s)

		results := make(chan Result, 2)
		serverFailure := errors.New("error response from telegram for method approveChatJoinRequest, 503 unavailable")
		rateLimit := &bot.TooManyRequestsError{RetryAfter: 1}
		wantAttempts := 1 + deps.MaxRetries
		var attempts atomic.Int32
		var limitedAt time.Time
		r := workerRuntimeRequest(Deadline, 1, func(context.Context, *bot.Bot) error {
			if int(attempts.Add(1)) == wantAttempts {
				limitedAt = time.Now()
				return rateLimit
			}
			return serverFailure
		}, func(result Result) { results <- result })
		r.Deadline = time.Now().Add(2 * time.Second)
		workerRuntimeEnqueue(t, s, r)

		got := workerRuntimeReceive(t, results)
		if got.Dropped != Expired || got.Outcome.Class != telegram.Retry || got.Attempts != wantAttempts || !got.Ambiguous || got.Err != rateLimit {
			t.Fatalf("result = %+v, want Expired/Retry after %d attempts with ambiguity and the 429 error", got, wantAttempts)
		}
		if now := time.Now(); !now.Equal(limitedAt) || !now.Before(r.Deadline) {
			t.Errorf("completion at %v, want immediately at the 429 (%v) before the deadline (%v)", now, limitedAt, r.Deadline)
		}
	})
}

func TestRuntimeAttemptTimeout(t *testing.T) {
	for _, tc := range []struct {
		name      string
		class     Class
		remaining time.Duration
		want      time.Duration
	}{
		{"deadline class", Deadline, time.Minute, 3 * time.Second},
		{"deadline limited", Deadline, 2 * time.Second, 2 * time.Second},
		{"decision class", Decision, 0, 10 * time.Second},
		{"decision limited", Decision, 2 * time.Second, 2 * time.Second},
		{"interactive class", Interactive, time.Minute, 5 * time.Second},
		{"interactive limited", Interactive, time.Second, time.Second},
		{"background class", Background, 0, 10 * time.Second},
		{"background limited", Background, time.Second, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				deps := workerRuntimeDeps(t)
				deps.MaxRetries = 0
				s := workerRuntimeStart(t, deps)
				defer workerRuntimeStop(t, s)
				results := make(chan Result, 2)
				start := time.Now()
				r := workerRuntimeRequest(tc.class, 1, func(ctx context.Context, _ *bot.Bot) error {
					deadline, ok := ctx.Deadline()
					if !ok || !deadline.Equal(start.Add(tc.want)) {
						t.Errorf("Do context deadline = %v, want +%v", deadline.Sub(start), tc.want)
						return errors.New("wrong deadline")
					}
					<-ctx.Done()
					return ctx.Err()
				}, func(result Result) { results <- result })
				if tc.remaining != 0 {
					r.Deadline = start.Add(tc.remaining)
				}
				workerRuntimeEnqueue(t, s, r)
				got := workerRuntimeReceive(t, results)
				if time.Since(start) != tc.want || got.Attempts != 1 || got.Dropped != 0 || got.Outcome.Class != telegram.Retry || !got.Ambiguous || !errors.Is(got.Err, context.DeadlineExceeded) {
					t.Errorf("at %v got %+v, want one timed out attempt after %v", time.Since(start), got, tc.want)
				}
			})
		})
	}
}

func TestRuntimeWorkerQuotas(t *testing.T) {
	for _, first := range []Class{Interactive, Background} {
		t.Run(first.String()+" starts first", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := workerRuntimeStart(t, workerRuntimeDeps(t))
				release := make(chan struct{})
				defer workerRuntimeStop(t, s)
				defer close(release)
				starts := make(chan Class, 36)
				add := func(class Class, count int) {
					t.Helper()
					for i := range count {
						workerRuntimeEnqueue(t, s, workerRuntimeRequest(class, int64(i)+1, func(ctx context.Context, _ *bot.Bot) error {
							starts <- class
							select {
							case <-release:
								return nil
							case <-ctx.Done():
								return ctx.Err()
							}
						}, func(Result) {}))
					}
				}
				assertStarts := func(want map[Class]int) {
					t.Helper()
					synctest.Wait()
					got := make(map[Class]int)
					for len(starts) != 0 {
						got[<-starts]++
					}
					for class := Deadline; class <= Background; class++ {
						if got[class] != want[class] {
							t.Errorf("%s started %d more calls, want %d", class, got[class], want[class])
						}
					}
				}
				second := Background
				if first == Background {
					second = Interactive
				}
				add(first, 8)
				assertStarts(map[Class]int{first: 4})
				add(second, 8)
				assertStarts(map[Class]int{second: 2})
				add(Deadline, 8)
				add(Decision, 8)
				assertStarts(map[Class]int{Deadline: 6, Decision: 4})
			})
		})
	}
}

func TestRuntimeStopDrainsOnlyHighClasses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := workerRuntimeDeps(t)
		deps.Workers, deps.ReservedDeadline, deps.ReservedDecision = 4, 1, 1
		deps.CapInteractive, deps.CapBackground = 1, 1
		var logs bytes.Buffer
		deps.Log = slog.New(slog.NewJSONHandler(&logs, nil))
		s := workerRuntimeStart(t, deps)
		type completion struct {
			class  Class
			queued bool
			result Result
		}
		starts := make(chan Class, 8)
		results := make(chan completion, 9)
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer workerRuntimeStop(t, s)
		defer unblock()
		for class := Deadline; class <= Background; class++ {
			workerRuntimeEnqueue(t, s, workerRuntimeRequest(class, 1, func(ctx context.Context, _ *bot.Bot) error {
				starts <- class
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}, func(result Result) { results <- completion{class: class, result: result} }))
		}
		synctest.Wait()
		if got := len(starts); got != 4 {
			t.Fatalf("started %d requests, want one per class", got)
		}
		for range 4 {
			<-starts
		}
		for class := Deadline; class <= Background; class++ {
			workerRuntimeEnqueue(t, s, workerRuntimeRequest(class, 2, func(ctx context.Context, _ *bot.Bot) error {
				starts <- class
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}, func(result Result) { results <- completion{class: class, queued: true, result: result} }))
		}
		synctest.Wait()
		if len(starts) != 0 || len(results) != 0 {
			t.Fatal("queued work started despite all workers being busy")
		}
		start := time.Now()
		stopped := make(chan error, 1)
		go func() { stopped <- s.Stop(context.Background()) }()
		synctest.Wait()
		r := workerRuntimeRequest(Background, 3, func(context.Context, *bot.Bot) error { return nil }, func(Result) {
			t.Error("rejected request invoked Done")
		})
		if err := s.Enqueue(r); !errors.Is(err, ErrClosed) {
			t.Errorf("Enqueue during Stop = %v, want ErrClosed", err)
		}
		if len(results) != 4 || len(stopped) != 0 {
			t.Fatalf("at start of Stop got %d completions and %d Stop returns", len(results), len(stopped))
		}
		seen := make(map[[2]int]bool)
		for range 4 {
			got := <-results
			attempts := 1
			if got.queued {
				attempts = 0
			}
			if got.class != Interactive && got.class != Background {
				t.Fatalf("%s completed at the start of Stop", got.class)
			}
			key := [2]int{int(got.class), attempts}
			if seen[key] {
				t.Fatalf("%s completion with %d attempts repeated", got.class, attempts)
			}
			seen[key] = true
			workerRuntimeShutdown(t, got.result, attempts, !got.queued)
		}
		unblock()
		if err := workerRuntimeReceive(t, stopped); err != nil {
			t.Fatal(err)
		}
		if len(results) != 4 || len(starts) != 2 || !time.Now().Equal(start) {
			t.Fatalf("drain took %v with %d completions and %d new calls", time.Since(start), len(results), len(starts))
		}
		for range 2 {
			if class := <-starts; class != Deadline && class != Decision {
				t.Errorf("queued %s call started during Stop", class)
			}
		}
		clear(seen)
		for range 4 {
			got := <-results
			queued := 0
			if got.queued {
				queued = 1
			}
			key := [2]int{int(got.class), queued}
			if (got.class != Deadline && got.class != Decision) || seen[key] || got.result.Outcome.Class != telegram.OK || got.result.Attempts != 1 || got.result.Dropped != 0 {
				t.Errorf("unexpected drain completion: %+v", got)
			}
			seen[key] = true
		}
		if err := s.Start(context.Background()); err == nil {
			t.Error("Start succeeded after Stop")
		}
		if err := s.Enqueue(r); !errors.Is(err, ErrClosed) {
			t.Errorf("Enqueue after Stop = %v, want ErrClosed", err)
		}
		if retryLogs := workerRuntimeLogs(t, &logs, "sender request retrying"); len(retryLogs) != 0 {
			t.Errorf("unexpected retry logs during shutdown: %+v", retryLogs)
		}
	})
}

func TestRuntimeStopWithRetryPending(t *testing.T) {
	for _, class := range []Class{Deadline, Decision, Interactive, Background} {
		t.Run(class.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				deps := workerRuntimeDeps(t)
				deps.MaxRetries = 1
				s := workerRuntimeStart(t, deps)
				results := make(chan Result, 2)
				var calls atomic.Int32
				workerRuntimeEnqueue(t, s, workerRuntimeRequest(class, 1, func(context.Context, *bot.Bot) error {
					if calls.Add(1) == 1 {
						return context.DeadlineExceeded
					}
					return nil
				}, func(result Result) { results <- result }))
				synctest.Wait()
				if calls.Load() != 1 || len(results) != 0 {
					t.Errorf("expected one attempt and a waiting retry, got %d attempts and %d results", calls.Load(), len(results))
				}
				startedStop := time.Now()
				workerRuntimeStop(t, s)
				got := workerRuntimeReceive(t, results)
				switch class {
				case Deadline, Decision:
					if got.Outcome.Class != telegram.OK || got.Attempts != 2 || got.Dropped != 0 || !got.Ambiguous {
						t.Errorf("drained retry result = %+v", got)
					}
					if elapsed := time.Since(startedStop); elapsed < 100*time.Millisecond || elapsed > 200*time.Millisecond {
						t.Errorf("drained retry after %v, want 100–200 ms", elapsed)
					}
				default:
					workerRuntimeShutdown(t, got, 1, true)
					if !time.Now().Equal(startedStop) {
						t.Errorf("discarding retry took %v", time.Since(startedStop))
					}
				}
				if len(results) != 0 || int(calls.Load()) != got.Attempts {
					t.Errorf("got %d calls and %d extra results for %+v", calls.Load(), len(results), got)
				}
			})
		})
	}
}

func TestRuntimeStopAtGraceLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := workerRuntimeDeps(t)
		workerRuntimeSingle(&deps)
		s := workerRuntimeStart(t, deps)
		started := make(chan struct{}, 1)
		active, queued := make(chan Result, 2), make(chan Result, 2)
		workerRuntimeEnqueue(t, s, workerRuntimeRequest(Decision, 1, func(ctx context.Context, _ *bot.Bot) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}, func(result Result) { active <- result }))
		workerRuntimeReceive(t, started)
		workerRuntimeEnqueue(t, s, workerRuntimeRequest(Deadline, 1, func(context.Context, *bot.Bot) error {
			t.Error("queued request started when the grace period ended")
			return nil
		}, func(result Result) { queued <- result }))
		start := time.Now()
		workerRuntimeStop(t, s)
		if got := time.Since(start); got != 5*time.Second {
			t.Errorf("Stop took %v, want 5s", got)
		}
		workerRuntimeShutdown(t, workerRuntimeReceive(t, active), 1, true)
		workerRuntimeShutdown(t, workerRuntimeReceive(t, queued), 0, false)
	})
}

func TestRuntimeStopContextExpiresBeforeWorkerReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := workerRuntimeStart(t, workerRuntimeDeps(t))
		started, canceled, release := make(chan struct{}, 1), make(chan struct{}, 1), make(chan struct{})
		results := make(chan Result, 2)
		unblock := sync.OnceFunc(func() { close(release) })
		defer workerRuntimeStop(t, s)
		defer unblock()
		workerRuntimeEnqueue(t, s, workerRuntimeRequest(Decision, 1, func(ctx context.Context, _ *bot.Bot) error {
			started <- struct{}{}
			<-ctx.Done()
			canceled <- struct{}{}
			<-release
			return ctx.Err()
		}, func(result Result) { results <- result }))
		workerRuntimeReceive(t, started)
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		if err := s.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 500*time.Millisecond {
			t.Fatalf("Stop at %v = %v, want deadline exceeded after 500 ms", time.Since(start), err)
		}
		workerRuntimeReceive(t, canceled)
		if len(results) != 0 {
			t.Fatal("in-flight Done called before Do returned")
		}
		unblock()
		workerRuntimeStop(t, s)
		workerRuntimeShutdown(t, workerRuntimeReceive(t, results), 1, true)
		if len(results) != 0 {
			t.Error("Done called more than once")
		}
	})
}

func TestRuntimeConcurrentStopUsesEarlierDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := workerRuntimeStart(t, workerRuntimeDeps(t))
		started := make(chan struct{})
		results := make(chan Result, 1)
		workerRuntimeEnqueue(t, s, workerRuntimeRequest(Decision, 1, func(ctx context.Context, _ *bot.Bot) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}, func(result Result) { results <- result }))
		<-started

		first := make(chan error, 1)
		go func() { first <- s.Stop(context.Background()) }()
		synctest.Wait()
		began := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Stop with earlier deadline = %v, want DeadlineExceeded", err)
		}
		if err := <-first; err != nil {
			t.Errorf("Stop without a deadline = %v, want nil", err)
		}
		if elapsed := time.Since(began); elapsed != time.Second {
			t.Errorf("stopped after %v, want 1s", elapsed)
		}
		workerRuntimeShutdown(t, <-results, 1, true)
	})
}

func TestRuntimeStopWaitsForDone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := workerRuntimeStart(t, workerRuntimeDeps(t))
		entered, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer workerRuntimeStop(t, s)
		defer unblock()
		workerRuntimeEnqueue(t, s, workerRuntimeRequest(Decision, 1, func(context.Context, *bot.Bot) error {
			return nil
		}, func(Result) {
			close(entered)
			<-release
		}))
		workerRuntimeReceive(t, entered)
		stopped := make(chan error, 1)
		go func() { stopped <- s.Stop(context.Background()) }()
		synctest.Wait()
		if len(stopped) != 0 {
			t.Fatal("Stop returned before Done returned")
		}
		unblock()
		if err := workerRuntimeReceive(t, stopped); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRuntimeWorkerCallbackExit(t *testing.T) {
	for _, tc := range []struct {
		name          string
		doExit        func()
		doneExit      func()
		wantErrorPart string
		callbackPanic uint64
	}{
		{name: "Do panic", doExit: func() { panic("attempt panic sentinel") }, wantErrorPart: "attempt panic sentinel"},
		{name: "Do Goexit", doExit: runtime.Goexit},
		{name: "Done panic", doneExit: func() { panic("callback panic sentinel") }, callbackPanic: 1},
		{name: "Done Goexit", doneExit: runtime.Goexit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				deps := workerRuntimeDeps(t)
				workerRuntimeSingle(&deps)
				deps.Metrics = metrics.NewSet()
				s := workerRuntimeStart(t, deps)
				defer workerRuntimeStop(t, s)
				first, next := make(chan Result, 8), make(chan Result, 8)
				workerRuntimeEnqueue(t, s, workerRuntimeRequest(Decision, 1, func(context.Context, *bot.Bot) error {
					if tc.doExit != nil {
						tc.doExit()
					}
					return nil
				}, func(result Result) {
					first <- result
					if tc.doneExit != nil {
						tc.doneExit()
					}
				}))
				got := workerRuntimeReceive(t, first)
				if tc.doExit != nil {
					if got.Outcome.Class != telegram.Fatal || got.Err == nil || !strings.Contains(strings.ToLower(got.Err.Error()), tc.wantErrorPart) {
						t.Errorf("first result = %+v, want Fatal with error mentioning %q", got, tc.wantErrorPart)
					}
				} else if got.Outcome.Class != telegram.OK {
					t.Errorf("first result = %+v, want OK", got)
				}
				if got.Attempts != 1 || got.Dropped != 0 || got.Ambiguous {
					t.Errorf("unexpected first result: %+v", got)
				}
				workerRuntimeEnqueue(t, s, workerRuntimeRequest(Decision, 2, func(context.Context, *bot.Bot) error {
					return nil
				}, func(result Result) { next <- result }))
				if got := workerRuntimeReceive(t, next); got.Outcome.Class != telegram.OK || got.Attempts != 1 {
					t.Errorf("follow-up result = %+v, want one successful attempt", got)
				}
				workerRuntimeStop(t, s)
				if len(first) != 0 || len(next) != 0 {
					t.Error("unexpected duplicate Done after Stop")
				}
				if got := deps.Metrics.GetOrCreateCounter("yabot_sender_callback_panics_total").Get(); got != tc.callbackPanic {
					t.Errorf("callback panics = %d, want %d", got, tc.callbackPanic)
				}
			})
		})
	}
}

func TestRuntimeDoneCanEnqueue(t *testing.T) {
	for _, tc := range []struct {
		name string
		drop DropReason
	}{
		{name: "worker completion"},
		{name: "expired in queue", drop: Expired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				deps := workerRuntimeDeps(t)
				workerRuntimeSingle(&deps)
				s := workerRuntimeStart(t, deps)
				defer workerRuntimeStop(t, s)
				results, next := make(chan Result, 2), make(chan Result, 2)
				enqueued := make(chan error, 2)
				followUp := workerRuntimeRequest(Background, 2, func(context.Context, *bot.Bot) error { return nil }, func(result Result) {
					next <- result
				})
				r := workerRuntimeRequest(Background, 1, func(context.Context, *bot.Bot) error {
					if tc.drop == Expired {
						t.Error("expired queued request called Do")
					}
					return nil
				}, func(result Result) {
					enqueued <- s.Enqueue(followUp)
					results <- result
				})
				if tc.drop == Expired {
					release := workerRuntimeOccupy(t, s)
					defer release()
					r.Deadline = time.Now().Add(10 * time.Millisecond)
					workerRuntimeEnqueue(t, s, r)
					time.Sleep(10 * time.Millisecond)
					release()
				} else {
					workerRuntimeEnqueue(t, s, r)
				}
				if err := workerRuntimeReceive(t, enqueued); err != nil {
					t.Fatalf("Enqueue inside Done: %v", err)
				}
				if got := workerRuntimeReceive(t, results); got.Dropped != tc.drop {
					t.Errorf("first result = %+v, want drop reason %s", got, tc.drop)
				}
				if got := workerRuntimeReceive(t, next); got.Outcome.Class != telegram.OK || got.Attempts != 1 || got.Dropped != 0 {
					t.Errorf("follow-up result = %+v, want one successful attempt", got)
				}
			})
		})
	}
}

func TestRuntimeQueueCallbackGoexit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := workerRuntimeDeps(t)
		workerRuntimeSingle(&deps)
		s := workerRuntimeStart(t, deps)
		defer workerRuntimeStop(t, s)
		release := workerRuntimeOccupy(t, s)
		defer release()
		results := make(chan Result, 4)
		for i := range 3 {
			r := workerRuntimeRequest(Background, 1, func(context.Context, *bot.Bot) error {
				if i < 2 {
					t.Error("expired queued request called Do")
				}
				return nil
			}, func(result Result) {
				results <- result
				if i == 0 {
					runtime.Goexit()
				}
			})
			if i < 2 {
				r.Deadline = time.Now().Add(10 * time.Millisecond)
			}
			workerRuntimeEnqueue(t, s, r)
		}
		time.Sleep(10 * time.Millisecond)
		release()
		counts := make(map[DropReason]int)
		for range 3 {
			got := workerRuntimeReceive(t, results)
			counts[got.Dropped]++
		}
		workerRuntimeStop(t, s)
		if counts[Expired] != 2 || counts[0] != 1 || len(results) != 0 {
			t.Errorf("got completions %v and %d extras, want two expirations and one success", counts, len(results))
		}
	})
}

func workerRuntimeOccupy(t *testing.T, s *Sender) func() {
	t.Helper()
	started, release := make(chan struct{}, 1), make(chan struct{})
	workerRuntimeEnqueue(t, s, workerRuntimeRequest(Decision, 3, func(ctx context.Context, _ *bot.Bot) error {
		started <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, func(Result) {}))
	workerRuntimeReceive(t, started)
	return sync.OnceFunc(func() { close(release) })
}
