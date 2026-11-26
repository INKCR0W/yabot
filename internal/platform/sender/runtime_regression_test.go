package sender

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-telegram/bot"

	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

func TestRuntimeReservedClassesUseSharedWorkers(t *testing.T) {
	for _, tc := range []struct {
		class    Class
		reserved int
	}{
		{Deadline, 6},
		{Decision, 4},
	} {
		t.Run(tc.class.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				deps := workerRuntimeDeps(t)
				deps.GlobalRPS, deps.GlobalBurst, deps.ChatQueue = 25, 5, 256
				s := workerRuntimeStart(t, deps)
				defer workerRuntimeStop(t, s)
				release := make(chan struct{})
				defer close(release)
				started := make(chan time.Time, tc.reserved+1)
				add := func(chat int64) {
					workerRuntimeEnqueue(t, s, workerRuntimeRequest(tc.class, chat, func(ctx context.Context, _ *bot.Bot) error {
						started <- time.Now()
						select {
						case <-release:
							return nil
						case <-ctx.Done():
							return ctx.Err()
						}
					}, func(Result) {}))
				}
				for i := range tc.reserved {
					add(int64(i + 1))
					workerRuntimeReceive(t, started)
				}
				time.Sleep(40 * time.Millisecond)
				admitted := time.Now()
				add(int64(tc.reserved + 1))
				synctest.Wait()
				select {
				case at := <-started:
					if !at.Equal(admitted) {
						t.Errorf("shared worker started after %v, want immediate start", at.Sub(admitted))
					}
				default:
					t.Fatalf("%s request %d did not start with a shared worker available", tc.class, tc.reserved+1)
				}
			})
		})
	}
}

func TestRuntimeDecisionCompletionsFreeSharedWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const firstCount, remainingCount = 4, 6
		s := workerRuntimeStart(t, workerRuntimeDeps(t))
		defer workerRuntimeStop(t, s)
		firstRelease, remainingRelease := make(chan struct{}), make(chan struct{})
		finishFirst := sync.OnceFunc(func() { close(firstRelease) })
		defer finishFirst()
		defer close(remainingRelease)
		started := make(chan struct{}, firstCount+remainingCount)
		firstDone := make(chan Result, firstCount)
		addDecision := func(chat int64, release <-chan struct{}, done func(Result)) {
			workerRuntimeEnqueue(t, s, workerRuntimeRequest(Decision, chat, func(ctx context.Context, _ *bot.Bot) error {
				started <- struct{}{}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}, done))
		}
		for i := range firstCount {
			addDecision(int64(i+1), firstRelease, func(r Result) { firstDone <- r })
		}
		synctest.Wait()
		if got := len(started); got != firstCount {
			t.Fatalf("started %d Decision calls, want %d before adding more", got, firstCount)
		}
		for i := range remainingCount {
			addDecision(int64(firstCount+i+1), remainingRelease, func(Result) {})
		}
		synctest.Wait()
		if got := len(started); got != firstCount+remainingCount {
			t.Fatalf("started %d Decision calls, want %d before releasing the first calls", got, firstCount+remainingCount)
		}

		finishFirst()
		synctest.Wait()
		for range firstCount {
			if got := gateRuntimeResult(t, firstDone); got.Outcome.Class != telegram.OK || got.Attempts != 1 {
				t.Fatalf("first Decision result = %+v, want one successful attempt", got)
			}
		}
		admitted := time.Now()
		interactive, done := gateRuntimeRequest(Interactive, firstCount+remainingCount+1, "getChatMember", admitted.Add(time.Second))
		gateRuntimeEnqueue(t, s, interactive)
		got := workerRuntimeReceive(t, done)
		if elapsed := time.Since(admitted); elapsed != 0 || got.Dropped != 0 || got.Outcome.Class != telegram.OK || got.Attempts != 1 {
			t.Fatalf("Interactive result after %v = %+v, want immediate success in one attempt", elapsed, got)
		}
	})
}

func TestRuntimeRetryJitterEndpoints(t *testing.T) {
	for _, tc := range []struct {
		name   string
		random func(int64) int64
		factor time.Duration
	}{
		{"minimum", func(int64) int64 { return 0 }, 2},
		{"maximum", func(n int64) int64 { return n - 1 }, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				deps := workerRuntimeDeps(t)
				deps.MaxRetries = 7
				s, err := New(deps)
				if err != nil {
					t.Fatal(err)
				}
				s.retryRandom = tc.random
				if err := s.Start(t.Context()); err != nil {
					t.Fatal(err)
				}
				defer workerRuntimeStop(t, s)
				started := make(chan time.Time, 1+deps.MaxRetries)
				done := make(chan Result, 1)
				workerRuntimeEnqueue(t, s, workerRuntimeRequest(Background, 1, func(context.Context, *bot.Bot) error {
					started <- time.Now()
					return context.DeadlineExceeded
				}, func(r Result) { done <- r }))
				prev := workerRuntimeReceive(t, started)
				for i, ceilingMS := range []time.Duration{200, 400, 800, 1600, 3200, 5000, 5000} {
					next := workerRuntimeReceive(t, started)
					want := ceilingMS * time.Millisecond / tc.factor
					if got := next.Sub(prev); got != want {
						t.Errorf("retry %d delay = %v, want %v", i+1, got, want)
					}
					prev = next
				}
				if got := workerRuntimeReceive(t, done); got.Attempts != 1+deps.MaxRetries || got.Dropped != 0 || got.Outcome.Class != telegram.Retry {
					t.Errorf("result = %+v, want exhausted retries", got)
				}
			})
		})
	}
}

func TestRuntimeOrdinaryRetryReturnsToQueueHead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := gateRuntimeDeps(t)
		deps.MaxRetries, deps.GlobalRPS, deps.GlobalBurst = 1, 1, 1
		s := gateRuntimeStart(t, deps)
		defer gateRuntimeStop(t, s)
		first, firstDone := gateRuntimeRequest(Decision, 4, "getChat", time.Time{})
		attempts := 0
		first.Do = func(context.Context, *bot.Bot) error {
			attempts++
			if attempts == 1 {
				return context.DeadlineExceeded
			}
			return nil
		}
		gateRuntimeEnqueue(t, s, first)
		synctest.Wait()
		if attempts != 1 {
			t.Fatalf("first request attempted %d times at admission, want one", attempts)
		}
		gateRuntimePending(t, firstDone)
		second, secondDone := gateRuntimeRequest(Decision, 4, "getChat", time.Time{})
		third, thirdDone := gateRuntimeRequest(Decision, 4, "getChat", time.Time{})
		gateRuntimeEnqueue(t, s, second)
		gateRuntimeEnqueue(t, s, third)
		time.Sleep(time.Second)
		synctest.Wait()
		if got := gateRuntimeResult(t, firstDone); got.Outcome.Class != telegram.OK || got.Attempts != 2 || !got.Ambiguous {
			t.Errorf("retried head = %+v, want success on the second attempt", got)
		}
		gateRuntimePending(t, secondDone)
		gateRuntimePending(t, thirdDone)
		time.Sleep(time.Second)
		synctest.Wait()
		if got := gateRuntimeResult(t, secondDone); got.Outcome.Class != telegram.OK || got.Attempts != 1 {
			t.Errorf("request behind retry = %+v, want success on the first attempt", got)
		}
		gateRuntimePending(t, thirdDone)
		time.Sleep(time.Second)
		synctest.Wait()
		if got := gateRuntimeResult(t, thirdDone); got.Outcome.Class != telegram.OK || got.Attempts != 1 {
			t.Errorf("last queued request = %+v, want success on the first attempt", got)
		}
	})
}

func TestRuntimeExpiryBeforeAttemptReturnsTokens(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pause bool
	}{
		{name: "deadline reached"},
		{name: "new pause reaches deadline", pause: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				deps := gateRuntimeDeps(t)
				deps.Workers, deps.GlobalRPS, deps.GlobalBurst = 2, 0.1, 1
				if tc.pause {
					deps.GlobalBurst = 2
				}
				const chat = -7
				s, delayed, resume := startRuntimeWithDelayedAttempt(t, deps)
				defer gateRuntimeStop(t, s)
				defer resume()
				pauseNow := make(chan struct{})
				var pauseDone <-chan Result
				if tc.pause {
					trigger, triggerDone := gateRuntimeRequest(Decision, chat, "getChat", time.Time{})
					pauseDone = triggerDone
					started := make(chan struct{}, 1)
					trigger.Do = func(ctx context.Context, _ *bot.Bot) error {
						started <- struct{}{}
						select {
						case <-pauseNow:
							return &bot.TooManyRequestsError{RetryAfter: 1}
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					gateRuntimeEnqueue(t, s, trigger)
					workerRuntimeReceive(t, started)
				}
				const remaining = 10 * time.Millisecond
				r, done := gateRuntimeRequest(Background, chat, "sendMessage", time.Now().Add(remaining))
				r.Do = func(context.Context, *bot.Bot) error {
					t.Error("expired request reached Do")
					return nil
				}
				gateRuntimeEnqueue(t, s, r)
				workerRuntimeReceive(t, delayed)
				if tc.pause {
					close(pauseNow)
					if r := workerRuntimeReceive(t, pauseDone); r.Outcome.RetryAfter != time.Second {
						t.Fatalf("pause trigger = %+v, want a one-second 429", r)
					}
				} else {
					time.Sleep(remaining)
				}
				resume()
				synctest.Wait()
				gateRuntimeExpired(t, done, 0)
				if tc.pause {
					time.Sleep(time.Second)
				}
				assertRuntimeImmediateMessage(t, s, chat)
			})
		})
	}
}

func TestRuntimeShutdownBeforeAttemptReturnsTokens(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := gateRuntimeDeps(t)
		deps.GlobalRPS, deps.GlobalBurst = 0.1, 1
		const chat = -7
		s, delayed, resume := startRuntimeWithDelayedAttempt(t, deps)
		defer gateRuntimeStop(t, s)
		defer resume()
		r, done := gateRuntimeRequest(Background, chat, "sendMessage", time.Time{})
		r.Do = func(context.Context, *bot.Bot) error {
			t.Error("request reached Do after shutdown")
			return nil
		}
		gateRuntimeEnqueue(t, s, r)
		workerRuntimeReceive(t, delayed)
		follow, followDone := gateRuntimeRequest(Decision, chat, "sendMessage", time.Time{})
		gateRuntimeEnqueue(t, s, follow)
		began := time.Now()
		stopped := make(chan error, 1)
		go func() { stopped <- s.Stop(context.Background()) }()
		synctest.Wait()
		resume()
		synctest.Wait()
		workerRuntimeShutdown(t, gateRuntimeResult(t, done), 0, false)
		if got := gateRuntimeResult(t, followDone); got.Outcome.Class != telegram.OK || got.Attempts != 1 || got.Dropped != 0 {
			t.Fatalf("stop drain result = %+v, want immediate success in one attempt", got)
		}
		if err := workerRuntimeReceive(t, stopped); err != nil || !time.Now().Equal(began) {
			t.Errorf("Stop = %v after %v, want immediate success", err, time.Since(began))
		}
	})
}

func startRuntimeWithDelayedAttempt(t *testing.T, deps Deps) (*Sender, <-chan struct{}, func()) {
	t.Helper()
	s, err := New(deps)
	if err != nil {
		t.Fatal(err)
	}
	delayed, resume := make(chan struct{}), make(chan struct{})
	pause := sync.OnceFunc(func() {
		close(delayed)
		<-resume
	})
	s.beforeAttempt = func(entry *item) {
		if entry.request.Class == Background {
			pause()
		}
	}
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	return s, delayed, sync.OnceFunc(func() { close(resume) })
}

func assertRuntimeImmediateMessage(t *testing.T, s *Sender, chat int64) {
	t.Helper()
	r, done := gateRuntimeRequest(Decision, chat, "sendMessage", time.Time{})
	gateRuntimeEnqueue(t, s, r)
	synctest.Wait()
	if got := gateRuntimeResult(t, done); got.Outcome.Class != telegram.OK || got.Attempts != 1 || got.Dropped != 0 {
		t.Fatalf("follow-up message result = %+v, want immediate success in one attempt", got)
	}
}
