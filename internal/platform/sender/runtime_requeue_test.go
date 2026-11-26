package sender

import (
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-telegram/bot"

	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

func TestRuntimeRequestsInterruptedBeforeDoKeepAdmissionOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const chat = -1
		const tokenInterval = 40 * time.Millisecond
		deps := gateRuntimeDeps(t)
		deps.Workers, deps.GlobalRPS, deps.GlobalBurst = 3, 25, 5
		s, err := New(deps)
		if err != nil {
			t.Fatal(err)
		}
		methods := [...]string{"sendChatJoinRequestWebApp", "answerChatJoinRequestQuery"}
		permits := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
		s.beforeAttempt = func(entry *item) {
			for i, method := range methods {
				if entry.request.Method == method {
					<-permits[i]
					return
				}
			}
		}
		if err := s.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer gateRuntimeStop(t, s)
		var resume [2]func()
		for i, permit := range permits {
			resume[i] = sync.OnceFunc(func() { close(permit) })
			defer resume[i]()
		}

		reply := make(chan struct{})
		trigger, triggerDone := gateRuntimeRequest(Decision, chat, "getChat", time.Time{})
		trigger.Do = func(ctx context.Context, _ *bot.Bot) error {
			select {
			case <-reply:
				return &bot.TooManyRequestsError{RetryAfter: 1}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		gateRuntimeEnqueue(t, s, trigger)
		synctest.Wait()

		recovery := time.Now().Add(time.Second)
		started := make(chan string, len(methods))
		var done [2]<-chan Result
		for i, method := range methods {
			deadline := recovery.Add(time.Second)
			if i == 0 {
				deadline = recovery.Add(10 * time.Millisecond)
			}
			r, result := gateRuntimeRequest(Deadline, chat, method, deadline)
			done[i] = result
			r.Do = func(context.Context, *bot.Bot) error {
				started <- method
				return nil
			}
			gateRuntimeEnqueue(t, s, r)
			synctest.Wait()
			if s.ClassWait(Deadline) != 0 {
				t.Fatalf("%s still queued before the 429", method)
			}
			gateRuntimePending(t, done[i])
		}
		close(reply)
		synctest.Wait()
		if got := gateRuntimeResult(t, triggerDone); got.Outcome.RetryAfter != time.Second {
			t.Fatalf("pause trigger = %+v, want 1s RetryAfter", got)
		}
		for i, unblock := range resume {
			unblock()
			synctest.Wait()
			gateRuntimePending(t, done[i])
			if got, want := s.ClassWait(Deadline), time.Duration(i+1)*tokenInterval; got != want {
				t.Fatalf("Deadline backlog after returning %s = %v, want %v", methods[i], got, want)
			}
		}

		// Leave a single token at recovery and no refill before the first deadline.
		time.Sleep(time.Until(recovery) - time.Nanosecond)
		for range deps.GlobalBurst - 1 {
			r, result := gateRuntimeRequest(Decision, chat-1, "getChat", time.Time{})
			gateRuntimeEnqueue(t, s, r)
			synctest.Wait()
			if got := gateRuntimeResult(t, result); got.Outcome.Class != telegram.OK {
				t.Fatalf("unpaused chat result = %+v, want OK", got)
			}
		}
		if len(started) != 0 {
			t.Fatalf("paused requests started before recovery: %d", len(started))
		}
		time.Sleep(time.Until(recovery.Add(tokenInterval)))
		synctest.Wait()
		var order []string
		for len(started) > 0 {
			order = append(order, <-started)
		}
		if !slices.Equal(order, methods[:]) {
			t.Errorf("Do start order = %v, want %v", order, methods)
		}
		for i, result := range done {
			if got := gateRuntimeResult(t, result); got.Outcome.Class != telegram.OK || got.Dropped != 0 || got.Attempts != 1 {
				t.Errorf("%s result = %+v, want success on the first attempt", methods[i], got)
			}
		}
	})
}

func TestRuntimeRetriesDueTogetherKeepAdmissionOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := gateRuntimeDeps(t)
		deps.MaxRetries = 1
		s, err := New(deps)
		if err != nil {
			t.Fatal(err)
		}
		s.retryRandom = func(int64) int64 { return 0 }
		if err := s.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer gateRuntimeStop(t, s)

		methods := [...]string{"getChat", "getChatMember"}
		started := make(chan string, 2*len(methods))
		var done [2]<-chan Result
		// Both first attempts return at the same virtual instant, so both retries become due together.
		for i, method := range methods {
			r, result := gateRuntimeRequest(Decision, -1, method, time.Time{})
			done[i] = result
			attempted := false
			r.Do = func(context.Context, *bot.Bot) error {
				started <- method
				if !attempted {
					attempted = true
					return context.DeadlineExceeded
				}
				return nil
			}
			gateRuntimeEnqueue(t, s, r)
			synctest.Wait()
			gateRuntimePending(t, done[i])
			if got := workerRuntimeReceive(t, started); got != method {
				t.Fatalf("first attempt = %s, want %s", got, method)
			}
		}

		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		var order []string
		for len(started) > 0 {
			order = append(order, <-started)
		}
		if !slices.Equal(order, methods[:]) {
			t.Errorf("retry start order = %v, want %v", order, methods)
		}
		for i, result := range done {
			if got := gateRuntimeResult(t, result); got.Outcome.Class != telegram.OK || got.Dropped != 0 || got.Attempts != 2 || !got.Ambiguous {
				t.Errorf("%s result = %+v, want ambiguous success after two attempts", methods[i], got)
			}
		}
	})
}
