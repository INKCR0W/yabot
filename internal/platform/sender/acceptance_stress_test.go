package sender

import (
	"context"
	"errors"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-telegram/bot"

	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

const (
	acceptanceStressSize      = 10_000
	acceptanceStressProducers = 20
)

type acceptanceStressTicket struct {
	admission  error
	deliveries atomic.Int32
	attempts   atomic.Int32
	result     Result
}

func (ticket *acceptanceStressTicket) complete(result Result) bool {
	if ticket.deliveries.Add(1) != 1 {
		return false
	}
	ticket.result = result
	return true
}

func TestSenderAcceptanceGoroutineLeaks(t *testing.T) {
	const chats = 200
	deps := acceptanceDeps(t, 1e6)
	deps.GlobalBurst = 1000
	before := runtime.NumGoroutine()
	s := gateRuntimeStart(t, deps)
	defer gateRuntimeStop(t, s)

	tickets := make([]acceptanceStressTicket, acceptanceStressSize)
	done := make(chan struct{})
	var remaining atomic.Int32
	remaining.Store(acceptanceStressSize)
	fatalErr := errors.New("fake fatal response")
	var producers sync.WaitGroup
	start := make(chan struct{})
	for worker := range acceptanceStressProducers {
		producers.Go(func() {
			<-start
			for id := worker; id < len(tickets); id += acceptanceStressProducers {
				ticket := &tickets[id]
				ticket.admission = s.Enqueue(Request{
					Class: Decision, ChatID: -int64(id%chats + 1), Method: "approveChatJoinRequest",
					Do: func(context.Context, *bot.Bot) error {
						attempt := ticket.attempts.Add(1)
						switch id % 3 {
						case 1:
							if attempt == 1 {
								return context.DeadlineExceeded
							}
						case 2:
							return fatalErr
						}
						return nil
					},
					Done: func(result Result) {
						if ticket.complete(result) && remaining.Add(-1) == 0 {
							close(done)
						}
					},
				})
			}
		})
	}
	close(start)
	producers.Wait()
	for id := range tickets {
		if err := tickets[id].admission; err != nil {
			t.Fatalf("request %d admission: %v", id, err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("waiting for completion: %d pending, %v", remaining.Load(), ctx.Err())
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	after := waitForAcceptanceGoroutines(t, before)

	var successes, retries, fatals int
	for id := range tickets {
		ticket := &tickets[id]
		checkAcceptanceDelivery(t, id, ticket)
		r := ticket.result
		wantClass, wantAttempts := telegram.OK, 1
		switch id % 3 {
		case 1:
			wantAttempts = 2
		case 2:
			wantClass = telegram.Fatal
		}
		if r.Outcome.Class != wantClass || r.Attempts != wantAttempts || r.Dropped != 0 || r.Ambiguous != (wantAttempts == 2) {
			t.Errorf("request %d: result %+v, want %s after %d attempts", id, r, wantClass, wantAttempts)
		}
		switch {
		case r.Outcome.Class == telegram.OK && r.Attempts == 1:
			successes++
		case r.Outcome.Class == telegram.OK && r.Attempts == 2:
			retries++
		case r.Outcome.Class == telegram.Fatal:
			fatals++
		}
	}
	t.Logf("completed=%d immediate successes=%d successes after retry=%d fatal=%d goroutines before=%d after=%d",
		len(tickets), successes, retries, fatals, before, after)
}

func waitForAcceptanceGoroutines(t *testing.T, baseline int) int {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(5 * time.Millisecond)
	defer poll.Stop()
	for {
		if count := runtime.NumGoroutine(); count <= baseline {
			return count
		}
		select {
		case <-poll.C:
		case <-deadline.C:
			count := runtime.NumGoroutine()
			if count > baseline {
				t.Errorf("goroutines after Stop = %d, baseline = %d", count, baseline)
			}
			return count
		}
	}
}

func TestSenderAcceptanceExactlyOnceUnderStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := acceptanceDeps(t, 1e6)
		deps.GlobalBurst = 1000
		s := gateRuntimeStart(t, deps)
		defer gateRuntimeStop(t, s)
		tickets := make([]acceptanceStressTicket, acceptanceStressSize)
		const probes = 2
		rateLimited := &bot.TooManyRequestsError{RetryAfter: 1}
		verifyAcceptanceErrors(t, s, tickets[:probes], rateLimited)

		type plan struct {
			class    Class
			response int
		}
		const (
			success = iota
			fatal
			retry
			throttled
			timeout
			responseCount
		)
		rng := rand.New(rand.NewPCG(461, 3079))
		plans := make([]plan, len(tickets))
		for i := probes; i < len(plans); i++ {
			plans[i] = plan{class: Deadline + Class(rng.IntN(int(Background))), response: rng.IntN(responseCount)}
		}
		fatalErr := errors.New("fake fatal response")
		var rateLimitResponses, timeoutResponses atomic.Int32
		enqueue := func(id int) {
			ticket, plan := &tickets[id], plans[id]
			ticket.admission = s.Enqueue(Request{
				Class: plan.class, ChatID: -int64(id + 1), Method: "approveChatJoinRequest",
				Deadline: time.Now().Add(30 * time.Second),
				Do: func(ctx context.Context, _ *bot.Bot) error {
					if ticket.attempts.Add(1) > 1 {
						return nil
					}
					switch plan.response {
					case fatal:
						return fatalErr
					case retry:
						return context.DeadlineExceeded
					case throttled:
						rateLimitResponses.Add(1)
						return rateLimited
					case timeout:
						<-ctx.Done()
						err := ctx.Err()
						if errors.Is(err, context.DeadlineExceeded) {
							timeoutResponses.Add(1)
						}
						return err
					default:
						return nil
					}
				},
				Done: func(result Result) { ticket.complete(result) },
			})
		}

		const beforeStop = acceptanceStressSize / 2
		const afterStop = acceptanceStressSize * 3 / 4
		var producers, initial sync.WaitGroup
		initial.Add(acceptanceStressProducers)
		racing, stopped := make(chan struct{}), make(chan struct{})
		for worker := range acceptanceStressProducers {
			producers.Go(func() {
				id := probes + worker
				for ; id < beforeStop; id += acceptanceStressProducers {
					enqueue(id)
				}
				initial.Done()
				<-racing
				for ; id < afterStop; id += acceptanceStressProducers {
					enqueue(id)
				}
				<-stopped
				for ; id < len(tickets); id += acceptanceStressProducers {
					enqueue(id)
				}
			})
		}
		initial.Wait()
		stopResult := make(chan error, 1)
		go func() {
			<-racing
			stopResult <- s.Stop(t.Context())
			close(stopped)
		}()
		close(racing)
		producers.Wait()
		if err := <-stopResult; err != nil {
			t.Fatal(err)
		}

		var accepted, closed, shutdown, attempts int
		maxAttempts := 1 + deps.MaxRetries
		for id := range tickets {
			ticket := &tickets[id]
			switch err := ticket.admission; {
			case err == nil:
				accepted++
				if ticket.result.Dropped == Shutdown {
					shutdown++
				}
				observed := int(ticket.attempts.Load())
				attempts += observed
				if observed > maxAttempts {
					t.Errorf("request %d ran %d attempts, want at most %d", id, observed, maxAttempts)
				}
			case errors.Is(err, ErrClosed):
				closed++
			default:
				t.Errorf("request %d admission: %v", id, err)
			}
			checkAcceptanceDelivery(t, id, ticket)
		}
		if accepted < beforeStop || closed < len(tickets)-afterStop || shutdown == 0 {
			t.Errorf("incomplete stop coverage: accepted=%d closed=%d shutdown=%d", accepted, closed, shutdown)
		}
		t.Logf("submitted=%d accepted=%d closed=%d shutdown=%d attempts=%d rate limits=%d timeouts=%d",
			len(tickets), accepted, closed, shutdown, attempts, 1+rateLimitResponses.Load(), 1+timeoutResponses.Load())
	})
}

func verifyAcceptanceErrors(t *testing.T, s *Sender, tickets []acceptanceStressTicket, rateLimited error) {
	t.Helper()
	deadline := time.Now().Add(200 * time.Millisecond)
	for i, do := range []func(context.Context) error{
		func(context.Context) error { return rateLimited },
		func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	} {
		ticket := &tickets[i]
		gateRuntimeEnqueue(t, s, Request{
			Class: Interactive, ChatID: -int64(i + 1), Method: "getChat", Deadline: deadline,
			Do: func(ctx context.Context, _ *bot.Bot) error {
				ticket.attempts.Add(1)
				return do(ctx)
			},
			Done: func(result Result) { ticket.complete(result) },
		})
	}
	time.Sleep(time.Until(deadline))
	synctest.Wait()
	if r := tickets[0].result; r.Outcome.RetryAfter != time.Second || r.Dropped != Expired || r.Attempts != 1 || r.Ambiguous {
		t.Fatalf("429 probe result = %+v, want expiry after one unambiguous attempt with retry_after=1s", r)
	}
	if r := tickets[1].result; !errors.Is(r.Err, context.DeadlineExceeded) || !r.Ambiguous || r.Attempts != 1 ||
		r.Outcome.Class != telegram.Retry || r.Outcome.RetryAfter != 0 {
		t.Fatalf("timeout probe result = %+v, want Retry without retry_after after one ambiguous attempt", r)
	}
}

func checkAcceptanceDelivery(t *testing.T, id int, ticket *acceptanceStressTicket) {
	t.Helper()
	deliveries, attempts := ticket.deliveries.Load(), ticket.attempts.Load()
	if ticket.admission != nil {
		if deliveries != 0 || attempts != 0 {
			t.Errorf("rejected request %d ran %d attempts and called Done %d times", id, attempts, deliveries)
		}
		return
	}
	if deliveries != 1 || ticket.result.Attempts != int(attempts) {
		t.Errorf("request %d called Done %d times, observed %d attempts, result %+v", id, deliveries, attempts, ticket.result)
	}
	if r := ticket.result; r.Dropped != 0 && r.Outcome.Class != telegram.Retry {
		t.Errorf("request %d dropped with %s outcome, want Retry: %+v", id, r.Outcome.Class, r)
	}
}
