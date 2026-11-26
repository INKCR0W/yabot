package sender

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-telegram/bot"

	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

const modelRuntimeRetries = 3

type modelRuntimeMethod struct {
	name  string
	scope methodClass
}

type modelRuntimePlan struct {
	class    Class
	chat     int64
	method   modelRuntimeMethod
	arrival  time.Duration
	budget   time.Duration
	attempts [1 + modelRuntimeRetries]modelRuntimeAttempt
}

type modelRuntimeAttempt struct {
	delay      time.Duration
	outcome    int
	retryAfter int
}

type modelRuntimeTicket struct {
	admission  error
	started    int
	delivered  int
	ambiguous  bool
	lastResult Result
}

type modelRuntimeStart struct {
	at    time.Time
	class Class
	chat  int64
	scope methodClass
}

type modelRuntimePause struct {
	modelRuntimeStart
	duration time.Duration
}

type modelRuntimeTrace struct {
	mu     sync.Mutex
	active [Background + 1]int
	starts []modelRuntimeStart
	pauses []modelRuntimePause
}

func TestSenderRuntimeRandomized(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const producers = 16
		rng := rand.New(rand.NewPCG(618040231, 3037))
		plans := make([][]modelRuntimePlan, producers)
		tickets := make([][]modelRuntimeTicket, producers)
		for i := range plans {
			plans[i] = make([]modelRuntimePlan, 168)
			tickets[i] = make([]modelRuntimeTicket, len(plans[i]))
			for j := range plans[i] {
				plans[i][j] = newModelRuntimePlan(rng)
			}
			slices.SortStableFunc(plans[i], func(a, b modelRuntimePlan) int {
				return cmp.Compare(a.arrival, b.arrival)
			})
		}
		stopAfter := 6300*time.Millisecond + time.Duration(rng.IntN(701))*time.Millisecond
		stopBudget := time.Duration(100+rng.IntN(651)) * time.Millisecond

		b, err := bot.New("0:fake", bot.WithSkipGetMe())
		if err != nil {
			t.Fatal(err)
		}
		deps := Deps{
			Bot: b, Workers: 16, ReservedDeadline: 6, ReservedDecision: 4,
			CapInteractive: 4, CapBackground: 4, GlobalRPS: 100, GlobalBurst: 5,
			ClassWeights: [4]int{8, 4, 2, 1}, ChatQueue: 12, MaxRetries: modelRuntimeRetries,
		}
		s, err := New(deps)
		if err != nil {
			t.Fatal(err)
		}
		var charged []time.Time
		s.onDispatch = func(at time.Time, entry *item) {
			charged = append(charged, at)
			r := entry.request
			if !r.Deadline.IsZero() && !at.Before(r.Deadline) {
				t.Errorf("dispatch at %v reached deadline %v", at, r.Deadline)
			}
			if until, blocked := s.gate.blockedUntil(at, r.ChatID, classifyMethod(r.Method)); blocked {
				t.Errorf("dispatched %q for chat %d during pause until %v", r.Method, r.ChatID, until)
			}
			busy := s.quota.busy
			if busy[r.Class] < 1 {
				t.Errorf("no %s workers counted at dispatch: %v", r.Class, busy)
			}
			checkModelRuntimeConcurrency(t, deps, busy)
		}
		start := time.Now()
		if err := s.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		trace := &modelRuntimeTrace{}
		var submitting sync.WaitGroup
		for i, batch := range plans {
			submitting.Go(func() {
				for j, plan := range batch {
					time.Sleep(time.Until(start.Add(plan.arrival)))
					ticket := &tickets[i][j]
					r := trace.request(t, deps, plan, ticket, start)
					ticket.admission = s.Enqueue(r)
				}
			})
		}
		stopped := make(chan error, 1)
		go func() {
			time.Sleep(time.Until(start.Add(stopAfter)))
			ctx, cancel := context.WithTimeout(t.Context(), stopBudget)
			defer cancel()
			stopped <- s.Stop(ctx)
		}()
		submitting.Wait()
		if err := <-stopped; err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("first Stop: %v", err)
		}
		if err := s.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}

		trace.mu.Lock()
		defer trace.mu.Unlock()
		checkModelRuntimeResults(t, tickets, trace)
		checkModelRuntimeShutdown(t, trace.starts, start.Add(stopAfter), start.Add(stopAfter+stopBudget))
		checkModelRuntimePauses(t, trace.starts, trace.pauses)
		checkModelRuntimeRate(t, deps, charged)
	})
}

func newModelRuntimePlan(rng *rand.Rand) modelRuntimePlan {
	methods := [...]modelRuntimeMethod{
		{"sendChatJoinRequestWebApp", methodJoinRequest}, {"approveChatJoinRequest", methodJoinRequest},
		{"sendMessage", methodMessage}, {"editMessageText", methodMessage},
		{"getChat", methodOther}, {"modelCustom", methodOther},
	}
	plan := modelRuntimePlan{
		class:   Class(1 + rng.IntN(int(Background))),
		chat:    int64(rng.IntN(79) - 39),
		method:  methods[rng.IntN(len(methods))],
		arrival: time.Duration(rng.IntN(8000)) * time.Millisecond,
	}
	if rng.IntN(4) == 0 {
		plan.chat = -1
	}
	if plan.class == Deadline || plan.class == Interactive || rng.IntN(2) == 0 {
		plan.budget = time.Duration(1+rng.IntN(5800)) * time.Millisecond
	}
	retryAfter := [...]int{1, 2, 2, 3, 31}
	for i := range plan.attempts {
		plan.attempts[i] = modelRuntimeAttempt{
			delay:      time.Duration(rng.IntN(40)) * time.Millisecond,
			outcome:    rng.IntN(96),
			retryAfter: retryAfter[rng.IntN(len(retryAfter))],
		}
	}
	return plan
}

func (trace *modelRuntimeTrace) request(t *testing.T, deps Deps, plan modelRuntimePlan, ticket *modelRuntimeTicket, start time.Time) Request {
	var deadline time.Time
	if plan.budget > 0 {
		deadline = start.Add(plan.arrival + plan.budget)
	}
	return Request{
		Class: plan.class, ChatID: plan.chat, Method: plan.method.name, Deadline: deadline,
		Do: func(ctx context.Context, _ *bot.Bot) (err error) {
			at := time.Now()
			trace.mu.Lock()
			attempt := ticket.started
			ticket.started++
			trace.active[plan.class]++
			active := trace.active
			trace.starts = append(trace.starts, modelRuntimeStart{at, plan.class, plan.chat, plan.method.scope})
			trace.mu.Unlock()

			defer func() {
				trace.mu.Lock()
				defer trace.mu.Unlock()
				trace.active[plan.class]--
				outcome := telegram.Classify(err)
				if outcome.Class == telegram.Retry {
					if outcome.RetryAfter == 0 {
						ticket.ambiguous = true
					} else {
						trace.pauses = append(trace.pauses, modelRuntimePause{
							modelRuntimeStart{time.Now(), plan.class, plan.chat, plan.method.scope}, outcome.RetryAfter,
						})
					}
				}
			}()
			checkModelRuntimeConcurrency(t, deps, active)
			if !deadline.IsZero() && !at.Before(deadline) {
				t.Errorf("%q for chat %d started at %v with deadline %v", plan.method.name, plan.chat, at, deadline)
			}
			if attempt >= len(plan.attempts) {
				t.Errorf("attempt %d exceeds limit", attempt+1)
				return errors.New("model attempt limit exceeded")
			}
			return plan.attempts[attempt].run(ctx, at.Sub(start))
		},
		Done: func(result Result) {
			trace.mu.Lock()
			defer trace.mu.Unlock()
			ticket.delivered++
			ticket.lastResult = result
		},
	}
}

func (a modelRuntimeAttempt) run(ctx context.Context, elapsed time.Duration) error {
	if a.delay > 0 {
		timer := time.NewTimer(a.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	switch a.outcome {
	case 0, 1, 2:
		retryAfter := a.retryAfter
		if elapsed < 4300*time.Millisecond {
			retryAfter = min(retryAfter, 2)
		}
		return &bot.TooManyRequestsError{RetryAfter: retryAfter}
	case 3, 4, 5:
		return context.DeadlineExceeded
	case 6:
		return &net.DNSError{Err: "model network failure", IsTimeout: true}
	case 7:
		return errors.New("error response from telegram for method model, 502 upstream unavailable")
	case 8, 9, 10:
		<-ctx.Done()
		return ctx.Err()
	case 11:
		panic("model request panic")
	case 12:
		return fmt.Errorf("%w, HIDE_REQUESTER_MISSING", bot.ErrorBadRequest)
	case 13:
		return bot.ErrorForbidden
	case 14:
		return fmt.Errorf("%w, CHAT_ADMIN_REQUIRED", bot.ErrorBadRequest)
	case 15:
		return &bot.MigrateError{MigrateToChatID: -9001}
	case 16:
		return errors.New("model fatal error")
	case 17:
		return bot.ErrorConflict
	case 18:
		return bot.ErrorUnauthorized
	default:
		return nil
	}
}

func checkModelRuntimeConcurrency(t *testing.T, deps Deps, active [Background + 1]int) {
	total := active[Deadline] + active[Decision] + active[Interactive] + active[Background]
	reserved := min(active[Deadline], deps.ReservedDeadline) + min(active[Decision], deps.ReservedDecision)
	shared := deps.Workers - deps.ReservedDeadline - deps.ReservedDecision
	if active[Interactive] > deps.CapInteractive || active[Background] > deps.CapBackground ||
		total-reserved > shared {
		t.Errorf("in-flight counts exceed worker limits: %v (shared usage %d, capacity %d)", active, total-reserved, shared)
	}
}

func checkModelRuntimeShutdown(t *testing.T, starts []modelRuntimeStart, stopping, cutoff time.Time) {
	for _, start := range starts {
		if start.at.After(cutoff) || (start.class >= Interactive && start.at.After(stopping)) {
			t.Errorf("%v attempt started during shutdown at %v (stop=%v, cutoff=%v)", start.class, start.at, stopping, cutoff)
		}
	}
}

func checkModelRuntimeResults(t *testing.T, tickets [][]modelRuntimeTicket, trace *modelRuntimeTrace) {
	var accepted, rejected, attempts, shutdowns int
	for i, batch := range tickets {
		for j, ticket := range batch {
			if err := ticket.admission; err != nil {
				rejected++
				if !errors.Is(err, ErrClosed) && !errors.Is(err, ErrBusy) && !errors.Is(err, ErrWouldMiss) {
					t.Errorf("request %d/%d admission: %v", i, j, err)
				}
				if ticket.delivered != 0 || ticket.started != 0 {
					t.Errorf("rejected request %d/%d ran %d attempts and delivered %d results", i, j, ticket.started, ticket.delivered)
				}
				continue
			}
			accepted++
			result := ticket.lastResult
			attempts += ticket.started
			if ticket.delivered != 1 || result.Attempts != ticket.started || result.Attempts > 1+modelRuntimeRetries {
				t.Errorf("request %d/%d: deliveries=%d, observed attempts=%d, result=%+v", i, j, ticket.delivered, ticket.started, result)
			}
			if result.Ambiguous != ticket.ambiguous {
				t.Errorf("request %d/%d: Ambiguous=%v, want %v", i, j, result.Ambiguous, ticket.ambiguous)
			}
			if result.Dropped != 0 && result.Outcome.Class != telegram.Retry {
				t.Errorf("request %d/%d: dropped result %+v", i, j, result)
			}
			if result.Dropped == Shutdown {
				shutdowns++
			}
		}
	}
	if accepted < 1000 || rejected == 0 || len(trace.pauses) == 0 || shutdowns == 0 {
		t.Errorf("incomplete workload: accepted=%d, rejected=%d, attempts=%d, 429s=%d, shutdowns=%d",
			accepted, rejected, attempts, len(trace.pauses), shutdowns)
	}
	if trace.active != [Background + 1]int{} {
		t.Errorf("requests still active after Stop: %v", trace.active)
	}
	t.Logf("accepted=%d, rejected=%d, attempts=%d, 429s=%d, shutdowns=%d",
		accepted, rejected, attempts, len(trace.pauses), shutdowns)
}

func checkModelRuntimePauses(t *testing.T, starts []modelRuntimeStart, responses []modelRuntimePause) {
	slices.SortStableFunc(starts, func(a, b modelRuntimeStart) int { return a.at.Compare(b.at) })
	slices.SortStableFunc(responses, func(a, b modelRuntimePause) int { return a.at.Compare(b.at) })
	chatUntil := make(map[int64]time.Time)
	var methodUntil [methodClassCount]time.Time
	var history [methodClassCount][]modelRuntimePause
	var globalUntil time.Time
	for _, start := range starts {
		// Calls and 429 responses at the same virtual instant may have started in either order.
		for len(responses) > 0 && responses[0].at.Before(start.at) {
			response := responses[0]
			responses = responses[1:]
			until := response.at.Add(response.duration)
			if response.duration > 30*time.Second && globalUntil.Before(until) {
				globalUntil = until
			}
			if response.chat != 0 {
				if chatUntil[response.chat].Before(until) {
					chatUntil[response.chat] = until
				}
				scopeHistory := append(history[response.scope], response)
				history[response.scope] = scopeHistory
				distinct := make(map[int64]struct{})
				longest := time.Duration(0)
				for _, event := range scopeHistory {
					if response.at.Sub(event.at) < 10*time.Second {
						distinct[event.chat] = struct{}{}
						longest = max(longest, event.duration)
					}
				}
				if len(distinct) < 3 {
					continue
				}
				until = response.at.Add(longest)
			}
			if methodUntil[response.scope].Before(until) {
				methodUntil[response.scope] = until
			}
		}
		if start.at.Before(globalUntil) || start.at.Before(chatUntil[start.chat]) ||
			start.at.Before(methodUntil[start.scope]) {
			t.Errorf("Do for chat %d, method scope %v at %v violated 429 pause (global=%v, chat=%v, method=%v)",
				start.chat, start.scope, start.at, globalUntil, chatUntil[start.chat], methodUntil[start.scope])
		}
	}
}

func checkModelRuntimeRate(t *testing.T, deps Deps, charged []time.Time) {
	for first, from := range charged {
		for offset, until := range charged[first:] {
			n := offset + 1
			allowed := float64(deps.GlobalBurst) + deps.GlobalRPS*until.Sub(from).Seconds()
			if float64(n) > allowed+1e-6 {
				t.Fatalf("charged %d tokens in %v, bucket permits %.6f", n, until.Sub(from), allowed)
			}
		}
	}
}
