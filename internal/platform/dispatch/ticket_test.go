package dispatch

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

const (
	ticketsInUseMetric = `yabot_dispatch_tickets_inuse{shard="0"}`
	lateCompleteMetric = `yabot_dispatch_ticket_late_total{op="complete"}`
	lateCancelMetric   = `yabot_dispatch_ticket_late_total{op="cancel"}`
)

func TestTicketAdmissionAndReuse(t *testing.T) {
	deps := testDeps()
	deps.TicketsPerShard = 2
	deps.Metrics = metrics.NewSet()
	d := startTestDispatcher(t, deps)

	first := requireTicket(t, d)
	second := requireTicket(t, d)
	if _, ok := tryAdmitTicket(t, d); ok {
		t.Fatal("Admit succeeded with every ticket reserved")
	}
	assertDispatchMetric(t, deps.Metrics, ticketsInUseMetric, 2)

	first.Cancel()
	assertDispatchMetric(t, deps.Metrics, ticketsInUseMetric, 1)
	third := requireTicket(t, d)
	first.Cancel()
	if _, ok := tryAdmitTicket(t, d); ok {
		t.Fatal("canceling a stale ticket freed another ticket's reservation")
	}
	assertDispatchMetric(t, deps.Metrics, lateCancelMetric, 1)

	second.Cancel()
	third.Cancel()
	assertDispatchMetric(t, deps.Metrics, ticketsInUseMetric, 0)
}

func TestTicketCompletesWithFullInbox(t *testing.T) {
	deps := testDeps()
	deps.Inbox = 1
	deps.TicketsPerShard = 2
	d := startTestDispatcher(t, deps)
	first := requireTicket(t, d)
	second := requireTicket(t, d)
	release := holdShardForTicketTest(t, d)
	inboxRan := make(chan struct{})
	submitTicketTestTask(t, d, func(context.Context, *Shard) { close(inboxRan) })
	if err := d.Submit(Key{}, func(context.Context, *Shard) {}); err != ErrBusy {
		t.Fatalf("Submit with a full inbox = %v, want %v", err, ErrBusy)
	}

	callbacks := make(chan int, 2)
	delivered := make(chan struct{})
	go func() {
		first.Complete(func(context.Context, *Shard) { callbacks <- 1 })
		second.Complete(func(context.Context, *Shard) { callbacks <- 2 })
		close(delivered)
	}()
	await(t, delivered)
	release()
	for _, want := range []int{1, 2} {
		if got := await(t, callbacks); got != want {
			t.Fatalf("completion callback = %d, want %d", got, want)
		}
	}
	await(t, inboxRan)
	first.Complete(func(context.Context, *Shard) { t.Error("late completion ran without metrics") })
	waitForShardTask(t, d)
}

func TestTicketArbitration(t *testing.T) {
	for _, tc := range []struct {
		name         string
		completeWins bool
	}{
		{name: "complete first", completeWins: true},
		{name: "cancel first", completeWins: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := testDeps()
			deps.TicketsPerShard = 1
			deps.Metrics = metrics.NewSet()
			logs := &ticketTestLogs{}
			deps.Log = slog.New(logs)
			d := startTestDispatcher(t, deps)
			ticket := requireTicket(t, d)
			release := holdShardForTicketTest(t, d)
			var callbacks atomic.Int64
			fn := func(context.Context, *Shard) { callbacks.Add(1) }
			if tc.completeWins {
				ticket.Complete(fn)
				ticket.Cancel()
				assertDispatchMetric(t, deps.Metrics, ticketsInUseMetric, 1)
				assertDispatchMetric(t, deps.Metrics, lateCancelMetric, 1)
			} else {
				ticket.Cancel()
				ticket.Complete(fn)
				assertDispatchMetric(t, deps.Metrics, ticketsInUseMetric, 0)
				assertDispatchMetric(t, deps.Metrics, lateCompleteMetric, 1)
			}
			release()
			waitForShardTask(t, d)

			var wantCallbacks int64
			if tc.completeWins {
				wantCallbacks = 1
			}
			if got := callbacks.Load(); got != wantCallbacks {
				t.Errorf("completion callbacks = %d, want %d", got, wantCallbacks)
			}
			assertDispatchMetric(t, deps.Metrics, ticketsInUseMetric, 0)
			if got := logs.warnings.Load(); got != 0 {
				t.Errorf("late ticket attempts logged %d warnings or errors", got)
			}
		})
	}
}

func TestTicketConcurrentArbitration(t *testing.T) {
	const attemptsPerOp = 64
	deps := testDeps()
	deps.TicketsPerShard = 1
	deps.Metrics = metrics.NewSet()
	logs := &ticketTestLogs{}
	deps.Log = slog.New(logs)
	d := startTestDispatcher(t, deps)
	ticket := requireTicket(t, d)
	release := holdShardForTicketTest(t, d)

	var callbacks atomic.Int64
	start := make(chan struct{})
	var callers sync.WaitGroup
	for range attemptsPerOp {
		callers.Go(func() {
			<-start
			ticket.Complete(func(context.Context, *Shard) { callbacks.Add(1) })
		})
		callers.Go(func() {
			<-start
			ticket.Cancel()
		})
	}
	close(start)
	finished := make(chan struct{})
	go func() {
		callers.Wait()
		close(finished)
	}()
	await(t, finished)

	lateCompletes := deps.Metrics.GetOrCreateCounter(lateCompleteMetric).Get()
	lateCancels := deps.Metrics.GetOrCreateCounter(lateCancelMetric).Get()
	if lateCompletes > attemptsPerOp || lateCancels > attemptsPerOp || lateCompletes+lateCancels != 2*attemptsPerOp-1 {
		t.Fatalf("late Complete/Cancel = %d/%d, want exactly one winner", lateCompletes, lateCancels)
	}
	completed := int(attemptsPerOp - lateCompletes)
	assertDispatchMetric(t, deps.Metrics, ticketsInUseMetric, completed)
	release()
	waitForShardTask(t, d)
	if got := callbacks.Load(); got != int64(completed) {
		t.Errorf("completion callbacks = %d, want %d", got, completed)
	}
	assertDispatchMetric(t, deps.Metrics, ticketsInUseMetric, 0)
	reused := requireTicket(t, d)
	if _, ok := tryAdmitTicket(t, d); ok {
		t.Error("allowance grew after concurrent Complete and Cancel")
	}
	reused.Cancel()
	if got := logs.warnings.Load(); got != 0 {
		t.Errorf("late ticket attempts logged %d warnings or errors", got)
	}
}

func TestTicketCancelWakesStoppingShard(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := startTestDispatcher(t, testDeps())
		requireTicket(t, d).Cancel()
		synctest.Wait()

		first := requireTicket(t, d)
		last := requireTicket(t, d)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		stopped := make(chan error, 1)
		go func() { stopped <- d.Stop(ctx) }()
		synctest.Wait()
		if err := d.Submit(Key{}, func(context.Context, *Shard) {}); err != ErrClosed {
			t.Fatalf("Submit during Stop = %v, want %v", err, ErrClosed)
		}
		for i, ticket := range []Ticket{first, last} {
			select {
			case err := <-stopped:
				t.Fatalf("Stop returned before cancellation %d: %v", i+1, err)
			default:
			}
			ticket.Cancel()
			synctest.Wait()
		}
		if err := await(t, stopped); err != nil {
			t.Fatalf("Stop after Cancel: %v", err)
		}
	})
}

func TestTicketInvalidOperationsPanic(t *testing.T) {
	for _, tc := range []struct {
		name  string
		valid bool
		op    func(Ticket)
	}{
		{name: "zero Cancel", op: Ticket.Cancel},
		{name: "zero Complete", op: func(ticket Ticket) { ticket.Complete(func(context.Context, *Shard) {}) }},
		{name: "nil callback", valid: true, op: func(ticket Ticket) { ticket.Complete(nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ticket Ticket
			if tc.valid {
				d := startTestDispatcher(t, testDeps())
				ticket = requireTicket(t, d)
			}
			defer func() {
				if recover() == nil {
					t.Error("ticket operation did not panic")
				}
			}()
			tc.op(ticket)
		})
	}
}

func requireTicket(t *testing.T, d *Dispatcher) Ticket {
	t.Helper()
	ticket, ok := tryAdmitTicket(t, d)
	if !ok {
		t.Fatal("Admit failed with a ticket available")
	}
	t.Cleanup(ticket.Cancel)
	return ticket
}

func tryAdmitTicket(t *testing.T, d *Dispatcher) (Ticket, bool) {
	t.Helper()
	type result struct {
		ticket Ticket
		ok     bool
	}
	admitted := make(chan result, 1)
	submitTicketTestTask(t, d, func(_ context.Context, shard *Shard) {
		ticket, ok := shard.Admit()
		admitted <- result{ticket, ok}
	})
	res := await(t, admitted)
	return res.ticket, res.ok
}

func submitTicketTestTask(t *testing.T, d *Dispatcher, task Task) {
	t.Helper()
	if err := d.Submit(Key{}, task); err != nil {
		t.Fatalf("Submit: %v", err)
	}
}

func holdShardForTicketTest(t *testing.T, d *Dispatcher) func() {
	t.Helper()
	entered := make(chan struct{})
	released := make(chan struct{})
	release := sync.OnceFunc(func() { close(released) })
	t.Cleanup(release)
	submitTicketTestTask(t, d, func(context.Context, *Shard) {
		close(entered)
		<-released
	})
	await(t, entered)
	return release
}

func waitForShardTask(t *testing.T, d *Dispatcher) {
	t.Helper()
	done := make(chan struct{})
	submitTicketTestTask(t, d, func(context.Context, *Shard) { close(done) })
	await(t, done)
}

func assertDispatchMetric(t *testing.T, set *metrics.Set, name string, want int) {
	t.Helper()
	var output strings.Builder
	set.WritePrometheus(&output)
	if !strings.Contains("\n"+output.String(), fmt.Sprintf("\n%s %d\n", name, want)) {
		t.Fatalf("want %s %d in metrics:\n%s", name, want, output.String())
	}
}

type ticketTestLogs struct {
	warnings atomic.Int64
}

func (h *ticketTestLogs) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelWarn
}

func (h *ticketTestLogs) Handle(_ context.Context, record slog.Record) error {
	if record.Level >= slog.LevelWarn {
		h.warnings.Add(1)
	}
	return nil
}

func (h *ticketTestLogs) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *ticketTestLogs) WithGroup(string) slog.Handler      { return h }
