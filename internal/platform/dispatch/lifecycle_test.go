package dispatch

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestStopDrainsAcceptedTasksAndWaitsForCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ticks := make(chan time.Time, 1)
		deps := testDeps()
		deps.NewTicker = func(int, time.Duration) (<-chan time.Time, func()) {
			return ticks, func() {}
		}
		d, err := New(deps)
		if err != nil {
			t.Fatal(err)
		}
		tickErrors := make(chan error, 1)
		d.OnTick(func(ctx context.Context, _ *Shard) { tickErrors <- ctx.Err() })
		if err := d.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := d.Stop(context.Background()); err != nil {
				t.Error(err)
			}
		})

		key := Key{A: 42}
		gate := make(chan struct{})
		release := sync.OnceFunc(func() { close(gate) })
		t.Cleanup(release)
		entered := make(chan struct{})
		if err := d.Submit(key, func(context.Context, *Shard) {
			close(entered)
			<-gate
		}); err != nil {
			t.Fatal(err)
		}
		await(t, entered)

		const queued = 4
		order := make(chan int, queued)
		for i := range queued {
			if err := d.Submit(key, func(context.Context, *Shard) { order <- i }); err != nil {
				t.Fatal(err)
			}
		}
		type admission struct {
			ticket Ticket
			ok     bool
		}
		admitted := make(chan [2]admission, 1)
		if err := d.Submit(key, func(_ context.Context, sh *Shard) {
			var results [2]admission
			for i := range results {
				results[i].ticket, results[i].ok = sh.Admit()
			}
			admitted <- results
		}); err != nil {
			t.Fatal(err)
		}
		stopCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stopped := make(chan error, 1)
		go func() { stopped <- d.Stop(stopCtx) }()
		await(t, d.closing)
		if err := d.Submit(key, func(context.Context, *Shard) {}); !errors.Is(err, ErrClosed) {
			t.Fatalf("Submit during Stop = %v, want ErrClosed", err)
		}
		release()
		var results [2]admission
		select {
		case results = <-admitted:
		case err := <-stopped:
			t.Fatalf("Stop returned before executing an accepted task: %v", err)
		}
		for i, result := range results {
			if !result.ok {
				t.Fatalf("Admit %d during drain failed", i+1)
			}
			defer result.ticket.Cancel()
		}
		ticks <- time.Now()
		if err := await(t, tickErrors); err != nil {
			t.Fatalf("tick during Stop received a canceled context: %v", err)
		}
		completed := make(chan int, len(results))
		for i, result := range results {
			synctest.Wait()
			select {
			case err := <-stopped:
				t.Fatalf("Stop returned before completion %d was sent: %v", i+1, err)
			default:
			}
			result.ticket.Complete(func(context.Context, *Shard) { completed <- i })
		}
		if err := await(t, stopped); err != nil {
			t.Fatal(err)
		}
		for want := range results {
			select {
			case got := <-completed:
				if got != want {
					t.Fatalf("completion = %d, want %d", got, want)
				}
			default:
				t.Fatalf("Stop returned before completion %d executed", want+1)
			}
		}
		for want := range queued {
			select {
			case got := <-order:
				if got != want {
					t.Fatalf("drained task = %d, want %d", got, want)
				}
			default:
				t.Fatalf("accepted task %d did not execute", want)
			}
		}
	})
}

func TestStopDeadlineDrainsInboxAndExitsWithTicketsInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tickerStopped := make(chan struct{})
		deps := testDeps()
		deps.NewTicker = func(int, time.Duration) (<-chan time.Time, func()) {
			return nil, func() { close(tickerStopped) }
		}
		d, err := New(deps)
		if err != nil {
			t.Fatal(err)
		}
		d.OnTick(func(context.Context, *Shard) {})
		if err := d.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := d.Stop(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Error(err)
			}
		})
		key := Key{}
		type admission struct {
			ticket Ticket
			ok     bool
		}
		admitted := make(chan admission, 1)
		if err := d.Submit(key, func(ctx context.Context, sh *Shard) {
			ticket, ok := sh.Admit()
			admitted <- admission{ticket: ticket, ok: ok}
			<-ctx.Done()
		}); err != nil {
			t.Fatal(err)
		}
		result := await(t, admitted)
		if !result.ok {
			t.Fatal("Admit failed")
		}
		drained := make(chan error, 1)
		if err := d.Submit(key, func(ctx context.Context, _ *Shard) { drained <- ctx.Err() }); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		firstStop := make(chan error, 1)
		go func() { firstStop <- d.Stop(ctx) }()
		synctest.Wait()

		canceledCtx, cancelSecond := context.WithCancel(context.Background())
		cancelSecond()
		concurrentStop := make(chan error, 1)
		go func() { concurrentStop <- d.Stop(canceledCtx) }()
		synctest.Wait()
		select {
		case err := <-concurrentStop:
			t.Fatalf("concurrent Stop returned before the first Stop: %v", err)
		default:
		}

		err = await(t, firstStop)
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "with 1 in-flight ticket") {
			t.Fatalf("Stop = %v, want deadline error reporting one in-flight ticket", err)
		}
		select {
		case err := <-drained:
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("drained task context error = %v, want cancellation", err)
			}
		default:
			t.Error("Stop returned before the accepted task ran")
		}
		select {
		case <-tickerStopped:
		default:
			t.Error("Stop returned before the shard exited")
		}
		if got := await(t, concurrentStop); got != err {
			t.Errorf("concurrent Stop = %v, want the first result %v", got, err)
		}
		if got := d.Stop(context.Background()); got != err {
			t.Errorf("repeated Stop = %v, want the first result %v", got, err)
		}

		lateCallback := make(chan struct{}, 1)
		returned := make(chan struct{})
		go func() {
			result.ticket.Complete(func(context.Context, *Shard) { lateCallback <- struct{}{} })
			close(returned)
		}()
		await(t, returned)
		select {
		case <-lateCallback:
			t.Error("completion callback executed after the shard exited")
		default:
		}
	})
}

func TestStartContextHasIndependentLifetime(t *testing.T) {
	d, err := New(testDeps())
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Stop(t.Context()); err != nil {
		t.Fatalf("Stop before Start = %v, want nil", err)
	}
	t.Cleanup(func() {
		if err := d.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	})
	canceledCtx, cancelStart := context.WithCancel(t.Context())
	cancelStart()
	if err := d.Start(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start with a canceled context = %v, want context.Canceled", err)
	}
	startupCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.Start(startupCtx); err != nil {
		t.Fatal(err)
	}
	if err := d.Start(t.Context()); err == nil {
		t.Error("repeated Start returned nil")
	}
	cancel()
	seen := make(chan context.Context, 1)
	if err := d.Submit(Key{}, func(ctx context.Context, _ *Shard) { seen <- ctx }); err != nil {
		t.Fatal(err)
	}
	runCtx := await(t, seen)
	if err := runCtx.Err(); err != nil {
		t.Fatalf("canceling the Start context canceled the run context: %v", err)
	}
	if err := d.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(runCtx.Err(), context.Canceled) {
		t.Errorf("run context error after Stop = %v, want cancellation", runCtx.Err())
	}
	if err := d.Start(t.Context()); err == nil {
		t.Error("Start after Stop returned nil")
	}
}

func TestSubmitRacingStopExecutesEveryAcceptedTask(t *testing.T) {
	deps := testDeps()
	deps.Shards = 4
	deps.Inbox = 64
	d := startTestDispatcher(t, deps)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const producers = 64
	started := make(chan struct{}, producers)
	producerErrors := make(chan error, producers)
	var accepted, executed atomic.Int64
	task := func(context.Context, *Shard) { executed.Add(1) }
	var group sync.WaitGroup
	for n := range producers {
		group.Go(func() {
			key := Key{A: int64(n)}
			for first := true; ctx.Err() == nil; first = false {
				err := d.Submit(key, task)
				if first {
					started <- struct{}{}
				}
				switch {
				case err == nil:
					accepted.Add(1)
				case errors.Is(err, ErrBusy):
					runtime.Gosched()
				case errors.Is(err, ErrClosed):
					return
				default:
					producerErrors <- err
					return
				}
			}
			producerErrors <- ctx.Err()
		})
	}
	for range producers {
		await(t, started)
	}
	if err := d.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	group.Wait()
	close(producerErrors)
	for err := range producerErrors {
		t.Errorf("producer = %v, want ErrClosed at shutdown", err)
	}
	if got, want := executed.Load(), accepted.Load(); want == 0 || got != want {
		t.Fatalf("executed %d tasks after accepting %d", got, want)
	}
}

func TestStopDeadlineRunsQueuedCompletionsBeforeInbox(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, err := New(testDeps())
		if err != nil {
			t.Fatal(err)
		}
		if err := d.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		t.Cleanup(func() {
			if err := d.Stop(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Error(err)
			}
		})
		key := Key{}
		tickets := make(chan [2]Ticket, 1)
		if err := d.Submit(key, func(ctx context.Context, sh *Shard) {
			first, ok1 := sh.Admit()
			second, ok2 := sh.Admit()
			if !ok1 || !ok2 {
				t.Error("Admit failed")
			}
			tickets <- [2]Ticket{first, second}
			<-ctx.Done()
		}); err != nil {
			t.Fatal(err)
		}
		pair := await(t, tickets)
		order := make(chan string, 5)
		pair[0].Complete(func(_ context.Context, sh *Shard) {
			order <- "first completion"
			ticket, ok := sh.Admit()
			if !ok {
				t.Error("Admit from completion failed")
				return
			}
			ticket.Complete(func(context.Context, *Shard) { order <- "completion queued during drain" })
		})
		pair[1].Complete(func(context.Context, *Shard) { order <- "second completion" })
		for _, name := range []string{"first inbox task", "second inbox task"} {
			if err := d.Submit(key, func(context.Context, *Shard) { order <- name }); err != nil {
				t.Fatal(err)
			}
		}
		err = d.Stop(ctx)
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "with 1 in-flight ticket") {
			t.Fatalf("Stop = %v, want deadline error reporting one in-flight ticket", err)
		}
		for _, want := range []string{"first completion", "second completion", "first inbox task", "second inbox task"} {
			select {
			case got := <-order:
				if got != want {
					t.Fatalf("ran %q, want %q", got, want)
				}
			default:
				t.Fatalf("%s did not run before Stop returned", want)
			}
		}
		select {
		case extra := <-order:
			t.Errorf("unexpected work during drain: %s", extra)
		default:
		}
	})
}
