package dispatch

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

func TestShardPriority(t *testing.T) {
	deps := testDeps()
	deps.Inbox = 3
	deps.TicketsPerShard = 2
	ticks := make(chan time.Time, 1)
	deps.NewTicker = func(int, time.Duration) (<-chan time.Time, func()) {
		return ticks, func() {}
	}
	events := make(chan string, 5)
	onEvent := func(name string) Task {
		return func(context.Context, *Shard) { events <- name }
	}
	d := startWithTestTicks(t, deps, onEvent("tick"))
	first := requireTicket(t, d)
	second := requireTicket(t, d)
	release := holdShardForTicketTest(t, d)

	submitTicketTestTask(t, d, onEvent("inbox 1"))
	submitTicketTestTask(t, d, onEvent("inbox 2"))
	ticks <- time.Unix(0, 0)
	first.Complete(onEvent("completion 1"))
	second.Complete(onEvent("completion 2"))
	release()

	wantEvents := []string{"completion 1", "completion 2", "tick", "inbox 1", "inbox 2"}
	var gotEvents []string
	for range wantEvents {
		gotEvents = append(gotEvents, await(t, events))
	}
	if !slices.Equal(gotEvents, wantEvents) {
		t.Errorf("execution order = %v, want %v", gotEvents, wantEvents)
	}
}

func TestShardDrainsReadyBeforeNextInboxTask(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := startTestDispatcher(t, testDeps())
		release := holdShardForTicketTest(t, d)
		events := make(chan string, 3)
		submitTicketTestTask(t, d, func(_ context.Context, sh *Shard) {
			for _, event := range []string{"ready 1", "ready 2"} {
				sh.DeferInternal(Key{}, mailboxLifecycleItem(t, func(context.Context, *Shard) { events <- event }))
			}
		})
		submitTicketTestTask(t, d, func(context.Context, *Shard) { events <- "inbox" })
		release()
		for _, want := range []string{"ready 1", "ready 2", "inbox"} {
			if got := await(t, events); got != want {
				t.Fatalf("ran %q, want %q", got, want)
			}
		}
	})
}

func TestOnTickCallsEachShardInRegistrationOrder(t *testing.T) {
	for _, tc := range []struct {
		name     string
		interval time.Duration
		want     time.Duration
	}{
		{name: "default interval", want: 200 * time.Millisecond},
		{name: "custom interval", interval: 137 * time.Millisecond, want: 137 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := testDeps()
			deps.Shards = 3
			deps.Tick = tc.interval
			ticks := make([]chan time.Time, deps.Shards)
			for index := range ticks {
				ticks[index] = make(chan time.Time, 1)
			}
			type tickerStart struct {
				index    int
				interval time.Duration
			}
			started := make(chan tickerStart, deps.Shards)
			stopped := make(chan int, deps.Shards)
			deps.NewTicker = func(index int, interval time.Duration) (<-chan time.Time, func()) {
				started <- tickerStart{index, interval}
				return ticks[index], func() { stopped <- index }
			}
			type tickEvent struct {
				shard    int
				callback string
			}
			events := make(chan tickEvent, deps.Shards*4)
			onTick := func(name string) Task {
				return func(_ context.Context, shard *Shard) {
					events <- tickEvent{shard.Index(), name}
				}
			}
			d := startWithTestTicks(t, deps, onTick("first"), onTick("second"))
			startedByShard := make([]int, deps.Shards)
			for range deps.Shards {
				ticker := await(t, started)
				startedByShard[ticker.index]++
				if ticker.interval != tc.want {
					t.Errorf("shard %d interval = %v, want %v", ticker.index, ticker.interval, tc.want)
				}
			}
			if !slices.Equal(startedByShard, []int{1, 1, 1}) {
				t.Errorf("ticker starts by shard = %v, want one each", startedByShard)
			}

			for cycle := range 3 {
				for _, source := range ticks {
					source <- time.Unix(int64(cycle), 0)
				}
				got := make([][]string, deps.Shards)
				for range deps.Shards * 2 {
					event := await(t, events)
					got[event.shard] = append(got[event.shard], event.callback)
				}
				for shard, callbacks := range got {
					if !slices.Equal(callbacks, []string{"first", "second"}) {
						t.Errorf("tick %d on shard %d ran %v, want [first second]", cycle, shard, callbacks)
					}
				}
			}
			if err := d.Stop(t.Context()); err != nil {
				t.Fatalf("Stop: %v", err)
			}
			stoppedByShard := make([]int, deps.Shards)
			for range deps.Shards {
				stoppedByShard[await(t, stopped)]++
			}
			if !slices.Equal(stoppedByShard, []int{1, 1, 1}) {
				t.Errorf("ticker stops by shard = %v, want one each", stoppedByShard)
			}
			if got := len(events); got != 0 {
				t.Errorf("received %d extra tick callbacks", got)
			}
		})
	}
}

func TestOnTickWithDefaultTicker(t *testing.T) {
	for _, tc := range []struct {
		name     string
		interval time.Duration
		want     time.Duration
	}{
		{name: "default interval", want: 200 * time.Millisecond},
		{name: "custom interval", interval: 137 * time.Millisecond, want: 137 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const shards = 4
				deps := testDeps()
				deps.Shards = shards
				deps.Tick = tc.interval
				deps.NewTicker = nil
				var calls [shards]atomic.Int32
				startWithTestTicks(t, deps, func(_ context.Context, sh *Shard) {
					calls[sh.Index()].Add(1)
				})
				synctest.Wait()
				started := time.Now()
				for _, step := range []struct {
					advance time.Duration
					want    int32
				}{
					{tc.want - time.Nanosecond, 0},
					{time.Nanosecond, 1},
					{tc.want - time.Nanosecond, 1},
					{time.Nanosecond, 2},
				} {
					time.Sleep(step.advance)
					synctest.Wait()
					for index := range calls {
						if got := calls[index].Load(); got != step.want {
							t.Errorf("shard %d after %s: tick calls = %d, want %d", index, time.Since(started), got, step.want)
						}
					}
				}
			})
		})
	}
}

func TestCompletionAndTickRecoverFromPanics(t *testing.T) {
	deps := testDeps()
	deps.Metrics = metrics.NewSet()
	panicLog := &workPanicLog{Handler: slog.DiscardHandler}
	deps.Log = slog.New(panicLog)
	ticks := make(chan time.Time, 1)
	deps.NewTicker = func(int, time.Duration) (<-chan time.Time, func()) {
		return ticks, func() {}
	}
	tickContinued := make(chan struct{})
	d := startWithTestTicks(t, deps,
		func(context.Context, *Shard) { panic("tick panic") },
		func(context.Context, *Shard) { close(tickContinued) },
	)
	ticket := requireTicket(t, d)
	release := holdShardForTicketTest(t, d)
	ticket.Complete(func(context.Context, *Shard) { panic("completion panic") })
	ticks <- time.Unix(0, 0)
	release()

	await(t, tickContinued)
	waitForShardTask(t, d)
	wantPanics := []string{"completion panic", "tick panic"}
	if len(panicLog.errors) != len(wantPanics) {
		t.Fatalf("logged panic errors = %v, want %v", panicLog.errors, wantPanics)
	}
	for index, want := range wantPanics {
		assertWorkPanicError(t, panicLog.errors[index], want)
	}
	assertDispatchMetric(t, deps.Metrics, `yabot_dispatch_panics_total{route="none"}`, 2)
	assertDispatchMetric(t, deps.Metrics, ticketsInUseMetric, 0)
}

func TestRunOnAllReportsAbnormalTaskExit(t *testing.T) {
	for _, tc := range []struct {
		name          string
		exit          func()
		shardSurvives bool
	}{
		{name: "panic nil", exit: func() { panic(nil) }, shardSurvives: true},
		{name: "Goexit", exit: runtime.Goexit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",panicnil=1")
			synctest.Test(t, func(t *testing.T) {
				deps := testDeps()
				deps.Metrics = metrics.NewSet()
				d := startTestDispatcher(t, deps)
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				err := d.RunOnAll(ctx, func(context.Context, *Shard, func(error)) { tc.exit() })
				assertWorkPanicError(t, err, nil)
				if err == nil || !strings.Contains(err.Error(), "task panicked: <nil>") {
					t.Errorf("RunOnAll = %v, want a panic error with a nil value", err)
				}
				assertDispatchMetric(t, deps.Metrics, `yabot_dispatch_panics_total{route="none"}`, 1)
				if tc.shardSurvives {
					waitForShardTask(t, d)
				}
			})
		})
	}
}

func assertWorkPanicError(t *testing.T, err error, wantValue any) {
	t.Helper()
	var panicErr *PanicError
	if !errors.As(err, &panicErr) || panicErr == nil {
		t.Fatalf("panic error = %v (%T), want *PanicError", err, err)
	}
	if panicErr.Value != wantValue || len(panicErr.Stack) == 0 {
		t.Errorf("panic value = %v, stack size = %d, want value %v and a stack", panicErr.Value, len(panicErr.Stack), wantValue)
	}
}

type workPanicLog struct {
	slog.Handler
	errors []error
}

func (*workPanicLog) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelError
}

func (log *workPanicLog) Handle(_ context.Context, record slog.Record) error {
	var err error
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key != "error" {
			return true
		}
		err, _ = attr.Value.Any().(error)
		return false
	})
	log.errors = append(log.errors, err)
	return nil
}

func startWithTestTicks(t *testing.T, deps Deps, callbacks ...Task) *Dispatcher {
	t.Helper()
	d, err := New(deps)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, callback := range callbacks {
		d.OnTick(callback)
	}
	if err := d.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := d.Stop(ctx); err != nil {
			t.Errorf("Stop cleanup: %v", err)
		}
	})
	return d
}
