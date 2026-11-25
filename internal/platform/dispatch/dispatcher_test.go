package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"hash/maphash"
	"log"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

func testDeps() Deps {
	return Deps{
		Shards:           1,
		Inbox:            8,
		TicketsPerShard:  2,
		MailboxPerEntity: 16,
		NewTicker: func(int, time.Duration) (<-chan time.Time, func()) {
			return nil, func() {}
		},
	}
}

func startTestDispatcher(t *testing.T, deps Deps) *Dispatcher {
	t.Helper()
	d, err := New(deps)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := d.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return d
}

func await[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for signal")
		var zero T
		return zero
	}
}

func TestNewValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Deps)
	}{
		{"shards", func(d *Deps) { d.Shards = 0 }},
		{"inbox", func(d *Deps) { d.Inbox = -1 }},
		{"tickets", func(d *Deps) { d.TicketsPerShard = 0 }},
		{"mailbox", func(d *Deps) { d.MailboxPerEntity = -1 }},
		{"tick", func(d *Deps) { d.Tick = -time.Nanosecond }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := testDeps()
			tc.change(&deps)
			if d, err := New(deps); d != nil || err == nil {
				t.Fatalf("New = (%v, %v), want (nil, error)", d, err)
			}
		})
	}
}

func TestSubmitKeyOrder(t *testing.T) {
	const producers = 32
	const tasksPerKey = 64
	deps := testDeps()
	deps.Shards = 4
	type result struct {
		key      int
		sequence int
		shard    int
	}
	observed := make(chan result, producers*tasksPerKey)
	d := startTestDispatcher(t, deps)
	var keys [producers]Key
	var wantShards [producers]int
	for i := range keys {
		keys[i] = Key{Space: Space(13 + i%5), A: int64(i - producers/2), B: int64(11 - 3*i)}
		wantShards[i] = int(maphash.Comparable(d.seed, keys[i]) % uint64(deps.Shards))
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var writers sync.WaitGroup
	for i, key := range keys {
		writers.Go(func() {
			for sequence := range tasksPerKey {
				task := func(_ context.Context, sh *Shard) {
					observed <- result{i, sequence, sh.Index()}
				}
				for {
					err := d.Submit(key, task)
					if err == nil {
						break
					}
					if !errors.Is(err, ErrBusy) || ctx.Err() != nil {
						t.Errorf("Submit(%v) = %v, context error = %v", key, err, ctx.Err())
						return
					}
					runtime.Gosched()
				}
			}
		})
	}
	writers.Wait()
	if err := d.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	close(observed)

	var counts [producers]int
	usedShards := make(map[int]struct{})
	for got := range observed {
		key := keys[got.key]
		if got.sequence != counts[got.key] {
			t.Errorf("key %v: sequence %d, want %d", key, got.sequence, counts[got.key])
		}
		if want := wantShards[got.key]; got.shard != want {
			t.Errorf("key %v: shard %d, want %d", key, got.shard, want)
		}
		usedShards[got.shard] = struct{}{}
		counts[got.key]++
	}
	if len(usedShards) < 2 {
		t.Errorf("submitted tasks used %d shards, want at least 2", len(usedShards))
	}
	for i, count := range counts {
		if count != tasksPerKey {
			t.Errorf("key %v: executed %d tasks, want %d", keys[i], count, tasksPerKey)
		}
	}
}

func TestSubmitFullInbox(t *testing.T) {
	deps := testDeps()
	deps.Shards = 2
	deps.Inbox = 1
	deps.Metrics = metrics.NewSet()
	d := startTestDispatcher(t, deps)
	entered := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	key := Key{}
	for maphash.Comparable(d.seed, key)%uint64(deps.Shards) == 0 {
		key.A++
	}
	if err := d.Submit(key, func(_ context.Context, sh *Shard) {
		if ticket, ok := sh.Admit(); ok {
			defer ticket.Cancel()
		} else {
			t.Error("Admit failed in an empty shard")
		}
		close(entered)
		<-release
	}); err != nil {
		t.Fatal(err)
	}
	await(t, entered)
	processed := make(chan struct{})
	if err := d.Submit(key, func(context.Context, *Shard) { close(processed) }); err != nil {
		t.Fatal(err)
	}
	if err := d.Submit(key, func(context.Context, *Shard) {
		t.Error("rejected task ran")
	}); !errors.Is(err, ErrBusy) {
		t.Fatalf("Submit with full inbox = %v, want ErrBusy", err)
	}
	assertDispatchMetric(t, deps.Metrics, `yabot_dispatch_inbox_depth{shard="0"}`, 0)
	assertDispatchMetric(t, deps.Metrics, `yabot_dispatch_tickets_inuse{shard="0"}`, 0)
	assertDispatchMetric(t, deps.Metrics, `yabot_dispatch_inbox_depth{shard="1"}`, 1)
	assertDispatchMetric(t, deps.Metrics, `yabot_dispatch_tickets_inuse{shard="1"}`, 1)
	unblock()
	await(t, processed)
	assertDispatchMetric(t, deps.Metrics, `yabot_dispatch_inbox_depth{shard="1"}`, 0)
	assertDispatchMetric(t, deps.Metrics, `yabot_dispatch_tickets_inuse{shard="1"}`, 0)
}

func TestSubmitClosed(t *testing.T) {
	d, err := New(testDeps())
	if err != nil {
		t.Fatal(err)
	}
	task := func(context.Context, *Shard) { t.Error("rejected task ran") }
	if err := d.Submit(Key{}, task); !errors.Is(err, ErrClosed) {
		t.Fatalf("Submit before Start = %v, want ErrClosed", err)
	}
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := d.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := d.Submit(Key{}, task); !errors.Is(err, ErrClosed) {
		t.Fatalf("Submit after Stop = %v, want ErrClosed", err)
	}
}

func TestTaskPanic(t *testing.T) {
	var logged bytes.Buffer
	deps := testDeps()
	deps.Log = slog.New(slog.NewJSONHandler(&logged, nil))
	deps.Metrics = metrics.NewSet()
	d := startTestDispatcher(t, deps)
	key := Key{Space: 17, A: -12, B: 19}
	if err := d.Submit(key, func(context.Context, *Shard) { panic("failed task") }); err != nil {
		t.Fatal(err)
	}
	processed := make(chan struct{})
	if err := d.Submit(key, func(context.Context, *Shard) { close(processed) }); err != nil {
		t.Fatal(err)
	}
	await(t, processed)

	var record struct {
		Level string
		Shard *int
		Key   Key
		Stack string
	}
	if err := json.Unmarshal(logged.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record.Level != "ERROR" || record.Shard == nil || *record.Shard != 0 ||
		record.Key != key || !strings.Contains(record.Stack, "TestTaskPanic") {
		t.Errorf("unexpected panic log: %s", logged.String())
	}
	var sample bytes.Buffer
	deps.Metrics.WritePrometheus(&sample)
	if !strings.Contains(sample.String(), "yabot_dispatch_panics_total{route=\"none\"} 1\n") {
		t.Errorf("missing panic count in %q", sample.String())
	}
}

func TestTaskPanicWithDefaultLogAndMetrics(t *testing.T) {
	var logged bytes.Buffer
	originalLogger := slog.Default()
	originalWriter, originalFlags := log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	t.Cleanup(func() {
		slog.SetDefault(originalLogger)
		log.SetOutput(originalWriter)
		log.SetFlags(originalFlags)
	})

	d := startTestDispatcher(t, testDeps())
	if err := d.Submit(Key{}, func(context.Context, *Shard) { panic("failed task") }); err != nil {
		t.Fatal(err)
	}
	waitForShardTask(t, d)
	if logged.Len() != 0 {
		t.Errorf("nil Log wrote to the default logger: %s", logged.String())
	}
	for _, name := range metrics.GetDefaultSet().ListMetricNames() {
		if strings.HasPrefix(name, "yabot_dispatch_") {
			t.Errorf("nil Metrics registered %s in the default set", name)
		}
	}
}

func TestRegistrationPanics(t *testing.T) {
	noop := func(context.Context, *Shard) {}
	for _, tc := range []struct {
		name    string
		started bool
		call    func(*Dispatcher)
	}{
		{"duplicate space", false, func(d *Dispatcher) { d.RegisterSpace("other", 17) }},
		{"duplicate name", false, func(d *Dispatcher) { d.RegisterSpace("one", 19) }},
		{"empty name", false, func(d *Dispatcher) { d.RegisterSpace("", 23) }},
		{"nil tick", false, func(d *Dispatcher) { d.OnTick(nil) }},
		{"space after start", true, func(d *Dispatcher) { d.RegisterSpace("other", 29) }},
		{"tick after start", true, func(d *Dispatcher) { d.OnTick(noop) }},
		{"local after start", true, func(d *Dispatcher) { NewLocal(d, func() int { return 0 }) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := New(testDeps())
			if err != nil {
				t.Fatal(err)
			}
			d.RegisterSpace("one", 17)
			d.RegisterSpace("two", 18)
			if tc.started {
				if err := d.Start(t.Context()); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := d.Stop(context.Background()); err != nil {
						t.Error(err)
					}
				})
			}
			defer func() {
				if recover() == nil {
					t.Error("registration did not panic")
				}
			}()
			tc.call(d)
		})
	}
}

func TestLocalPerShard(t *testing.T) {
	deps := testDeps()
	deps.Shards = 4
	d, err := New(deps)
	if err != nil {
		t.Fatal(err)
	}
	initialized := 0
	local := NewLocal(d, func() map[int]int {
		initialized++
		return make(map[int]int)
	})
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for round := 1; round <= 2; round++ {
		err := d.RunOnAll(ctx, func(_ context.Context, sh *Shard, done func(error)) {
			value := local.Get(sh)
			value[sh.Index()]++
			if len(value) != 1 || value[sh.Index()] != round {
				t.Errorf("shard %d on round %d: local state = %v", sh.Index(), round, value)
			}
			done(nil)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if initialized != deps.Shards {
		t.Errorf("initializer called %d times, want %d", initialized, deps.Shards)
	}
}
