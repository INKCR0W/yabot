package dispatch

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"log/slog"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

type Space uint16

type Key struct {
	Space Space
	A, B  int64
}

// Task runs on its shard and must not perform slow I/O or call runtime.Goexit.
type Task func(ctx context.Context, sh *Shard)

var (
	ErrBusy   = errors.New("dispatch: inbox is full")
	ErrClosed = errors.New("dispatch: dispatcher is not running")
)

type Deps struct {
	Shards, Inbox, TicketsPerShard int
	MailboxPerEntity               int
	Tick                           time.Duration
	Log                            *slog.Logger
	Metrics                        *metrics.Set
	// NewTicker is called once per shard, concurrently, with the interval after defaults are applied.
	NewTicker func(shard int, interval time.Duration) (ticks <-chan time.Time, stop func())
	// Now supplies replay deadlines and must be safe for concurrent calls. It defaults to time.Now.
	Now func() time.Time
}

type dispatcherState uint8

const (
	stateCreated dispatcherState = iota
	stateRunning
	stateStopping
	stateStopped
)

type work struct {
	key     Key
	route   string
	task    Task
	onPanic func(error)
}

type Dispatcher struct {
	deps   Deps
	seed   maphash.Seed
	shards []*Shard

	mu          sync.RWMutex
	state       dispatcherState
	spaces      map[Space]string
	spaceNames  map[string]struct{}
	tickFns     []Task
	cancelRun   context.CancelFunc
	closing     chan struct{}
	workersDone chan struct{}
	stopped     chan struct{}
	stopErr     error

	panics          *metrics.Counter
	lateComplete    *metrics.Counter
	lateCancel      *metrics.Counter
	lateDone        *metrics.Counter
	mailboxOverflow *metrics.Counter
}

// New defaults Tick to 200 ms and uses a discard logger when Log is nil.
func New(deps Deps) (*Dispatcher, error) {
	switch {
	case deps.Shards <= 0:
		return nil, errors.New("dispatch: shards must be positive")
	case deps.Inbox <= 0:
		return nil, errors.New("dispatch: inbox must be positive")
	case deps.TicketsPerShard <= 0:
		return nil, errors.New("dispatch: tickets per shard must be positive")
	case deps.MailboxPerEntity <= 0:
		return nil, errors.New("dispatch: mailbox per entity must be positive")
	case deps.Tick < 0:
		return nil, errors.New("dispatch: tick must not be negative")
	}
	if deps.Tick == 0 {
		deps.Tick = 200 * time.Millisecond
	}
	if deps.Log == nil {
		deps.Log = slog.New(slog.DiscardHandler)
	}
	if deps.NewTicker == nil {
		deps.NewTicker = func(_ int, interval time.Duration) (<-chan time.Time, func()) {
			ticker := time.NewTicker(interval)
			return ticker.C, ticker.Stop
		}
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}

	d := &Dispatcher{
		deps:        deps,
		seed:        maphash.MakeSeed(),
		shards:      make([]*Shard, deps.Shards),
		spaces:      make(map[Space]string),
		spaceNames:  make(map[string]struct{}),
		closing:     make(chan struct{}),
		workersDone: make(chan struct{}),
		stopped:     make(chan struct{}),
	}
	for i := range d.shards {
		d.shards[i] = newShard(d, i)
	}
	if deps.Metrics != nil {
		d.bindMetrics(deps.Metrics)
	}
	return d, nil
}

func (d *Dispatcher) bindMetrics(set *metrics.Set) {
	d.panics = set.GetOrCreateCounter(`yabot_dispatch_panics_total{route="none"}`)
	d.lateComplete = set.GetOrCreateCounter(`yabot_dispatch_ticket_late_total{op="complete"}`)
	d.lateCancel = set.GetOrCreateCounter(`yabot_dispatch_ticket_late_total{op="cancel"}`)
	d.lateDone = set.GetOrCreateCounter("yabot_dispatch_run_on_all_done_late_total")
	d.mailboxOverflow = set.GetOrCreateCounter("yabot_dispatch_mailbox_overflow_total")
	for i, sh := range d.shards {
		label := fmt.Sprintf(`{shard="%d"}`, i)
		set.GetOrCreateGauge("yabot_dispatch_inbox_depth"+label, func() float64 {
			return float64(len(sh.inbox))
		})
		set.GetOrCreateGauge("yabot_dispatch_tickets_inuse"+label, func() float64 {
			return float64(sh.inUse.Load())
		})
	}
}

func (d *Dispatcher) Name() string { return "dispatch" }

// Start succeeds at most once. Shard contexts have a lifetime controlled by Stop.
func (d *Dispatcher) Start(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state != stateCreated {
		return errors.New("dispatch: dispatcher has already started")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	d.cancelRun = cancel
	d.state = stateRunning

	var workers sync.WaitGroup
	for _, sh := range d.shards {
		workers.Go(func() { sh.run(runCtx) })
	}
	go func() {
		workers.Wait()
		close(d.workersDone)
	}()
	return nil
}

// Stop drains accepted tasks even if its context expires. The first Stop after Start
// supplies the deadline for all concurrent and subsequent Stop calls.
func (d *Dispatcher) Stop(ctx context.Context) error {
	d.mu.Lock()
	switch d.state {
	case stateCreated:
		d.mu.Unlock()
		return nil
	case stateStopping, stateStopped:
		d.mu.Unlock()
		<-d.stopped
		return d.stopErr
	}
	d.state = stateStopping
	close(d.closing)
	d.mu.Unlock()

	var err error
	select {
	case <-d.workersDone:
	case <-ctx.Done():
		select {
		case <-d.workersDone:
		default:
			d.cancelRun()
			<-d.workersDone
			var inFlight int64
			for _, sh := range d.shards {
				inFlight += sh.inUse.Load()
			}
			err = fmt.Errorf("dispatch: stopped with %d in-flight tickets: %w", inFlight, ctx.Err())
		}
	}
	d.cancelRun()

	d.mu.Lock()
	d.stopErr = err
	d.state = stateStopped
	close(d.stopped)
	d.mu.Unlock()
	return err
}

// RegisterSpace requires a unique name and Space. Registration closes at Start.
func (d *Dispatcher) RegisterSpace(name string, sp Space) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state != stateCreated {
		panic("dispatch: spaces must be registered before Start")
	}
	if name == "" {
		panic("dispatch: space name must not be empty")
	}
	if owner, exists := d.spaces[sp]; exists {
		panic(fmt.Sprintf("dispatch: space %d is already registered as %q", sp, owner))
	}
	if _, exists := d.spaceNames[name]; exists {
		panic(fmt.Sprintf("dispatch: space name %q is already registered", name))
	}
	d.spaces[sp] = name
	d.spaceNames[name] = struct{}{}
}

// OnTick registers a nonnil callback before Start. Each shard uses registration order.
func (d *Dispatcher) OnTick(fn func(ctx context.Context, sh *Shard)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state != stateCreated {
		panic("dispatch: tick callbacks must be registered before Start")
	}
	if fn == nil {
		panic("dispatch: tick callback must not be nil")
	}
	d.tickFns = append(d.tickFns, fn)
}

// Submit accepts tasks without waiting for inbox space. A nil error guarantees execution,
// including when Submit races with Stop.
func (d *Dispatcher) Submit(k Key, t Task) error {
	return d.submit(k, "", t, nil)
}

func (d *Dispatcher) submit(k Key, route string, t Task, onPanic func(error)) error {
	return d.enqueue(d.shards[d.shardIndex(k)], work{key: k, route: route, task: t, onPanic: onPanic})
}

func (d *Dispatcher) shardIndex(k Key) int {
	return int(maphash.Comparable(d.seed, k) % uint64(len(d.shards)))
}

func (d *Dispatcher) enqueue(sh *Shard, item work) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.state != stateRunning {
		return ErrClosed
	}
	select {
	case sh.inbox <- item:
		return nil
	default:
		return ErrBusy
	}
}

func (d *Dispatcher) isRunning() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.state == stateRunning
}
