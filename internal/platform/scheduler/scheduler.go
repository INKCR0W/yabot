package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/VictoriaMetrics/metrics"
)

const RetryDelay = 50 * time.Millisecond

// Submitter must return promptly and accept the task only on a nil error.
type Submitter interface {
	Submit(k dispatch.Key, t dispatch.Task) error
}

type Deps struct {
	Dispatcher Submitter
	Clock      Clock
	Log        *slog.Logger
	Metrics    *metrics.Set
}

// Scheduler must be created with New.
type Scheduler struct {
	dispatcher Submitter
	clock      Clock
	log        *slog.Logger
	wake       chan struct{}
	stop       chan struct{}
	done       chan struct{}

	mu       sync.Mutex
	pending  taskHeap
	sequence uint64
	started  bool
	stopping bool

	dropped      *metrics.Counter
	busyRetries  *metrics.Counter
	submitErrors *metrics.Counter
}

func New(deps Deps) (*Scheduler, error) {
	if deps.Dispatcher == nil {
		return nil, errors.New("scheduler: dispatcher is required")
	}
	if deps.Clock == nil {
		deps.Clock = SystemClock()
	}
	if deps.Log == nil {
		deps.Log = slog.New(slog.DiscardHandler)
	}
	s := &Scheduler{
		dispatcher: deps.Dispatcher,
		clock:      deps.Clock,
		log:        deps.Log,
		wake:       make(chan struct{}, 1),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	if set := deps.Metrics; set != nil {
		set.GetOrCreateGauge("yabot_scheduler_pending", s.pendingCount)
		s.dropped = set.GetOrCreateCounter("yabot_scheduler_dropped_total")
		s.busyRetries = set.GetOrCreateCounter("yabot_scheduler_busy_retries_total")
		s.submitErrors = set.GetOrCreateCounter("yabot_scheduler_submit_errors_total")
	}
	return s, nil
}

func (s *Scheduler) Name() string { return "scheduler" }

// Start succeeds at most once. The worker runs until Stop is called.
func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return errors.New("scheduler: already started")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.started = true
	go s.run()
	return nil
}

// After Start, Stop discards pending tasks; modules rebuild timers during recovery after a restart.
// Before Start, Stop is a no-op and queued tasks run once the scheduler starts.
// If ctx expires, a later Stop can still wait for the worker to exit.
func (s *Scheduler) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return nil
	}
	if !s.stopping {
		s.stopping = true
		close(s.stop)
	}
	s.mu.Unlock()

	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		select {
		case <-s.done:
			return nil
		default:
			return ctx.Err()
		}
	}
}

// At is safe for concurrent use and panics for a nil task.
// Deadlines are anchored to the clock's monotonic reading at insertion when available.
// There is no cancellation: tasks check their own validity when due, and callers' timer counts determine heap size.
func (s *Scheduler) At(at time.Time, k dispatch.Key, t dispatch.Task) {
	if t == nil {
		panic("scheduler: task must not be nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		s.countDropped(1)
		return
	}
	now := s.clock.Now()
	at = now.Add(at.Sub(now))
	item := &scheduledTask{at: at, sequence: s.sequence, key: k, task: t}
	s.sequence++
	heap.Push(&s.pending, item)
	if s.pending[0] == item {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

func (s *Scheduler) run() {
	defer close(s.done)
	defer s.discardPending()
	var timer Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	var armedAt time.Time
	var ticks <-chan time.Time
	for {
		at, pending := s.submitDue()
		switch {
		case !pending && ticks != nil:
			timer.Stop()
			ticks = nil
		case pending && (ticks == nil || !armedAt.Equal(at)):
			if timer == nil {
				timer = s.clock.NewTimer(at.Sub(s.clock.Now()))
			} else {
				timer.Reset(at.Sub(s.clock.Now()))
			}
			armedAt, ticks = at, timer.C()
			// The clock may have reached at while the timer was being armed.
			if !at.After(s.clock.Now()) {
				continue
			}
		}
		select {
		case <-s.stop:
			return
		case <-s.wake:
		case <-ticks:
			ticks = nil
		}
	}
}

func (s *Scheduler) submitDue() (next time.Time, pending bool) {
	for {
		now := s.clock.Now()
		s.mu.Lock()
		if s.stopping || len(s.pending) == 0 {
			s.mu.Unlock()
			return time.Time{}, false
		}
		item := s.pending[0]
		if item.at.After(now) {
			s.mu.Unlock()
			return item.at, true
		}
		heap.Pop(&s.pending)
		s.mu.Unlock()
		s.submit(item)
	}
}

func (s *Scheduler) submit(item *scheduledTask) {
	err := s.dispatcher.Submit(item.key, item.task)
	if err == nil {
		return
	}
	item.at = s.clock.Now().Add(RetryDelay)
	if errors.Is(err, dispatch.ErrBusy) {
		if s.busyRetries != nil {
			s.busyRetries.Inc()
		}
		if !item.loggedBusy {
			item.loggedBusy = true
			s.log.Info("scheduler retrying task after busy dispatcher", "key", item.key, "retry_in", RetryDelay)
		}
	} else {
		if s.submitErrors != nil {
			s.submitErrors.Inc()
		}
		if !item.loggedError {
			item.loggedError = true
			s.log.Error("scheduler could not submit task", "key", item.key, "error", err)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		s.countDropped(1)
		return
	}
	heap.Push(&s.pending, item)
}

func (s *Scheduler) pendingCount() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return float64(len(s.pending))
}

func (s *Scheduler) discardPending() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.countDropped(len(s.pending))
	s.pending = nil
}

func (s *Scheduler) countDropped(n int) {
	if s.dropped != nil {
		s.dropped.Add(n)
	}
}

type scheduledTask struct {
	at          time.Time
	sequence    uint64
	key         dispatch.Key
	task        dispatch.Task
	loggedBusy  bool
	loggedError bool
}

type taskHeap []*scheduledTask

func (h taskHeap) Len() int { return len(h) }

func (h taskHeap) Less(i, j int) bool {
	if order := h[i].at.Compare(h[j].at); order != 0 {
		return order < 0
	}
	return h[i].sequence < h[j].sequence
}

func (h taskHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *taskHeap) Push(item any) { *h = append(*h, item.(*scheduledTask)) }

func (h *taskHeap) Pop() any {
	last := len(*h) - 1
	item := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return item
}
