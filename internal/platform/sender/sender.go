package sender

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot"
)

type Deps struct {
	Bot              *bot.Bot
	Log              *slog.Logger
	Metrics          *metrics.Set
	Workers          int
	ReservedDeadline int
	ReservedDecision int
	CapInteractive   int
	CapBackground    int
	GlobalRPS        float64
	GlobalBurst      int
	ClassWeights     [4]int // Deadline, Decision, Interactive, Background.
	ChatQueue        int
	MaxRetries       int
}

type senderState uint8

const (
	senderCreated senderState = iota
	senderRunning
	senderStopping
	senderStopped
)

// Sender must be constructed with New and cannot be restarted after Stop.
type Sender struct {
	deps             Deps
	metrics          senderMetrics
	wake             chan struct{}
	done             chan struct{}
	healthPauseUntil atomic.Pointer[time.Time]

	mu          sync.Mutex
	state       senderState
	base        context.Context
	stopAt      time.Time
	stopWaiters map[*stopWaiter]struct{}
	abort       bool
	queue       *queue
	global      tokenBucket
	chatBuckets *chatBuckets
	gate        rateGate
	quota       workerQuota
	calls       map[*call]struct{}
	liveWorkers int
	retries     retryHeap
	expireChats map[classChat]struct{}
	completions []completion
	globalPause bool
	pauseLogs   []bool

	loops                   uint64
	pauseScans              uint64
	fullPauseScans          uint64
	onDispatch              func(time.Time, *item)
	beforeAttempt           func(*item)
	beforeAttemptResultLock func()
	retryRandom             func(int64) int64
}

type classChat struct {
	class Class
	chat  int64
}

func New(deps Deps) (*Sender, error) {
	if err := validateDeps(deps); err != nil {
		return nil, err
	}
	if deps.Log == nil {
		deps.Log = slog.New(slog.DiscardHandler)
	}
	var weights [Background + 1]int
	copy(weights[Deadline:], deps.ClassWeights[:])
	return &Sender{
		deps:        deps,
		metrics:     newSenderMetrics(deps.Metrics),
		wake:        make(chan struct{}, 1),
		done:        make(chan struct{}),
		queue:       newQueue(deps.ChatQueue, weights),
		chatBuckets: newChatBuckets(),
		quota:       newWorkerQuota(deps),
		calls:       make(map[*call]struct{}),
		expireChats: make(map[classChat]struct{}),
		stopWaiters: make(map[*stopWaiter]struct{}),
		retryRandom: rand.Int64N,
	}, nil
}

func validateDeps(deps Deps) error {
	switch {
	case deps.Bot == nil:
		return errors.New("sender: Bot is required")
	case deps.Workers < 1:
		return errors.New("sender: Workers must be positive")
	case deps.ReservedDeadline < 0 || deps.ReservedDecision < 0:
		return errors.New("sender: reserved worker counts must be nonnegative")
	case deps.ReservedDeadline >= deps.Workers || deps.ReservedDecision >= deps.Workers-deps.ReservedDeadline:
		return errors.New("sender: reservations must leave at least one shared worker")
	case !(deps.GlobalRPS > 0) || math.IsInf(deps.GlobalRPS, 1):
		return errors.New("sender: GlobalRPS must be positive and finite")
	case deps.GlobalBurst < 1:
		return errors.New("sender: GlobalBurst must be positive")
	case deps.ChatQueue < 1:
		return errors.New("sender: ChatQueue must be positive")
	case deps.MaxRetries < 0:
		return errors.New("sender: MaxRetries must be nonnegative")
	}
	shared := deps.Workers - deps.ReservedDeadline - deps.ReservedDecision
	if deps.CapInteractive < 1 || deps.CapInteractive > shared {
		return fmt.Errorf("sender: CapInteractive must be between 1 and %d", shared)
	}
	if deps.CapBackground < 1 || deps.CapBackground > shared {
		return fmt.Errorf("sender: CapBackground must be between 1 and %d", shared)
	}
	for i, weight := range deps.ClassWeights {
		if weight < 1 {
			return fmt.Errorf("sender: %s class weight must be positive", Deadline+Class(i))
		}
	}
	return nil
}

func (*Sender) Name() string { return "sender" }

// HealthCheck reports an active global pause without waiting for the scheduler.
// The error text starts with "degraded:" and names the UTC resume time.
func (s *Sender) HealthCheck() error {
	now := time.Now()
	if until := s.healthPauseUntil.Load(); until != nil && now.Before(*until) {
		return fmt.Errorf("degraded: sender globally paused until %s", until.UTC().Format(time.RFC3339Nano))
	}
	return nil
}

// Start succeeds at most once. Cancellation of ctx does not stop the sender.
func (s *Sender) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != senderCreated {
		return errors.New("sender: cannot start an already started or stopped sender")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.base = context.WithoutCancel(ctx)
	s.global = newTokenBucket(s.deps.GlobalRPS, s.deps.GlobalBurst, time.Now())
	s.state = senderRunning
	go s.run()
	return nil
}

// Enqueue calls Done exactly once on success. Admission errors leave Done to the caller.
func (s *Sender) Enqueue(r Request) error {
	if err := validateRequest(r); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != senderRunning {
		return ErrClosed
	}
	now := time.Now()
	method := classifyMethod(r.Method)
	if r.Class == Deadline && s.wouldMiss(now, r.Class, r.ChatID, method, r.Deadline) {
		return ErrWouldMiss
	}
	entry := &item{request: r, method: method, enqueuedAt: now}
	if err := s.queue.push(entry); err != nil {
		return err
	}
	if _, expires := s.pauseStatus(now, entry); expires {
		entry.expireOnAdmission = true
		s.expireChats[classChat{r.Class, r.ChatID}] = struct{}{}
	}
	s.metrics.setDepth(r.Class, s.queue.depth(r.Class))
	s.signal()
	return nil
}

// WouldMiss is primarily for Deadline requests, for which it assumes a join-request method.
func (s *Sender) WouldMiss(class Class, chatID int64, deadline time.Time) bool {
	if !class.valid() || deadline.IsZero() {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wouldMiss(time.Now(), class, chatID, predictedMethod(class), deadline)
}

func (s *Sender) wouldMiss(now time.Time, class Class, chatID int64, method methodClass, deadline time.Time) bool {
	return !now.Add(s.estimateWait(now, class, chatID, method)).Before(deadline)
}

// EstimateWait uses the same pause and queue estimate as WouldMiss.
func (s *Sender) EstimateWait(class Class, chatID int64) time.Duration {
	if !class.valid() {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.estimateWait(time.Now(), class, chatID, predictedMethod(class))
}

func (s *Sender) estimateWait(now time.Time, class Class, chatID int64, method methodClass) time.Duration {
	estimate := s.queue.queueWait(class, chatID, now, s.deps.GlobalRPS)
	if until, paused := s.gate.blockedUntil(now, chatID, method); paused {
		return max(estimate, until.Sub(now))
	}
	return estimate
}

func predictedMethod(class Class) methodClass {
	if class == Deadline {
		return methodJoinRequest
	}
	return methodOther
}

// ClassWait estimates the time to send the queued requests at the class's current rate share.
func (s *Sender) ClassWait(class Class) time.Duration {
	if !class.valid() {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queue.classWait(class, s.deps.GlobalRPS)
}

// SetChatWeight applies a temporary weight to the chat's Deadline requests.
func (s *Sender) SetChatWeight(chatID int64, w float64, until time.Time) {
	s.mu.Lock()
	valid := s.queue.setWeight(chatID, w, until)
	s.mu.Unlock()
	if !valid {
		s.deps.Log.Warn("sender ignored invalid chat weight", "chat_id", chatID, "weight", w)
	}
}

func (s *Sender) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
