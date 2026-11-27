package sender

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

type workerQuota struct {
	reserved [Background + 1]int
	caps     [Background + 1]int
	busy     [Background + 1]int
	shared   int
}

func newWorkerQuota(deps Deps) workerQuota {
	return workerQuota{
		reserved: [Background + 1]int{Deadline: deps.ReservedDeadline, Decision: deps.ReservedDecision},
		caps:     [Background + 1]int{Interactive: deps.CapInteractive, Background: deps.CapBackground},
		shared:   deps.Workers - deps.ReservedDeadline - deps.ReservedDecision,
	}
}

func (q *workerQuota) canTake(class Class) bool {
	if q.busy[class] < q.reserved[class] {
		return true
	}
	sharedUsed := 0
	for c := Deadline; c <= Background; c++ {
		sharedUsed += max(0, q.busy[c]-q.reserved[c])
	}
	return sharedUsed < q.shared && (q.caps[class] == 0 || q.busy[class] < q.caps[class])
}

type call struct {
	entry  *item
	cancel context.CancelFunc
}

type completion struct {
	entry  *item
	result Result
}

func (s *Sender) work(run *call) {
	defer s.workerExited()
	if s.beforeAttempt != nil {
		s.beforeAttempt(run.entry)
	}
	ctx := s.beginAttempt(run)
	if ctx == nil {
		return
	}

	var err error
	var outcome telegram.Outcome
	returned := false
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("sender: Do panicked: %v", value)
			outcome = telegram.Classify(err)
		} else if !returned {
			err = errors.New("sender: Do called runtime.Goexit")
			outcome = telegram.Classify(err)
		}
		run.cancel()
		s.finishAttempt(run, outcome, err)
	}()
	err = run.entry.request.Do(ctx, s.deps.Bot)
	outcome = telegram.Classify(err)
	returned = true
}

func (s *Sender) beginAttempt(run *call) context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.advanceStop(now)
	entry := run.entry
	r := entry.request
	blocked, expires := s.pauseStatus(now, entry)
	switch {
	case s.shuttingDown(r.Class):
		s.queueDrop(entry, Shutdown)
	case expires || expiresBy(entry, now):
		s.queueDrop(entry, Expired)
	case blocked:
		s.requeue(entry)
	default:
		timeout := 10 * time.Second
		switch r.Class {
		case Deadline:
			timeout = 3 * time.Second
		case Interactive:
			timeout = 5 * time.Second
		}
		at := earlierPause(now.Add(timeout), r.Deadline)
		ctx, cancel := context.WithDeadline(s.base, at)
		run.cancel = cancel
		return ctx
	}
	s.global.refund(now)
	s.chatBuckets.refund(now, r.ChatID, entry.method)
	s.releaseCall(run)
	return nil
}

func (s *Sender) finishAttempt(run *call, outcome telegram.Outcome, err error) {
	entry := run.entry
	r := entry.request
	s.lockAttemptResult()
	now := time.Now()
	s.advanceStop(now)
	result := Result{
		Outcome:   outcome,
		Err:       err,
		Attempts:  entry.result.Attempts + 1,
		Ambiguous: entry.result.Ambiguous || (outcome.Class == telegram.Retry && outcome.RetryAfter == 0),
	}
	entry.result = result
	rateLimited := outcome.Class == telegram.Retry && outcome.RetryAfter > 0
	var expiresDuringPause bool
	if rateLimited {
		s.record429(now, r.ChatID, entry.method, outcome.RetryAfter)
		_, expiresDuringPause = s.pauseStatus(now, entry)
	}
	s.releaseCall(run)

	complete := true
	switch {
	case s.shuttingDown(r.Class):
		result = drop(result, Shutdown)
	case expiresDuringPause:
		result = drop(result, Expired)
	case outcome.Class == telegram.Retry && result.Attempts <= s.deps.MaxRetries:
		complete = !s.retry(entry, now, rateLimited)
	}
	s.mu.Unlock()

	s.metrics.countAttempt(r, outcome.Class)
	if outcome.Class == telegram.Fatal {
		s.deps.Log.Error("sender request failed", "method", r.Method, "chat_id", r.ChatID, "error", err)
	} else if !complete && outcome.RetryAfter == 0 {
		s.deps.Log.Info("sender request retrying", "method", r.Method, "chat_id", r.ChatID, "attempt", result.Attempts, "error", err)
	}
	if complete {
		s.deliver(completion{entry, result}, now)
	}
}

func (s *Sender) lockAttemptResult() {
	// Simulate a delayed lock acquisition without blocking a mutex in synctest.
	if s.beforeAttemptResultLock != nil {
		s.beforeAttemptResultLock()
	}
	s.mu.Lock()
}

func (s *Sender) releaseCall(run *call) {
	delete(s.calls, run)
	class := run.entry.request.Class
	s.quota.busy[class]--
	s.metrics.setBusy(class, s.quota.busy[class])
	s.signal()
}

func (s *Sender) workerExited() {
	s.mu.Lock()
	s.liveWorkers--
	s.signal()
	s.mu.Unlock()
}

func (s *Sender) retry(entry *item, now time.Time, rateLimited bool) bool {
	if rateLimited {
		s.requeue(entry)
		return true
	}
	entry.retryAt = now.Add(s.retryDelay(entry.result.Attempts))
	if expiresBy(entry, entry.retryAt) {
		return false
	}
	heap.Push(&s.retries, entry)
	return true
}

func (s *Sender) retryDelay(attempt int) time.Duration {
	const base = 200 * time.Millisecond
	const limit = 5 * time.Second
	delay := min(base<<min(attempt-1, 5), limit)
	return delay/2 + time.Duration(s.retryRandom(int64(delay-delay/2)+1))
}

type retryHeap []*item

func (h retryHeap) Len() int           { return len(h) }
func (h retryHeap) Less(i, j int) bool { return h[i].retryAt.Before(h[j].retryAt) }
func (h retryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *retryHeap) Push(entry any)    { *h = append(*h, entry.(*item)) }
func (h *retryHeap) Pop() any {
	last := len(*h) - 1
	entry := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return entry
}

func drop(result Result, reason DropReason) Result {
	result.Dropped = reason
	result.Outcome.Class = telegram.Retry
	return result
}

func (s *Sender) deliver(c completion, now time.Time) {
	r := c.entry.request
	s.metrics.countResult(r.Class, c.result.Dropped, now.Sub(c.entry.enqueuedAt))
	defer func() {
		if value := recover(); value != nil {
			s.metrics.countCallbackPanic()
			s.deps.Log.Error("sender Done callback panicked", "method", r.Method, "chat_id", r.ChatID, "panic", value)
		}
	}()
	r.Done(c.result)
}
