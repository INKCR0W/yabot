package sender

import (
	"container/heap"
	"time"
)

func (s *Sender) run() {
	finished := false
	defer func() {
		if !finished {
			// A Done callback can exit this goroutine. The remaining work stays owned by Sender.
			go s.run()
		}
	}()

	for {
		s.mu.Lock()
		s.loops++
		now := time.Now()
		s.advanceStop(now)
		for _, chat := range s.gate.advance(now) {
			s.queue.unpark(chat)
		}
		s.observeGate(now)
		s.expireAdmissions()
		s.promoteRetries(now)
		if len(s.pauseLogs) > 0 {
			logs := s.pauseLogs
			s.pauseLogs = nil
			s.mu.Unlock()
			for _, paused := range logs {
				if paused {
					s.deps.Log.Warn("sender entered global pause")
				} else {
					s.deps.Log.Info("sender left global pause")
				}
			}
			continue
		}
		if len(s.completions) > 0 {
			c := s.completions[0]
			s.completions[0] = completion{}
			s.completions = s.completions[1:]
			if len(s.completions) == 0 {
				s.completions = nil
			}
			s.mu.Unlock()
			s.deliver(c, now)
			continue
		}
		if s.state == senderStopping && s.liveWorkers == 0 && s.queue.empty() && len(s.retries) == 0 {
			s.finishStop(now)
			finished = true
			s.mu.Unlock()
			return
		}

		wakeAt, _ := s.gate.nextRecovery(now)
		if len(s.retries) > 0 {
			wakeAt = earlierPause(wakeAt, s.retries[0].retryAt)
		}
		if s.state == senderStopping && !s.abort {
			wakeAt = earlierPause(wakeAt, s.stopAt)
		}
		if !s.globalPause && !s.queue.empty() {
			at, dispatched := s.dispatch(now)
			if dispatched || len(s.completions) > 0 {
				s.mu.Unlock()
				continue
			}
			wakeAt = earlierPause(wakeAt, at)
		}
		s.mu.Unlock()
		if !wakeAt.After(now) {
			wakeAt = time.Time{}
		}
		s.wait(wakeAt)
	}
}

func (s *Sender) wait(at time.Time) {
	var ready <-chan time.Time
	if !at.IsZero() {
		timer := time.NewTimer(time.Until(at))
		defer timer.Stop()
		ready = timer.C
	}
	select {
	case <-s.wake:
	case <-ready:
	}
}

func (s *Sender) dispatch(now time.Time) (time.Time, bool) {
	if !s.global.take(now) {
		ready, _ := s.global.next(now)
		return ready, false
	}
	var wakeAt time.Time
	entry, expired := s.queue.next(now, func(candidate *item) bool {
		r := candidate.request
		wakeAt = earlierPause(wakeAt, r.Deadline)
		if s.liveWorkers == s.deps.Workers || !s.quota.canTake(r.Class) {
			return false
		}
		if _, blocked := s.gate.blockedUntil(now, r.ChatID, candidate.method); blocked {
			return false
		}
		if !s.chatBuckets.has(now, r.ChatID, candidate.method) {
			ready, _ := s.chatBuckets.next(now, r.ChatID, candidate.method)
			wakeAt = earlierPause(wakeAt, ready)
			return false
		}
		return true
	})
	for _, entry := range expired {
		s.queueDrop(entry, Expired)
	}
	if entry == nil {
		s.global.refund(now)
		return wakeAt, false
	}
	r := entry.request
	if !s.chatBuckets.take(now, r.ChatID, entry.method) {
		panic("sender: selected request has no chat token")
	}
	run := &call{entry: entry}
	s.calls[run] = struct{}{}
	s.liveWorkers++
	s.quota.busy[r.Class]++
	s.metrics.setBusy(r.Class, s.quota.busy[r.Class])
	s.metrics.setDepth(r.Class, s.queue.depth(r.Class))
	if s.onDispatch != nil {
		s.onDispatch(now, entry)
	}
	go s.work(run)
	return time.Time{}, true
}

func (s *Sender) expireAdmissions() {
	for key := range s.expireChats {
		expired := s.queue.removeChat(key.class, key.chat, func(entry *item) bool {
			return entry.expireOnAdmission
		})
		for _, entry := range expired {
			s.queueDrop(entry, Expired)
		}
		delete(s.expireChats, key)
	}
}

func (s *Sender) promoteRetries(now time.Time) {
	for len(s.retries) > 0 && !now.Before(s.retries[0].retryAt) {
		entry := heap.Pop(&s.retries).(*item)
		_, expires := s.pauseStatus(now, entry)
		if expires || expiresBy(entry, now) {
			s.queueDrop(entry, Expired)
		} else {
			s.requeue(entry)
		}
	}
}

func (s *Sender) requeue(entry *item) {
	s.queue.pushFront(entry)
	class := entry.request.Class
	s.metrics.setDepth(class, s.queue.depth(class))
}

func (s *Sender) queueDrop(entry *item, reason DropReason) {
	s.completions = append(s.completions, completion{entry, drop(entry.result, reason)})
	class := entry.request.Class
	s.metrics.setDepth(class, s.queue.depth(class))
}

func expiresBy(entry *item, at time.Time) bool {
	deadline := entry.request.Deadline
	return !deadline.IsZero() && !at.Before(deadline)
}

func (s *Sender) pauseStatus(now time.Time, entry *item) (blocked, expires bool) {
	until, blocked := s.gate.blockedUntil(now, entry.request.ChatID, entry.method)
	return blocked, blocked && expiresBy(entry, until)
}

func (s *Sender) observeGate(now time.Time) {
	_, paused := s.gate.globalPause(now)
	if paused != s.globalPause {
		s.globalPause = paused
		s.pauseLogs = append(s.pauseLogs, paused)
	}
	if s.metrics.set != nil {
		s.metrics.setGate(s.gate.counts(now))
	}
}

func (s *Sender) record429(now time.Time, chat int64, method methodClass, retryAfter time.Duration) {
	s.observeGate(now)
	methodUntil := activePause(now, s.gate.methods[method])
	globalUntil, _ := s.gate.globalPause(now)
	update := s.gate.record429(now, chat, method, retryAfter)
	s.observeGate(now)
	chatExtended := !update.chatUntil.IsZero()
	if chatExtended {
		s.queue.park(chat)
	}
	methodExtended := update.methodUntil.After(methodUntil)
	globalExtended := update.globalUntil.After(globalUntil)
	if !chatExtended && !methodExtended && !globalExtended {
		return
	}
	expires := func(entry *item) bool {
		_, expires := s.pauseStatus(now, entry)
		return expires
	}
	for class := Deadline; class <= Background; class++ {
		var expired []*item
		s.pauseScans++
		if methodExtended || globalExtended {
			s.fullPauseScans++
			expired = s.queue.removeClass(class, func(entry *item) bool {
				affected := globalExtended || (methodExtended && entry.method == method) ||
					(chatExtended && entry.request.ChatID == chat)
				return affected && expires(entry)
			})
		} else {
			expired = s.queue.removeChat(class, chat, expires)
		}
		for _, entry := range expired {
			s.queueDrop(entry, Expired)
		}
	}
}
