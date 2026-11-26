package sender

import (
	"slices"
	"time"
)

const (
	gateWindow          = 10 * time.Second
	gateWindowLimit     = 4_096
	gateChatThreshold   = 3
	gateGlobalThreshold = 30 * time.Second
)

// A rateGate is ready to use at its zero value and needs external synchronization.
type rateGate struct {
	chats       map[int64]time.Time
	methods     [methodClassCount]time.Time
	globalUntil time.Time
	windows     [methodClassCount]retryWindow
}

type gateUpdate struct {
	// chatUntil is set only when the input chat is newly paused or its pause is extended.
	chatUntil time.Time
	// methodUntil and globalUntil report active pauses after the update. Zero means inactive.
	methodUntil   time.Time
	globalUntil   time.Time
	enteredGlobal bool
}

type gateCounts struct {
	chats   int
	methods int
	global  int
}

func (g *rateGate) record429(now time.Time, chatID int64, class methodClass, retryAfter time.Duration) gateUpdate {
	_, wasGlobal := g.globalPause(now)
	if retryAfter <= 0 {
		retryAfter = time.Second
	}
	until := now.Add(retryAfter)
	var update gateUpdate
	if chatID == 0 {
		g.methods[class] = laterPause(g.methods[class], until)
	} else {
		if previous := g.chats[chatID]; previous.IsZero() || previous.Before(until) {
			if g.chats == nil {
				g.chats = make(map[int64]time.Time)
			}
			g.chats[chatID] = until
			update.chatUntil = until
		}
		if delay := g.windows[class].record(now, chatID, retryAfter); delay > 0 {
			g.methods[class] = laterPause(g.methods[class], now.Add(delay))
		}
	}
	if retryAfter > gateGlobalThreshold {
		g.globalUntil = laterPause(g.globalUntil, until)
	}
	update.methodUntil = activePause(now, g.methods[class])
	globalUntil, isGlobal := g.globalPause(now)
	update.globalUntil = globalUntil
	update.enteredGlobal = isGlobal && !wasGlobal
	return update
}

func (g *rateGate) blockedUntil(now time.Time, chatID int64, class methodClass) (time.Time, bool) {
	global, _ := g.globalPause(now)
	until := laterPause(g.chats[chatID], g.methods[class])
	until = activePause(now, laterPause(until, global))
	return until, !until.IsZero()
}

func (g *rateGate) globalPause(now time.Time) (time.Time, bool) {
	explicit := activePause(now, g.globalUntil)
	var earliest time.Time
	for _, until := range g.methods {
		if !now.Before(until) || until.IsZero() {
			return explicit, !explicit.IsZero()
		}
		earliest = earlierPause(earliest, until)
	}
	return laterPause(explicit, earliest), true
}

func (g *rateGate) nextRecovery(now time.Time) (time.Time, bool) {
	next := activePause(now, g.globalUntil)
	for _, until := range g.methods {
		next = earlierPause(next, activePause(now, until))
	}
	for _, until := range g.chats {
		next = earlierPause(next, activePause(now, until))
	}
	return next, !next.IsZero()
}

// advance returns each expired chat once, in unspecified order.
func (g *rateGate) advance(now time.Time) []int64 {
	var expired []int64
	for chatID, until := range g.chats {
		if !now.Before(until) {
			expired = append(expired, chatID)
			delete(g.chats, chatID)
		}
	}
	for class := range g.windows {
		g.windows[class].expire(now)
	}
	return expired
}

func (g *rateGate) counts(now time.Time) gateCounts {
	var counts gateCounts
	for _, until := range g.chats {
		if !activePause(now, until).IsZero() {
			counts.chats++
		}
	}
	for _, until := range g.methods {
		if !activePause(now, until).IsZero() {
			counts.methods++
		}
	}
	if _, paused := g.globalPause(now); paused {
		counts.global = 1
	}
	return counts
}

func activePause(now, until time.Time) time.Time {
	if now.Before(until) {
		return until
	}
	return time.Time{}
}

func laterPause(a, b time.Time) time.Time {
	if a.IsZero() || (!b.IsZero() && b.After(a)) {
		return b
	}
	return a
}

func earlierPause(a, b time.Time) time.Time {
	if a.IsZero() || (!b.IsZero() && b.Before(a)) {
		return b
	}
	return a
}

type retryRecord struct {
	at         time.Time
	chatID     int64
	retryAfter time.Duration
}

type retryWindow []retryRecord

func (w *retryWindow) record(now time.Time, chatID int64, retryAfter time.Duration) time.Duration {
	w.expire(now)
	if len(*w) == gateWindowLimit {
		copy(*w, (*w)[1:])
		*w = (*w)[:gateWindowLimit-1]
	}
	*w = append(*w, retryRecord{at: now, chatID: chatID, retryAfter: retryAfter})
	var distinct [gateChatThreshold]int64
	var count int
	var longest time.Duration
	for _, record := range *w {
		longest = max(longest, record.retryAfter)
		if count < len(distinct) && !slices.Contains(distinct[:count], record.chatID) {
			distinct[count] = record.chatID
			count++
		}
	}
	if count >= gateChatThreshold {
		return longest
	}
	return 0
}

func (w *retryWindow) expire(now time.Time) {
	*w = slices.DeleteFunc(*w, func(record retryRecord) bool {
		return now.Sub(record.at) >= gateWindow
	})
}
