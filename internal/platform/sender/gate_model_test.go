package sender

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

type model429 struct {
	at         time.Duration
	chatID     int64
	class      methodClass
	retryAfter time.Duration
}

func (event model429) delay() time.Duration {
	if event.retryAfter <= 0 {
		return time.Second
	}
	return event.retryAfter
}

type gateModelPauses struct {
	chats    map[int64]time.Duration
	methods  [methodClassCount]time.Duration
	explicit time.Duration
}

func modelGatePauses(history []model429) gateModelPauses {
	pauses := gateModelPauses{chats: make(map[int64]time.Duration)}
	for i, event := range history {
		delay := event.delay()
		until := event.at + delay
		if delay > 30*time.Second {
			pauses.explicit = max(pauses.explicit, until)
		}
		if event.chatID == 0 {
			pauses.methods[event.class] = max(pauses.methods[event.class], until)
			continue
		}
		pauses.chats[event.chatID] = max(pauses.chats[event.chatID], until)

		// Later events at the same timestamp must not contribute to this event's window.
		distinct := make(map[int64]struct{})
		var longest time.Duration
		for _, prior := range history[:i+1] {
			if prior.chatID != 0 && prior.class == event.class && event.at-prior.at < 10*time.Second {
				distinct[prior.chatID] = struct{}{}
				longest = max(longest, prior.delay())
			}
		}
		if len(distinct) >= 3 {
			pauses.methods[event.class] = max(pauses.methods[event.class], event.at+longest)
		}
	}
	return pauses
}

func modelActivePause(at, until time.Duration) time.Duration {
	if until > at {
		return until
	}
	return 0
}

func (pauses gateModelPauses) globalAt(at time.Duration) time.Duration {
	derived := min(pauses.methods[methodJoinRequest], pauses.methods[methodMessage], pauses.methods[methodOther])
	return modelActivePause(at, max(pauses.explicit, derived))
}

func (pauses gateModelPauses) summaryAt(at time.Duration) (time.Duration, gateCounts) {
	var next time.Duration
	consider := func(until time.Duration) bool {
		if until <= at {
			return false
		}
		if next == 0 || until < next {
			next = until
		}
		return true
	}
	consider(pauses.explicit)
	var counts gateCounts
	for _, until := range pauses.chats {
		if consider(until) {
			counts.chats++
		}
	}
	for _, until := range pauses.methods {
		if consider(until) {
			counts.methods++
		}
	}
	if pauses.globalAt(at) > 0 {
		counts.global = 1
	}
	return next, counts
}

func (pauses gateModelPauses) expiredBetween(advancedThrough, at time.Duration) []int64 {
	var expired []int64
	for chatID, until := range pauses.chats {
		if advancedThrough < until && until <= at {
			expired = append(expired, chatID)
		}
	}
	slices.Sort(expired)
	return expired
}

func modelGateTime(start time.Time, offset time.Duration) time.Time {
	if offset == 0 {
		return time.Time{}
	}
	return start.Add(offset)
}

func TestRateGateAgainstHistory(t *testing.T) {
	for sequence := range 256 {
		t.Run(fmt.Sprintf("sequence_%03d", sequence), func(t *testing.T) {
			checkRateGateSequence(t, rand.New(rand.NewPCG(429, uint64(sequence))))
		})
	}
}

func checkRateGateSequence(t *testing.T, random *rand.Rand) {
	t.Helper()
	start := time.Unix(100, 0)
	steps := []time.Duration{0, 0, time.Nanosecond, time.Second, 2 * time.Second, 4 * time.Second,
		10*time.Second - time.Nanosecond, 10 * time.Second, 10*time.Second + time.Nanosecond}
	delays := []time.Duration{-time.Second, 0, time.Second, 2 * time.Second, 5 * time.Second,
		29 * time.Second, 30 * time.Second, 31 * time.Second, 45 * time.Second}
	chats := []int64{-2, -1, 0, 1, 2}
	classes := []methodClass{methodJoinRequest, methodMessage, methodOther}
	var gate rateGate
	var history []model429
	var pauses gateModelPauses
	var at, advancedThrough time.Duration
	for step := range 16 {
		recovery, _ := pauses.summaryAt(at)
		if recovery > 0 && random.IntN(5) == 0 {
			at = recovery
			checkRateGateQueries(t, &gate, pauses, start, at, step)
		} else {
			at += steps[random.IntN(len(steps))]
		}
		event := model429{
			at:         at,
			chatID:     chats[random.IntN(len(chats))],
			class:      classes[random.IntN(len(classes))],
			retryAfter: delays[random.IntN(len(delays))],
		}
		before := pauses
		history = append(history, event)
		pauses = modelGatePauses(history)
		want := gateUpdate{
			methodUntil: modelGateTime(start, modelActivePause(at, pauses.methods[event.class])),
			globalUntil: modelGateTime(start, pauses.globalAt(at)),
		}
		if pauses.chats[event.chatID] > before.chats[event.chatID] {
			want.chatUntil = modelGateTime(start, pauses.chats[event.chatID])
		}
		if got := gate.record429(start.Add(at), event.chatID, event.class, event.retryAfter); got != want {
			t.Fatalf("step %d, record429(%s, %d, %s, %s) = %+v, want %+v",
				step, at, event.chatID, event.class, event.retryAfter, got, want)
		}
		checkRateGateQueries(t, &gate, pauses, start, at, step)
		checkRateGateAdvance(t, &gate, pauses, start, advancedThrough, at)
		advancedThrough = at
	}
	at += time.Minute
	checkRateGateQueries(t, &gate, pauses, start, at, len(history))
	checkRateGateAdvance(t, &gate, pauses, start, advancedThrough, at)
	checkRateGateAdvance(t, &gate, pauses, start, at, at)
}

func checkRateGateQueries(t *testing.T, gate *rateGate, pauses gateModelPauses, start time.Time, at time.Duration, step int) {
	t.Helper()
	now := start.Add(at)
	global := pauses.globalAt(at)
	if got, ok := gate.globalPause(now); !got.Equal(modelGateTime(start, global)) || ok != (global > 0) {
		t.Fatalf("step %d at %s: globalPause() = (%v, %v), want deadline %v", step, at, got, ok, modelGateTime(start, global))
	}
	next, counts := pauses.summaryAt(at)
	if got, ok := gate.nextRecovery(now); !got.Equal(modelGateTime(start, next)) || ok != (next > 0) {
		t.Fatalf("step %d at %s: nextRecovery() = (%v, %v), want deadline %v", step, at, got, ok, modelGateTime(start, next))
	}
	if got := gate.counts(now); got != counts {
		t.Fatalf("step %d at %s: counts() = %+v, want %+v", step, at, got, counts)
	}
	for _, chatID := range []int64{-2, -1, 0, 1, 2, 99} {
		for _, class := range []methodClass{methodJoinRequest, methodMessage, methodOther} {
			until := modelActivePause(at, max(pauses.chats[chatID], pauses.methods[class], global))
			want := modelGateTime(start, until)
			if got, ok := gate.blockedUntil(now, chatID, class); !got.Equal(want) || ok != (until > 0) {
				t.Fatalf("step %d at %s: blockedUntil(%d, %s) = (%v, %v), want deadline %v", step, at, chatID, class, got, ok, want)
			}
		}
	}
}

func checkRateGateAdvance(t *testing.T, gate *rateGate, pauses gateModelPauses, start time.Time, advancedThrough, at time.Duration) {
	t.Helper()
	got := gate.advance(start.Add(at))
	slices.Sort(got)
	if want := pauses.expiredBetween(advancedThrough, at); !slices.Equal(got, want) {
		t.Fatalf("advance at %s after %s = %v, want %v", at, advancedThrough, got, want)
	}
}
