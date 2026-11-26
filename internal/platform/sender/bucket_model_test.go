package sender

import (
	"fmt"
	"math/big"
	"math/rand/v2"
	"testing"
	"time"
)

type tokenBucketReference struct {
	ratePerNS *big.Rat
	capacity  *big.Rat
	balance   *big.Rat
	asOf      time.Time
}

func (m *tokenBucketReference) balanceAt(now time.Time) *big.Rat {
	balance := new(big.Rat).Set(m.balance)
	if elapsed := now.Sub(m.asOf); elapsed > 0 && m.ratePerNS.Sign() > 0 {
		earned := new(big.Rat).Mul(big.NewRat(int64(elapsed), 1), m.ratePerNS)
		balance.Add(balance, earned)
	}
	if balance.Cmp(m.capacity) > 0 {
		balance.Set(m.capacity)
	}
	return balance
}

func (m *tokenBucketReference) has(now time.Time) bool {
	return m.balanceAt(now).Cmp(big.NewRat(1, 1)) >= 0
}

func (m *tokenBucketReference) take(now time.Time) bool {
	one := big.NewRat(1, 1)
	balance := m.balanceAt(now)
	if balance.Cmp(one) < 0 {
		return false
	}
	m.balance = balance.Sub(balance, one)
	if now.After(m.asOf) {
		m.asOf = now
	}
	return true
}

func (m *tokenBucketReference) refund(now time.Time) {
	balance := m.balanceAt(now)
	balance.Add(balance, big.NewRat(1, 1))
	if balance.Cmp(m.capacity) > 0 {
		balance.Set(m.capacity)
	}
	m.balance = balance
	if now.After(m.asOf) {
		m.asOf = now
	}
}

func TestTokenBucketReference(t *testing.T) {
	rates := []struct {
		numerator   int64
		denominator int64
	}{
		{25, 1}, {20, 60}, {1, 1}, {1, 2}, {0, 1}, {-1, 1},
	}
	deltas := []time.Duration{
		0, 0, time.Nanosecond, -time.Nanosecond, -time.Second,
		time.Millisecond, 39 * time.Millisecond, 40*time.Millisecond - time.Nanosecond,
		40 * time.Millisecond, 40*time.Millisecond + time.Nanosecond,
		500 * time.Millisecond, time.Second - time.Nanosecond, time.Second, 3 * time.Second,
	}
	for _, rate := range rates {
		for _, burst := range []int{1, 5} {
			t.Run(fmt.Sprintf("rate_%d_over_%d_burst_%d", rate.numerator, rate.denominator, burst), func(t *testing.T) {
				rng := rand.New(rand.NewPCG(41, 63))
				start := time.Unix(100, 0)
				now := start
				bucket := newTokenBucket(float64(rate.numerator)/float64(rate.denominator), burst, now)
				model := tokenBucketReference{
					ratePerNS: big.NewRat(rate.numerator, rate.denominator*int64(time.Second)),
					capacity:  big.NewRat(int64(burst), 1),
					balance:   big.NewRat(int64(burst), 1),
					asOf:      now,
				}
				for step := range 384 {
					now = now.Add(deltas[rng.IntN(len(deltas))])
					at := now.Sub(start)
					switch rng.IntN(6) {
					case 0:
						if got, want := bucket.has(now), model.has(now); got != want {
							t.Fatalf("step %d at %s: has() = %v, want %v", step, at, got, want)
						}
					case 1:
						checkTokenBucketEstimate(t, &bucket, &model, now, at, step)
					case 2:
						bucket.refund(now)
						model.refund(now)
					default:
						if got, want := bucket.take(now), model.take(now); got != want {
							t.Fatalf("step %d at %s: take() = %v, want %v", step, at, got, want)
						}
					}
				}
			})
		}
	}
}

func checkTokenBucketEstimate(t *testing.T, bucket *tokenBucket, model *tokenBucketReference, now time.Time, at time.Duration, step int) {
	t.Helper()
	got, ok := bucket.next(now)
	switch {
	case model.has(now):
		if !ok || !got.Equal(now) {
			t.Fatalf("step %d at %s: next() = (%v, %v), want (%v, true)", step, at, got, ok, now)
		}
	case model.ratePerNS.Sign() <= 0:
		if ok || !got.IsZero() {
			t.Fatalf("step %d at %s: next() = (%v, %v), want (zero, false)", step, at, got, ok)
		}
	default:
		if !ok || !got.After(now) || !model.has(got) || model.has(got.Add(-2*time.Nanosecond)) {
			t.Fatalf("step %d at %s: next() = (%v, %v), want within 1ns after the first available token", step, at, got, ok)
		}
	}
}

type chatBucketReferenceEntry struct {
	lastTake time.Time
	lastUse  int
}

type chatBucketReference map[int64]chatBucketReferenceEntry

func (m chatBucketReference) next(now time.Time, chatID int64, step int) time.Time {
	entry, ok := m[chatID]
	if !ok {
		return now
	}
	entry.lastUse = step
	m[chatID] = entry
	interval := time.Second
	if chatID < 0 {
		interval = 3 * time.Second
	}
	ready := entry.lastTake.Add(interval)
	if ready.After(now) {
		return ready
	}
	return now
}

func (m chatBucketReference) consume(now time.Time, chatID int64, step, capacity int) {
	if _, exists := m[chatID]; !exists && len(m) == capacity {
		oldest := step
		var evicted int64
		for id, entry := range m {
			if entry.lastUse < oldest {
				evicted, oldest = id, entry.lastUse
			}
		}
		delete(m, evicted)
	}
	m[chatID] = chatBucketReferenceEntry{lastTake: now, lastUse: step}
}

func TestChatBucketsReference(t *testing.T) {
	const capacity = 10_000
	for _, tc := range []struct {
		name    string
		preload int
		deltas  []time.Duration
	}{
		{"sparse", 0, []time.Duration{0, 1, 250 * time.Millisecond, time.Second - 1, time.Second, 3*time.Second - 1, 3 * time.Second}},
		{"at capacity", capacity, []time.Duration{0, 1, time.Millisecond}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(71, 17))
			now := time.Unix(100, 0)
			buckets := newChatBuckets()
			model := make(chatBucketReference)
			for chatID := 1; chatID <= tc.preload; chatID++ {
				if !buckets.take(now, int64(chatID), methodMessage) {
					t.Fatalf("preload could not consume token for chat %d", chatID)
				}
				model.consume(now, int64(chatID), chatID-tc.preload, capacity)
			}

			classes := []methodClass{methodMessage, methodMessage, methodMessage, methodJoinRequest, methodOther}
			for step := 1; step <= 384; step++ {
				now = now.Add(tc.deltas[rng.IntN(len(tc.deltas))])
				chatID := int64(rng.IntN(32) - 8)
				if rng.IntN(4) == 0 {
					chatID = int64(capacity + 1 + rng.IntN(8))
				}
				class := classes[rng.IntN(len(classes))]
				limited := class == methodMessage && chatID != 0
				ready := now
				if limited {
					ready = model.next(now, chatID, step)
				}
				available := ready.Equal(now)
				switch rng.IntN(3) {
				case 0:
					if got := buckets.has(now, chatID, class); got != available {
						t.Fatalf("step %d, chat %d, class %s: has() = %v, want %v", step, chatID, class, got, available)
					}
				case 1:
					if got, ok := buckets.next(now, chatID, class); !ok || !got.Equal(ready) {
						t.Fatalf("step %d, chat %d, class %s: next() = (%v, %v), want (%v, true)", step, chatID, class, got, ok, ready)
					}
				case 2:
					if got := buckets.take(now, chatID, class); got != available {
						t.Fatalf("step %d, chat %d, class %s: take() = %v, want %v", step, chatID, class, got, available)
					}
					if available && limited {
						model.consume(now, chatID, step, capacity)
					}
				}
				if len(buckets.entries) != len(model) {
					t.Fatalf("step %d, chat %d, class %s: %d entries, want %d", step, chatID, class, len(buckets.entries), len(model))
				}
			}
		})
	}
}
