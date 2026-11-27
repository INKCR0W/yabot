package httpx

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestKeyedLimiterMatchesReferenceModel(t *testing.T) {
	const (
		refillRate = 4
		burst      = 3
		keyCount   = 6
	)

	for _, capacity := range []int{1, 3} {
		t.Run(fmt.Sprintf("capacity_%d", capacity), func(t *testing.T) {
			var evictions int
			limiter := NewKeyedLimiter[int](refillRate, burst, capacity, func() { evictions++ })
			model := modelLimiter{
				buckets:  make(map[int]modelBucket),
				rate:     refillRate,
				burst:    burst,
				capacity: capacity,
			}
			random := rand.New(rand.NewPCG(85, 901))
			now := time.Unix(1, 0)
			// Every step adds a binary-exact fraction of one token, away from rounding ambiguity.
			timeSteps := []time.Duration{0, 0, 0, 62500 * time.Microsecond, 125 * time.Millisecond, 500 * time.Millisecond}
			for turn := 0; turn < 2000; turn++ {
				key := random.IntN(keyCount)
				now = now.Add(timeSteps[random.IntN(len(timeSteps))])
				want := model.allow(key, now, turn)
				if got := limiter.Allow(key, now); got != want || evictions != model.evictions {
					t.Fatalf("step %d, key %d, time %v: allowed = %t, want %t; evictions = %d, want %d",
						turn, key, now, got, want, evictions, model.evictions)
				}
			}
			if evictions == 0 {
				t.Fatal("model did not exercise eviction")
			}
		})
	}
}

type modelBucket struct {
	credit int64
	at     time.Time
	turn   int
}

type modelLimiter struct {
	buckets   map[int]modelBucket
	rate      int64
	burst     int64
	capacity  int
	evictions int
}

func (m *modelLimiter) allow(key int, now time.Time, turn int) bool {
	const token = int64(time.Second)
	maxCredit := m.burst * token
	bucket, exists := m.buckets[key]
	if !exists {
		if len(m.buckets) == m.capacity {
			oldestTurn := turn
			var oldestKey int
			for candidate, other := range m.buckets {
				if other.turn < oldestTurn {
					oldestKey, oldestTurn = candidate, other.turn
				}
			}
			delete(m.buckets, oldestKey)
			m.evictions++
		}
		bucket.credit = maxCredit
	} else {
		bucket.credit = min(maxCredit, bucket.credit+m.rate*now.Sub(bucket.at).Nanoseconds())
	}
	allowed := bucket.credit >= token
	if allowed {
		bucket.credit -= token
	}
	bucket.at, bucket.turn = now, turn
	m.buckets[key] = bucket
	return allowed
}

func TestKeyedLimiterDoesNotRefillAgainAfterClockMovesBack(t *testing.T) {
	limiter := NewKeyedLimiter[string](1, 2, 2, nil)
	now := time.Unix(10, 0)
	for _, step := range []struct {
		key   string
		at    time.Duration
		allow bool
	}{
		{"first", 0, true},
		{"other", time.Hour, true},
		{"first", -time.Second, true},
		{"first", 0, false},
		{"first", 999 * time.Millisecond, false},
		{"first", time.Second, true},
		{"first", 3 * time.Second, true},
		{"first", time.Second, true},
		{"first", 1500 * time.Millisecond, false},
		{"first", 2 * time.Second, false},
		{"first", 4 * time.Second, true},
	} {
		if got := limiter.Allow(step.key, now.Add(step.at)); got != step.allow {
			t.Fatalf("Allow(%q, %v) = %t, want %t", step.key, step.at, got, step.allow)
		}
	}
}

func TestKeyedLimiterConcurrentRequestsShareBurst(t *testing.T) {
	const burst = 11
	limiter := NewKeyedLimiter[int](1, burst, 1, nil)
	now := time.Unix(10, 0)
	start := make(chan struct{})
	var allowed atomic.Int32
	var workers sync.WaitGroup
	for range 100 {
		workers.Go(func() {
			<-start
			if limiter.Allow(1, now) {
				allowed.Add(1)
			}
		})
	}
	close(start)
	workers.Wait()
	if got := allowed.Load(); got != burst {
		t.Errorf("allowed %d concurrent requests, want %d", got, burst)
	}
}

func TestKeyedLimiterEvictionCallbackMayReenter(t *testing.T) {
	now := time.Unix(10, 0)
	var limiter *KeyedLimiter[int]
	calls := 0
	limiter = NewKeyedLimiter[int](1, 1, 1, func() {
		calls++
		if limiter.Allow(2, now) {
			t.Error("replacement bucket had another token during the callback")
		}
	})
	if !limiter.Allow(1, now) {
		t.Fatal("first request was denied")
	}
	done := make(chan bool, 1)
	go func() { done <- limiter.Allow(2, now) }()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	select {
	case allowed := <-done:
		if !allowed || calls != 1 {
			t.Errorf("allowed = %t, want true; callback count = %d, want 1", allowed, calls)
		}
	case <-ctx.Done():
		t.Fatal("eviction callback could not reenter the limiter")
	}
}

func TestKeyedLimiterRejectsNonReflexiveKeys(t *testing.T) {
	now := time.Unix(10, 0)
	evictions := 0
	limiter := NewKeyedLimiter[float64](1, 1, 1, func() { evictions++ })
	if !limiter.Allow(1, now) {
		t.Fatal("valid key was denied")
	}
	if limiter.Allow(math.NaN(), now) || limiter.Allow(1, now) || evictions != 0 {
		t.Errorf("non-reflexive key affected the active bucket, eviction count = %d", evictions)
	}
}

func TestKeyedLimiterRejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rate     float64
		burst    int
		capacity int
	}{
		{"zero_rate", 0, 1, 1},
		{"negative_rate", -1, 1, 1},
		{"infinite_rate", math.Inf(1), 1, 1},
		{"unlimited_sentinel_rate", math.MaxFloat64, 1, 1},
		{"nan_rate", math.NaN(), 1, 1},
		{"zero_burst", 1, 0, 1},
		{"zero_capacity", 1, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("invalid configuration did not panic")
				}
			}()
			NewKeyedLimiter[int](tc.rate, tc.burst, tc.capacity, nil)
		})
	}
}
