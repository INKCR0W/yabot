package httpx

import (
	"container/list"
	"math"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// KeyedLimiter holds independent token buckets in a bounded, concurrency-safe cache.
// Create it with NewKeyedLimiter; keys must be comparable at run time.
type KeyedLimiter[K comparable] struct {
	mu       sync.Mutex
	entries  map[K]*list.Element
	recency  list.List
	rate     rate.Limit
	burst    int
	capacity int
	onEvict  func()
}

type limiterEntry[K comparable] struct {
	key          K
	bucket       *rate.Limiter
	lastObserved time.Time
}

// NewKeyedLimiter requires 0 < rps < math.MaxFloat64 (the rate package reserves the maximum as unlimited)
// and positive burst and capacity.
// onEvict may be nil or run concurrently, and is called without holding the limiter lock.
func NewKeyedLimiter[K comparable](rps float64, burst, capacity int, onEvict func()) *KeyedLimiter[K] {
	if !(rps > 0) || rps >= math.MaxFloat64 || burst <= 0 || capacity <= 0 {
		panic("keyed limiter requires positive finite rate, burst, and capacity")
	}
	return &KeyedLimiter[K]{
		entries:  make(map[K]*list.Element),
		rate:     rate.Limit(rps),
		burst:    burst,
		capacity: capacity,
		onEvict:  onEvict,
	}
}

// Allow spends one token if available. Denied attempts also count as cache use.
// Keys unequal to themselves (such as NaN) are denied without entering the cache.
func (l *KeyedLimiter[K]) Allow(key K, now time.Time) bool {
	if key != key {
		return false
	}

	allowed, evicted := l.allow(key, now)
	if evicted && l.onEvict != nil {
		l.onEvict()
	}
	return allowed
}

func (l *KeyedLimiter[K]) allow(key K, now time.Time) (allowed, evicted bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	element, exists := l.entries[key]
	if !exists {
		if len(l.entries) == l.capacity {
			oldest := l.recency.Back()
			delete(l.entries, oldest.Value.(*limiterEntry[K]).key)
			l.recency.Remove(oldest)
			evicted = true
		}
		entry := &limiterEntry[K]{key: key, bucket: rate.NewLimiter(l.rate, l.burst), lastObserved: now}
		element = l.recency.PushFront(entry)
		l.entries[key] = element
	} else {
		l.recency.MoveToFront(element)
	}

	entry := element.Value.(*limiterEntry[K])
	// Clock samples can reach the lock out of order. Do not refill the same interval twice.
	if now.Before(entry.lastObserved) {
		now = entry.lastObserved
	}
	entry.lastObserved = now
	return entry.bucket.AllowN(now, 1), evicted
}
