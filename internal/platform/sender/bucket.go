package sender

import (
	"container/list"
	"math"
	"time"
)

const (
	groupChatRPS   = 20.0 / 60
	privateChatRPS = 1
	chatBucketCap  = 10_000
	tokenUnit      = float64(time.Second)
)

type tokenBucket struct {
	rate      float64
	burst     int
	tokens    int
	remainder float64
	updated   time.Time
}

func newTokenBucket(rate float64, burst int, now time.Time) tokenBucket {
	burst = max(0, burst)
	return tokenBucket{rate: rate, burst: burst, tokens: burst, updated: now}
}

func (b *tokenBucket) refill(elapsed time.Duration) float64 {
	if elapsed <= 0 || !(b.rate > 0) {
		return b.remainder
	}
	// Scale before splitting tokens so common refill intervals do not lose fractional precision.
	return math.FMA(float64(elapsed), b.rate, b.remainder)
}

func (b *tokenBucket) balance(now time.Time) (int, float64) {
	elapsed := now.Sub(b.updated)
	if elapsed <= 0 || b.tokens == b.burst {
		return b.tokens, b.remainder
	}
	earned := b.refill(elapsed)
	generated := earned / tokenUnit
	if generated >= float64(math.MaxInt) || int(generated) >= b.burst-b.tokens {
		return b.burst, 0
	}
	whole := int(generated)
	return b.tokens + whole, math.FMA(-float64(whole), tokenUnit, earned)
}

func (b *tokenBucket) has(now time.Time) bool {
	return b.tokens > 0 || (b.burst > 0 && b.refill(now.Sub(b.updated)) >= tokenUnit)
}

func (b *tokenBucket) take(now time.Time) bool {
	tokens, remainder := b.balance(now)
	if tokens == 0 {
		return false
	}
	b.tokens, b.remainder = tokens-1, remainder
	if now.After(b.updated) {
		b.updated = now
	}
	return true
}

func (b *tokenBucket) refund(now time.Time) {
	b.tokens, b.remainder = b.balance(now)
	if b.tokens < b.burst {
		b.tokens++
	}
	if b.tokens == b.burst {
		b.remainder = 0
	}
	if now.After(b.updated) {
		b.updated = now
	}
}

// next estimates token availability within a time.Duration of the refill baseline.
func (b *tokenBucket) next(now time.Time) (time.Time, bool) {
	if b.has(now) {
		return now, true
	}
	if !(b.rate > 0) || b.burst == 0 {
		return time.Time{}, false
	}
	ns := math.Ceil((tokenUnit - b.remainder) / b.rate)
	if ns >= float64(math.MaxInt64) {
		return time.Time{}, false
	}
	ready := b.updated.Add(max(time.Nanosecond, time.Duration(ns)))
	if !ready.After(now) {
		ready = now.Add(time.Nanosecond)
	}
	return ready, true
}

type chatBucketKey struct {
	kind   chatBucketKind
	chatID int64
}

type chatBucketEntry struct {
	key    chatBucketKey
	bucket tokenBucket
}

type chatBuckets struct {
	recency list.List
	entries map[chatBucketKey]*list.Element
}

func newChatBuckets() *chatBuckets {
	return &chatBuckets{entries: make(map[chatBucketKey]*list.Element)}
}

func (b *chatBuckets) has(now time.Time, chatID int64, class methodClass) bool {
	bucket := b.find(chatBucketKey{kind: class.bucketKind(chatID), chatID: chatID})
	return bucket == nil || bucket.has(now)
}

func (b *chatBuckets) take(now time.Time, chatID int64, class methodClass) bool {
	key := chatBucketKey{kind: class.bucketKind(chatID), chatID: chatID}
	if key.kind == noChatBucket {
		return true
	}
	bucket := b.find(key)
	if bucket == nil {
		bucket = b.insert(key, now)
	}
	return bucket.take(now)
}

func (b *chatBuckets) next(now time.Time, chatID int64, class methodClass) (time.Time, bool) {
	bucket := b.find(chatBucketKey{kind: class.bucketKind(chatID), chatID: chatID})
	if bucket == nil {
		return now, true
	}
	return bucket.next(now)
}

func (b *chatBuckets) find(key chatBucketKey) *tokenBucket {
	element := b.entries[key]
	if element == nil {
		return nil
	}
	b.recency.MoveToFront(element)
	return &element.Value.(*chatBucketEntry).bucket
}

func (b *chatBuckets) insert(key chatBucketKey, now time.Time) *tokenBucket {
	rate := groupChatRPS
	if key.kind == privateChatBucket {
		rate = privateChatRPS
	}
	entry := &chatBucketEntry{key: key, bucket: newTokenBucket(rate, 1, now)}
	b.entries[key] = b.recency.PushFront(entry)
	if len(b.entries) > chatBucketCap {
		evicted := b.recency.Remove(b.recency.Back()).(*chatBucketEntry)
		delete(b.entries, evicted.key)
	}
	return &entry.bucket
}
