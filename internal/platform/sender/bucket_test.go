package sender

import (
	"testing"
	"time"
)

func TestTokenBucketTake(t *testing.T) {
	type check struct {
		at    time.Duration
		count int
	}
	tests := []struct {
		name     string
		rate     float64
		burst    int
		checks   []check
		nextWait time.Duration
	}{
		{"full on creation and capped on refill", 25, 5, []check{{0, 5}, {time.Second, 5}}, 0},
		{"refill to capacity discards the excess fraction", 25, 5, []check{{0, 5}, {210 * time.Millisecond, 5}}, 40 * time.Millisecond},
		{"fractional refill", 10, 2, []check{{0, 2}, {190 * time.Millisecond, 1}, {200*time.Millisecond - 1, 0}, {200 * time.Millisecond, 1}}, 0},
		{"zero rate", 0, 1, []check{{0, 1}, {time.Hour, 0}}, 0},
		{"negative rate", -5, 1, []check{{0, 1}, {time.Hour, 0}}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Unix(100, 0)
			bucket := newTokenBucket(tt.rate, tt.burst, start)
			for _, check := range tt.checks {
				now := start.Add(check.at)
				for i := 0; i < check.count; i++ {
					if !bucket.take(now) {
						t.Fatalf("take at %s succeeded %d times, want %d", check.at, i, check.count)
					}
				}
				if bucket.take(now) {
					t.Errorf("take at %s succeeded more than %d times", check.at, check.count)
				}
			}
			if tt.nextWait > 0 {
				now := start.Add(tt.checks[len(tt.checks)-1].at)
				if got, ok := bucket.next(now); !ok || got.Sub(now) != tt.nextWait {
					t.Errorf("next after the last take = (%v, %v), want (%v, true)", got, ok, now.Add(tt.nextWait))
				}
			}
		})
	}
}

func TestTokenBucketNext(t *testing.T) {
	tests := []struct {
		name string
		rate float64
		wait time.Duration
	}{
		{"exact millisecond boundary", 25, 40 * time.Millisecond},
		{"round up to a nanosecond", 3, 333_333_334 * time.Nanosecond},
		{"fractional rate", 0.125, 8 * time.Second},
		{"zero rate", 0, 0},
		{"negative rate", -1, 0},
		{"refill one nanosecond beyond time.Duration", 1e9 / (1 << 63), 0},
		{"refill exceeds time.Duration", 1e-12, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Unix(100, 0)
			bucket := newTokenBucket(tt.rate, 1, start)
			if got, ok := bucket.next(start); !ok || !got.Equal(start) {
				t.Fatalf("next from full bucket = (%v, %v), want (%v, true)", got, ok, start)
			}
			if !bucket.has(start) || !bucket.take(start) {
				t.Fatal("checking a full bucket consumed its token")
			}
			if tt.wait == 0 {
				if got, ok := bucket.next(start.Add(time.Hour)); ok || !got.IsZero() {
					t.Errorf("next without a reachable refill = (%v, %v), want (zero, false)", got, ok)
				}
				return
			}
			ready := start.Add(tt.wait)
			before := ready.Add(-time.Nanosecond)
			if got, ok := bucket.next(ready); !ok || !got.Equal(ready) || !bucket.has(ready) {
				t.Errorf("token not available at %v, next returned (%v, %v)", ready, got, ok)
			}
			for _, now := range []time.Time{start, before} {
				if got, ok := bucket.next(now); !ok || !got.Equal(ready) {
					t.Errorf("next at %v = (%v, %v), want (%v, true)", now, got, ok, ready)
				}
				if bucket.has(now) || bucket.take(now) {
					t.Errorf("token available before refill at %v", now)
				}
			}
			if !bucket.take(ready) {
				t.Errorf("take failed at reported next time %v", ready)
			}
		})
	}
}

func TestTokenBucketRefund(t *testing.T) {
	tests := []struct {
		name     string
		rate     float64
		burst    int
		spent    int
		refunds  int
		want     int
		refundAt time.Duration
		nextAt   time.Duration
	}{
		{"return a reservation", 2, 2, 1, 1, 2, 0, 500 * time.Millisecond},
		{"cap repeated returns", 2, 2, 2, 3, 2, 0, 500 * time.Millisecond},
		{"cap return to full bucket", 2, 2, 0, 1, 2, 0, 500 * time.Millisecond},
		{"preserve partial refill", 2, 2, 2, 1, 1, 125 * time.Millisecond, 500 * time.Millisecond},
		{"cap return after refill", 2, 2, 2, 1, 2, time.Second, 1500 * time.Millisecond},
		{"return to capacity discards the excess fraction", 25, 5, 1, 1, 5, 20 * time.Millisecond, 60 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Unix(100, 0)
			bucket := newTokenBucket(tt.rate, tt.burst, start)
			for range tt.spent {
				if !bucket.take(start) {
					t.Fatal("could not reserve from fresh bucket")
				}
			}
			now := start.Add(tt.refundAt)
			for range tt.refunds {
				bucket.refund(now)
			}
			for i := 0; i < tt.want; i++ {
				if !bucket.take(now) {
					t.Fatalf("take succeeded %d times after refunds, want %d", i, tt.want)
				}
			}
			if bucket.take(now) {
				t.Errorf("take succeeded more than %d times after refunds", tt.want)
			}
			want := start.Add(tt.nextAt)
			if got, ok := bucket.next(now); !ok || !got.Equal(want) {
				t.Errorf("next after refunds and takes = (%v, %v), want (%v, true)", got, ok, want)
			}
			if bucket.take(want.Add(-time.Nanosecond)) || !bucket.take(want) {
				t.Errorf("take after refund did not respect the refill boundary at %v", want)
			}
		})
	}
}

func TestTokenBucketRollback(t *testing.T) {
	start := time.Unix(100, 0)
	later := start.Add(time.Second)
	bucket := newTokenBucket(2, 2, later)
	if !bucket.take(start) || !bucket.take(later) || bucket.take(start) {
		t.Fatal("rollback changed the number of available initial tokens")
	}
	bucket.refund(start)
	if !bucket.take(start) || bucket.has(later) {
		t.Fatal("refund during rollback created more than one token")
	}
	want := later.Add(500 * time.Millisecond)
	if got, ok := bucket.next(start); !ok || !got.Equal(want) {
		t.Errorf("next after rollback = (%v, %v), want (%v, true)", got, ok, want)
	}
	if bucket.take(want.Add(-time.Nanosecond)) || !bucket.take(want) {
		t.Error("rollback changed the refill boundary")
	}
}

func TestChatBucketsTake(t *testing.T) {
	tests := []struct {
		name  string
		class methodClass
		chat  int64
		wait  time.Duration
	}{
		{"group message", methodMessage, -42, 3 * time.Second},
		{"private message", methodMessage, 42, time.Second},
		{"no chat", methodMessage, 0, 0},
		{"join request", methodJoinRequest, -42, 0},
		{"other method", methodOther, 42, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Unix(100, 0)
			buckets := newChatBuckets()
			if !buckets.has(start, tt.chat, tt.class) || !buckets.take(start, tt.chat, tt.class) {
				t.Fatal("initial chat token unavailable")
			}
			ready := start.Add(tt.wait)
			if got, ok := buckets.next(start, tt.chat, tt.class); !ok || !got.Equal(ready) {
				t.Errorf("next = (%v, %v), want (%v, true)", got, ok, ready)
			}
			if tt.wait > 0 {
				before := ready.Add(-time.Nanosecond)
				if buckets.has(before, tt.chat, tt.class) || buckets.take(before, tt.chat, tt.class) {
					t.Error("chat token available before refill")
				}
				if !buckets.take(start, 2*tt.chat, tt.class) {
					t.Error("exhausting one chat affected a different chat")
				}
				for _, class := range []methodClass{methodJoinRequest, methodOther} {
					if !buckets.has(start, tt.chat, class) {
						t.Errorf("has denied chat %d for method class %s", tt.chat, class)
					}
					if got, ok := buckets.next(start, tt.chat, class); !ok || !got.Equal(start) {
						t.Errorf("next for chat %d and method class %s = (%v, %v), want (%v, true)", tt.chat, class, got, ok, start)
					}
					if !buckets.take(start, tt.chat, class) {
						t.Errorf("take denied chat %d for method class %s", tt.chat, class)
					}
				}
			}
			if !buckets.has(ready, tt.chat, tt.class) || !buckets.take(ready, tt.chat, tt.class) {
				t.Error("chat token unavailable at its reported ready time")
			}
		})
	}
}

func TestChatBucketsLRU(t *testing.T) {
	const capacity = 10_000
	now := time.Unix(100, 0)
	buckets := newChatBuckets()
	for chatID := int64(1); chatID <= capacity; chatID++ {
		if !buckets.take(now, chatID, methodMessage) {
			t.Fatalf("first take for chat %d failed", chatID)
		}
	}
	if buckets.has(now, 1, methodMessage) {
		t.Fatal("chat 1 unexpectedly has a token")
	}
	if got, ok := buckets.next(now, 2, methodMessage); !ok || !got.Equal(now.Add(time.Second)) {
		t.Fatalf("next for exhausted chat 2 = (%v, %v)", got, ok)
	}
	newChat := int64(capacity + 1)
	if !buckets.take(now, newChat, methodMessage) || len(buckets.entries) != capacity {
		t.Fatalf("inserting chat %d left %d entries, want %d", newChat, len(buckets.entries), capacity)
	}
	for _, chatID := range []int64{1, 2, 4, newChat} {
		if buckets.has(now, chatID, methodMessage) {
			t.Errorf("chat %d was evicted instead of the least recently used chat", chatID)
		}
	}
	if !buckets.take(now, 3, methodMessage) || buckets.take(now, 3, methodMessage) {
		t.Error("the evicted chat did not get a fresh one-token bucket")
	}
}

func TestChatBucketsQueriesDoNotInsert(t *testing.T) {
	for _, class := range []methodClass{methodMessage, methodJoinRequest, methodOther} {
		t.Run(class.String(), func(t *testing.T) {
			now := time.Unix(100, 0)
			buckets := newChatBuckets()
			if !buckets.has(now, 42, class) {
				t.Error("new chat has no available token")
			}
			if got := len(buckets.entries); got != 0 {
				t.Errorf("has inserted %d entries, want 0", got)
			}
			if got, ok := buckets.next(now, -42, class); !ok || !got.Equal(now) {
				t.Errorf("next for a new chat = (%v, %v), want (%v, true)", got, ok, now)
			}
			if got := len(buckets.entries); got != 0 {
				t.Errorf("next inserted %d entries, want 0", got)
			}
		})
	}
}
