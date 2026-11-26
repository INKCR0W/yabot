package sender

import (
	"slices"
	"testing"
	"time"
)

func TestRateGateChatPause(t *testing.T) {
	now := time.Unix(100, 0)
	until := now.Add(5 * time.Second)
	var gate rateGate
	if got := gate.record429(now, -1, methodMessage, 5*time.Second); got != (gateUpdate{chatUntil: until}) {
		t.Fatalf("record429() = %+v, want chat pause until %v", got, until)
	}
	for _, tc := range []struct {
		name  string
		at    time.Time
		chat  int64
		class methodClass
		want  time.Time
	}{
		{"affected chat", now, -1, methodMessage, until},
		{"same chat, different method", now, -1, methodJoinRequest, until},
		{"another chat", now, -2, methodMessage, time.Time{}},
		{"no chat", now, 0, methodMessage, time.Time{}},
		{"one nanosecond before recovery", until.Add(-time.Nanosecond), -1, methodMessage, until},
		{"at recovery", until, -1, methodMessage, time.Time{}},
		{"after recovery", until.Add(time.Nanosecond), -1, methodMessage, time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, paused := gate.blockedUntil(tc.at, tc.chat, tc.class)
			checkGateTime(t, got, paused, tc.want)
		})
	}
}

func TestRateGateMethodEscalation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		span       time.Duration
		chats      [3]int64
		thirdClass methodClass
		upgrades   bool
	}{
		{"three different chats", 3 * time.Second, [3]int64{1, 2, 3}, methodMessage, true},
		{"one chat three times", 3 * time.Second, [3]int64{1, 1, 1}, methodMessage, false},
		{"two different chats", 3 * time.Second, [3]int64{1, 2, 1}, methodMessage, false},
		{"one nanosecond inside the window", 10*time.Second - time.Nanosecond, [3]int64{1, 2, 3}, methodMessage, true},
		{"exactly ten seconds", 10 * time.Second, [3]int64{1, 2, 3}, methodMessage, false},
		{"over ten seconds", 10*time.Second + time.Nanosecond, [3]int64{1, 2, 3}, methodMessage, false},
		{"different method class", 3 * time.Second, [3]int64{1, 2, 3}, methodOther, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Unix(100, 0)
			now := start.Add(tc.span)
			var gate rateGate
			gate.record429(start, tc.chats[0], methodMessage, 6*time.Second)
			gate.record429(start.Add(time.Second), tc.chats[1], methodMessage, 13*time.Second)
			update := gate.record429(now, tc.chats[2], tc.thirdClass, 4*time.Second)
			var want time.Time
			if tc.upgrades {
				want = now.Add(13 * time.Second)
			}
			if !update.methodUntil.Equal(want) || !update.globalUntil.IsZero() {
				t.Fatalf("record429() = %+v, want method pause until %v and no global pause", update, want)
			}
			got, paused := gate.blockedUntil(now, 9, methodMessage)
			checkGateTime(t, got, paused, want)
			got, paused = gate.blockedUntil(now, 9, methodOther)
			checkGateTime(t, got, paused, time.Time{})
		})
	}
}

func TestRateGateRetryAfter(t *testing.T) {
	for _, tc := range []struct {
		name       string
		chatID     int64
		retryAfter time.Duration
		wantDelay  time.Duration
		global     bool
	}{
		{"no chat pauses the method class", 0, 5 * time.Second, 5 * time.Second, false},
		{"zero retry after", 1, 0, time.Second, false},
		{"negative retry after", 0, -3 * time.Second, time.Second, false},
		{"exactly thirty seconds", 1, 30 * time.Second, 30 * time.Second, false},
		{"over thirty seconds", 1, 30*time.Second + time.Nanosecond, 30*time.Second + time.Nanosecond, true},
		{"thirty-one seconds with chat", 1, 31 * time.Second, 31 * time.Second, true},
		{"thirty-one seconds without chat", 0, 31 * time.Second, 31 * time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			until := now.Add(tc.wantDelay)
			var gate rateGate
			var want gateUpdate
			wantCounts := gateCounts{chats: 1}
			if tc.chatID == 0 {
				want.methodUntil = until
				wantCounts = gateCounts{methods: 1}
			} else {
				want.chatUntil = until
			}
			if tc.global {
				want.globalUntil = until
				wantCounts.global = 1
			}
			if got := gate.record429(now, tc.chatID, methodMessage, tc.retryAfter); got != want {
				t.Fatalf("record429() = %+v, want %+v", got, want)
			}
			if got := gate.counts(now); got != wantCounts {
				t.Fatalf("counts() = %+v, want %+v", got, wantCounts)
			}
			got, paused := gate.blockedUntil(now, tc.chatID, methodMessage)
			checkGateTime(t, got, paused, until)
			got, paused = gate.blockedUntil(now, 2, methodJoinRequest)
			checkGateTime(t, got, paused, want.globalUntil)
			got, paused = gate.globalPause(until)
			checkGateTime(t, got, paused, time.Time{})
		})
	}
}

func TestRateGateAllMethodsPaused(t *testing.T) {
	for _, explicitDelay := range []time.Duration{0, 31 * time.Second} {
		t.Run(explicitDelay.String(), func(t *testing.T) {
			now := time.Unix(100, 0)
			first := now.Add(3 * time.Second)
			var gate rateGate
			if explicitDelay > 0 {
				gate.record429(now, 1, methodOther, explicitDelay)
			}
			gate.record429(now, 0, methodJoinRequest, 8*time.Second)
			gate.record429(now, 0, methodOther, 5*time.Second)
			update := gate.record429(now, 0, methodMessage, 3*time.Second)
			until := now.Add(max(3*time.Second, explicitDelay))
			if !update.globalUntil.Equal(until) || !update.methodUntil.Equal(first) {
				t.Fatalf("record429() = %+v, want method until %v and global until %v", update, first, until)
			}
			got, paused := gate.globalPause(until.Add(-time.Nanosecond))
			checkGateTime(t, got, paused, until)
			got, paused = gate.blockedUntil(now, 2, methodJoinRequest)
			checkGateTime(t, got, paused, now.Add(max(8*time.Second, explicitDelay)))
			got, paused = gate.nextRecovery(now)
			checkGateTime(t, got, paused, first)
			if counts := gate.counts(now); counts.methods != 3 || counts.global != 1 {
				t.Fatalf("counts() = %+v, want three method pauses and one global pause", counts)
			}
			got, paused = gate.globalPause(until)
			checkGateTime(t, got, paused, time.Time{})
			got, paused = gate.blockedUntil(until, 2, methodMessage)
			checkGateTime(t, got, paused, time.Time{})
		})
	}
	t.Run("method pauses outlast explicit pause", func(t *testing.T) {
		start := time.Unix(100, 0)
		now := start.Add(5 * time.Second)
		until := start.Add(35 * time.Second)
		var gate rateGate
		gate.record429(start, 1, methodOther, 31*time.Second)
		gate.record429(now, 0, methodJoinRequest, 30*time.Second)
		gate.record429(now, 0, methodOther, 30*time.Second)
		update := gate.record429(now, 0, methodMessage, 30*time.Second)
		if !update.globalUntil.Equal(until) {
			t.Errorf("record429().globalUntil = %v, want %v", update.globalUntil, until)
		}
		got, paused := gate.globalPause(now)
		checkGateTime(t, got, paused, until)
	})
}

func TestRateGatePauseExtension(t *testing.T) {
	for _, tc := range []struct {
		name   string
		chatID int64
		global bool
	}{
		{"chat", 1, false},
		{"method", 0, false},
		{"chat and global", 1, true},
		{"method and global", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			initial := 10 * time.Second
			if tc.global {
				initial = 40 * time.Second
			}
			var gate rateGate
			for _, event := range []struct {
				offset     time.Duration
				retryAfter time.Duration
				wantDelay  time.Duration
				changes    bool
			}{
				{0, initial, initial, true},
				{time.Second, initial - 2*time.Second, initial, false},
				{2 * time.Second, initial - 2*time.Second, initial, false},
				{3 * time.Second, initial, initial + 3*time.Second, true},
			} {
				at, until := now.Add(event.offset), now.Add(event.wantDelay)
				got := gate.record429(at, tc.chatID, methodMessage, event.retryAfter)
				var want gateUpdate
				if tc.chatID == 0 {
					want.methodUntil = until
				} else if event.changes {
					want.chatUntil = until
				}
				if tc.global {
					want.globalUntil = until
				}
				if got != want {
					t.Fatalf("record429() at %v = %+v, want %+v", event.offset, got, want)
				}
				blockedUntil, paused := gate.blockedUntil(at, tc.chatID, methodMessage)
				checkGateTime(t, blockedUntil, paused, until)
			}
		})
	}
}

func TestRateGateEscalationWithExistingPause(t *testing.T) {
	now := time.Unix(100, 0)
	var gate rateGate
	gate.record429(now, 0, methodMessage, 20*time.Second)
	gate.record429(now, 1, methodMessage, time.Second)
	gate.record429(now, 2, methodMessage, 5*time.Second)
	for _, tc := range []struct {
		retryAfter time.Duration
		wantDelay  time.Duration
	}{
		{time.Second, 20 * time.Second},
		{25 * time.Second, 25 * time.Second},
	} {
		t.Run(tc.retryAfter.String(), func(t *testing.T) {
			until := now.Add(tc.wantDelay)
			update := gate.record429(now, 3, methodMessage, tc.retryAfter)
			if !update.methodUntil.Equal(until) {
				t.Fatalf("method pause = %v, want %v", update.methodUntil, until)
			}
		})
	}
}

func TestRateGateLatestApplicablePause(t *testing.T) {
	start := time.Unix(100, 0)
	now := start.Add(20 * time.Second)
	var gate rateGate
	gate.record429(start, 5, methodJoinRequest, 31*time.Second)
	gate.record429(now, 1, methodMessage, 18*time.Second)
	gate.record429(now, 0, methodMessage, 15*time.Second)
	for _, tc := range []struct {
		name  string
		chat  int64
		class methodClass
		until time.Time
	}{
		{"chat", 1, methodMessage, now.Add(18 * time.Second)},
		{"method", 2, methodMessage, now.Add(15 * time.Second)},
		{"global", 2, methodOther, start.Add(31 * time.Second)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, paused := gate.blockedUntil(now, tc.chat, tc.class)
			checkGateTime(t, got, paused, tc.until)
		})
	}
}

func TestRateGateMethodPauseOutlastsChat(t *testing.T) {
	start := time.Unix(100, 0)
	now := start.Add(2 * time.Second)
	var gate rateGate
	gate.record429(start, 1, methodMessage, 5*time.Second)
	gate.record429(start.Add(time.Second), 2, methodMessage, 5*time.Second)
	gate.record429(now, 3, methodMessage, time.Second)
	got, paused := gate.blockedUntil(now, 3, methodMessage)
	checkGateTime(t, got, paused, start.Add(7*time.Second))
}

func TestRateGateChatless429DoesNotCountTowardEscalation(t *testing.T) {
	start := time.Unix(100, 0)
	now := start.Add(4 * time.Second)
	var gate rateGate
	gate.record429(start, 0, methodMessage, 2*time.Second)
	gate.record429(start.Add(3*time.Second), 1, methodMessage, 5*time.Second)
	gate.record429(now, 2, methodMessage, 5*time.Second)
	got, paused := gate.blockedUntil(now, 3, methodMessage)
	checkGateTime(t, got, paused, time.Time{})
}

func TestRateGateRecoveryAndCounts(t *testing.T) {
	t.Run("advance through recoveries", func(t *testing.T) {
		now := time.Unix(100, 0)
		var gate rateGate
		gate.record429(now, 2, methodMessage, time.Second)
		gate.record429(now, 1, methodJoinRequest, time.Second)
		gate.record429(now, 3, methodOther, 4*time.Second)
		gate.record429(now, 0, methodMessage, 3*time.Second)
		gate.record429(now, 0, methodJoinRequest, 31*time.Second)
		for _, tc := range []struct {
			at      time.Duration
			expired []int64
			next    time.Duration
			counts  gateCounts
		}{
			{0, nil, time.Second, gateCounts{chats: 3, methods: 2, global: 1}},
			{time.Second, []int64{1, 2}, 3 * time.Second, gateCounts{chats: 1, methods: 2, global: 1}},
			{time.Second, nil, 3 * time.Second, gateCounts{chats: 1, methods: 2, global: 1}},
			{3 * time.Second, nil, 4 * time.Second, gateCounts{chats: 1, methods: 1, global: 1}},
			{4 * time.Second, []int64{3}, 31 * time.Second, gateCounts{methods: 1, global: 1}},
			{31 * time.Second, nil, 0, gateCounts{}},
		} {
			at := now.Add(tc.at)
			if got := gate.counts(at); got != tc.counts {
				t.Fatalf("counts() at %v = %+v, want %+v", tc.at, got, tc.counts)
			}
			var want time.Time
			if tc.next != 0 {
				want = now.Add(tc.next)
			}
			got, ok := gate.nextRecovery(at)
			if !got.Equal(want) || ok != !want.IsZero() {
				t.Fatalf("nextRecovery() at %v = (%v, %v), want (%v, %v)", tc.at, got, ok, want, !want.IsZero())
			}
			expired := gate.advance(at)
			slices.Sort(expired)
			if !slices.Equal(expired, tc.expired) {
				t.Fatalf("advance() at %v = %v, want %v", tc.at, expired, tc.expired)
			}
		}
	})
	t.Run("explicit global pause recovers first", func(t *testing.T) {
		start := time.Unix(100, 0)
		now := start.Add(5 * time.Second)
		globalUntil := start.Add(31 * time.Second)
		chatUntil := start.Add(35 * time.Second)
		var gate rateGate
		gate.record429(start, 1, methodMessage, 31*time.Second)
		update := gate.record429(now, 1, methodMessage, 30*time.Second)
		if !update.chatUntil.Equal(chatUntil) {
			t.Fatalf("record429().chatUntil = %v, want %v", update.chatUntil, chatUntil)
		}
		got, ok := gate.nextRecovery(now)
		checkGateTime(t, got, ok, globalUntil)
		got, ok = gate.nextRecovery(globalUntil)
		checkGateTime(t, got, ok, chatUntil)
	})
	t.Run("explicit global pause recovers before derived pause", func(t *testing.T) {
		start := time.Unix(100, 0)
		now := start.Add(5 * time.Second)
		var gate rateGate
		gate.record429(start, 0, methodMessage, 31*time.Second)
		for _, class := range []methodClass{methodJoinRequest, methodMessage, methodOther} {
			gate.record429(now, 0, class, 30*time.Second)
		}
		got, ok := gate.nextRecovery(now)
		checkGateTime(t, got, ok, start.Add(31*time.Second))
	})
}

func TestRateGateWindowLimit(t *testing.T) {
	const capacity = 4_096
	now := time.Unix(100, 0)
	var gate rateGate
	gate.record429(now, 1, methodMessage, 30*time.Second)
	gate.record429(now, 2, methodMessage, time.Second)
	// Seed the duplicate history to avoid a full window scan for every setup event.
	window := &gate.windows[methodMessage]
	*window = append(*window, slices.Repeat((*window)[1:], capacity-3)...)
	for range 2 {
		gate.record429(now, 2, methodMessage, time.Second)
	}
	if got := len(*window); got != capacity {
		t.Fatalf("window size = %d, want %d", got, capacity)
	}
	if update := gate.record429(now, 3, methodMessage, 5*time.Second); !update.methodUntil.IsZero() {
		t.Fatalf("an evicted chat contributed to escalation: %+v", update)
	}
	until := now.Add(5 * time.Second)
	if update := gate.record429(now, 4, methodMessage, time.Second); !update.methodUntil.Equal(until) {
		t.Fatalf("method pause = %v, want %v", update.methodUntil, until)
	}
	got, paused := gate.blockedUntil(now, 1, methodOther)
	checkGateTime(t, got, paused, now.Add(30*time.Second))
	gate.advance(now.Add(10 * time.Second))
	if got := len(*window); got != 0 {
		t.Fatalf("window size after expiry = %d, want 0", got)
	}
}

func checkGateTime(t *testing.T, got time.Time, ok bool, want time.Time) {
	t.Helper()
	if !got.Equal(want) || ok != !want.IsZero() {
		t.Fatalf("got (%v, %v), want (%v, %v)", got, ok, want, !want.IsZero())
	}
}
