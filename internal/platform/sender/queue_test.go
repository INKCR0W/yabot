package sender

import (
	"errors"
	"math"
	"slices"
	"testing"
	"time"
)

func testQueue(chatCap int) *queue {
	return newQueue(chatCap, [Background + 1]int{Deadline: 8, Decision: 4, Interactive: 2, Background: 1})
}

func pushItems(t *testing.T, q *queue, class Class, chatID int64, n int, deadline time.Time) []*item {
	t.Helper()
	items := make([]*item, n)
	for i := range items {
		items[i] = &item{request: Request{Class: class, ChatID: chatID, Deadline: deadline}}
		if err := q.push(items[i]); err != nil {
			t.Fatalf("push(%v, %d) item %d: %v", class, chatID, i, err)
		}
	}
	return items
}

func mustTake(t *testing.T, q *queue, now time.Time, canSend func(*item) bool) *item {
	t.Helper()
	got, expired := q.next(now, canSend)
	if got == nil || len(expired) != 0 {
		t.Fatalf("next = %p, %v, want an item and no expirations", got, expired)
	}
	return got
}

func TestQueueClassShares(t *testing.T) {
	const count = 1200
	for _, tt := range []struct {
		name    string
		classes []Class
		want    []int
	}{
		{"two classes", []Class{Deadline, Decision}, []int{800, 400}},
		{"four classes", []Class{Deadline, Decision, Interactive, Background}, []int{640, 320, 160, 80}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(10, 0)
			q := testQueue(count + 1)
			for _, class := range tt.classes {
				pushItems(t, q, class, 1, count+1, now.Add(time.Second))
			}
			var got [Background + 1]int
			for range count {
				got[mustTake(t, q, now, nil).request.Class]++
			}
			for i, class := range tt.classes {
				want := tt.want[i]
				if got[class] < want-1 || got[class] > want+1 {
					t.Errorf("%v sent %d, want [%d, %d]", class, got[class], want-1, want+1)
				}
			}
		})
	}
}

func TestQueueEmptyClassYieldsTurnBeforeRefill(t *testing.T) {
	now := time.Unix(10, 0)
	deadline := now.Add(time.Second)
	q := testQueue(256)
	pushItems(t, q, Deadline, 1, 1, deadline)
	pushItems(t, q, Decision, 2, 2, time.Time{})

	if got := mustTake(t, q, now, nil).request.Class; got != Deadline {
		t.Fatalf("first class = %v, want deadline", got)
	}
	pushItems(t, q, Deadline, 1, 5, deadline)
	if got := mustTake(t, q, now, nil).request.Class; got != Decision {
		t.Fatalf("class after deadline refill = %v, want decision", got)
	}
}

func TestQueueChatShares(t *testing.T) {
	now := time.Unix(10, 0)
	until := now.Add(time.Second)
	type testCase struct {
		name      string
		class     Class
		at        time.Time
		wantLight int
	}
	cases := []testCase{
		{"deadline downweighted", Deadline, now, 20},
		{"deadline restored at expiry", Deadline, until, 50},
	}
	for _, class := range []Class{Decision, Interactive, Background} {
		cases = append(cases, testCase{class.String() + " unaffected", class, now, 50})
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			q := testQueue(256)
			if !q.setWeight(10, 0.25, until) {
				t.Fatal("could not set chat weight")
			}
			pushItems(t, q, tt.class, 10, 120, until.Add(time.Second))
			pushItems(t, q, tt.class, 20, 120, until.Add(time.Second))
			light := 0
			for range 100 {
				if mustTake(t, q, tt.at, nil).request.ChatID == 10 {
					light++
				}
			}
			if light != tt.wantLight {
				t.Fatalf("chat 10 sends = %d of 100, want %d", light, tt.wantLight)
			}
			for _, chatID := range []int64{10, 20, 10, 20} {
				if got := mustTake(t, q, until, nil).request.ChatID; got != chatID {
					t.Fatalf("chat at weight expiry = %d, want %d", got, chatID)
				}
			}
		})
	}
}

func TestQueueMultiChatShares(t *testing.T) {
	const count = 3000
	type bounds struct{ min, max int }
	for _, tt := range []struct {
		name    string
		weights []float64
		want    []bounds
	}{
		{"quarter last", []float64{1, 1, 0.25}, []bounds{{1332, 1335}, {1332, 1335}, {332, 335}}},
		{"ascending weights", []float64{0.25, 0.5, 1}, []bounds{{427, 430}, {856, 859}, {1713, 1716}}},
		{"three tenths first", []float64{0.3, 1, 1}, []bounds{{390, 393}, {1303, 1306}, {1303, 1306}}},
		{"alternating weights", []float64{1, 0.25, 1, 0.25}, []bounds{{1199, 1201}, {299, 301}, {1199, 1201}, {299, 301}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(10, 0)
			until := now.Add(time.Second)
			q := testQueue(count + 1)
			for chatID, weight := range tt.weights {
				if !q.setWeight(int64(chatID), weight, until) {
					t.Fatalf("could not set chat %d weight to %g", chatID, weight)
				}
				pushItems(t, q, Deadline, int64(chatID), count+1, until)
			}
			got := make([]int, len(tt.weights))
			for range count {
				got[mustTake(t, q, now, nil).request.ChatID]++
			}
			for chatID, want := range tt.want {
				if got[chatID] < want.min || got[chatID] > want.max {
					t.Errorf("chat %d sent %d of %d, want [%d, %d]", chatID, got[chatID], count, want.min, want.max)
				}
			}
		})
	}
}

func TestQueueBlockedScansPreserveShares(t *testing.T) {
	now := time.Unix(10, 0)
	until := now.Add(time.Second)
	blocked := func(*item) bool { return false }
	for _, tt := range []struct {
		name      string
		second    Class
		weight    float64
		total     int
		wantFirst int
	}{
		{"classes", Decision, 1, 120, 80},
		{"chats", Deadline, 0.25, 100, 20},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q := testQueue(256)
			if !q.setWeight(1, tt.weight, until) {
				t.Fatal("could not set chat weight")
			}
			pushItems(t, q, Deadline, 1, tt.total+1, until)
			pushItems(t, q, tt.second, 2, tt.total+1, until)
			first := 0
			for range tt.total {
				if mustTake(t, q, now, nil).request.ChatID == 1 {
					first++
				}
				if entry, expired := q.next(now, blocked); entry != nil || len(expired) != 0 {
					t.Fatalf("blocked next = %p, %v, want empty", entry, expired)
				}
			}
			if first != tt.wantFirst {
				t.Errorf("shares = %d:%d, want %d:%d", first, tt.total-first, tt.wantFirst, tt.total-tt.wantFirst)
			}
		})
	}
}

func TestQueueChatRoundRobin(t *testing.T) {
	q := testQueue(256)
	now := time.Unix(10, 0)
	chatIDs := []int64{3, 0, -1}
	for _, chatID := range chatIDs {
		pushItems(t, q, Background, chatID, 5, time.Time{})
	}
	for range 5 {
		for _, chatID := range chatIDs {
			if got := mustTake(t, q, now, nil).request.ChatID; got != chatID {
				t.Fatalf("next chat = %d, want %d", got, chatID)
			}
		}
	}
}

func TestQueueFractionalWeightsOnly(t *testing.T) {
	now := time.Unix(10, 0)
	for _, tt := range []struct {
		name    string
		weights [2]float64
		want    [2]int
	}{
		{"quarter and half", [2]float64{0.25, 0.5}, [2]int{20, 40}},
		{"below and at minimum", [2]float64{1.0 / 128, 1.0 / 64}, [2]int{30, 30}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q := testQueue(256)
			for chatID, weight := range tt.weights {
				if !q.setWeight(int64(chatID), weight, now.Add(time.Second)) {
					t.Fatalf("could not set chat weight %g", weight)
				}
				pushItems(t, q, Deadline, int64(chatID), 60, now.Add(time.Second))
			}
			var got [2]int
			for range 60 {
				checks := [2]int{}
				selected := mustTake(t, q, now, func(i *item) bool {
					checks[i.request.ChatID]++
					return true
				})
				got[selected.request.ChatID]++
				if checks[0] > 1 || checks[1] > 1 {
					t.Fatalf("one scan checked each chat %v times", checks)
				}
			}
			if got != tt.want {
				t.Fatalf("chat sends = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueueVirtualRounds(t *testing.T) {
	now := time.Unix(10, 0)
	until := now.Add(time.Second)
	type step struct {
		allowed []int64
		want    int64
	}
	for _, tt := range []struct {
		name    string
		weights [3]float64
		steps   []step
	}{
		{
			name:    "fractional rounds up",
			weights: [3]float64{0.375, 0.375, 1},
			steps:   []step{{[]int64{0, 1}, 0}, {[]int64{0, 1, 2}, 1}},
		},
		{
			name:    "chats after winner skip final round",
			weights: [3]float64{0.25, 0.25, 1},
			steps:   []step{{[]int64{0, 2}, 2}, {[]int64{0, 1}, 0}, {[]int64{0, 1, 2}, 2}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q := testQueue(256)
			for chat, weight := range tt.weights {
				if !q.setWeight(int64(chat), weight, until) {
					t.Fatalf("could not set weight for chat %d", chat)
				}
				pushItems(t, q, Deadline, int64(chat), len(tt.steps), until)
			}
			for i, step := range tt.steps {
				got := mustTake(t, q, now, func(entry *item) bool {
					return slices.Contains(step.allowed, entry.request.ChatID)
				}).request.ChatID
				if got != step.want {
					t.Fatalf("step %d with chats %v: selected %d, want %d", i+1, step.allowed, got, step.want)
				}
			}
		})
	}
}

func TestQueueSmallChatsAreServedInFirstRound(t *testing.T) {
	q := testQueue(3000)
	now := time.Unix(10, 0)
	pushItems(t, q, Deadline, 0, 3000, now.Add(time.Second))
	for chatID := int64(1); chatID < 100; chatID++ {
		pushItems(t, q, Deadline, chatID, 1, now.Add(time.Second))
	}
	var seen [100]bool
	for range len(seen) {
		chatID := mustTake(t, q, now, nil).request.ChatID
		if seen[chatID] {
			t.Fatalf("chat %d took a second slot in the first round", chatID)
		}
		seen[chatID] = true
	}
}

func TestQueueSmallChatsMeetDeadlinesUnderContinuousLoad(t *testing.T) {
	const (
		smallChats    = 99
		arrivalWindow = 10 * time.Second
		budget        = 8 * time.Second
		sendInterval  = 10 * time.Millisecond
	)
	q := testQueue(256)
	start := time.Unix(10, 0)
	push := func(chatID int64, elapsed time.Duration) {
		entry := &item{request: Request{Class: Deadline, ChatID: chatID, Deadline: start.Add(elapsed + budget)}}
		if err := q.push(entry); err != nil && !errors.Is(err, ErrBusy) {
			t.Fatalf("push chat %d at %v: %v", chatID, elapsed, err)
		}
	}

	bigArrivals, smallArrivals, bigSent, smallSent := 0, 0, 0, 0
	for sendAt := sendInterval; sendAt <= arrivalWindow+budget; sendAt += sendInterval {
		for {
			bigAt := time.Duration(bigArrivals) * time.Second / 300
			smallAt := time.Duration(smallArrivals/smallChats)*time.Second +
				time.Duration(1+smallArrivals%smallChats)*sendInterval
			if smallAt < arrivalWindow && smallAt <= sendAt && smallAt < bigAt {
				push(int64(1+smallArrivals%smallChats), smallAt)
				smallArrivals++
			} else if bigAt < arrivalWindow && bigAt <= sendAt {
				push(0, bigAt)
				bigArrivals++
			} else {
				break
			}
		}
		now := start.Add(sendAt)
		if entry, _ := q.next(now, nil); entry != nil {
			if !now.Before(entry.request.Deadline) {
				t.Fatalf("sent chat %d at or after its deadline", entry.request.ChatID)
			}
			if entry.request.ChatID == 0 {
				bigSent++
			} else {
				smallSent++
			}
		}
	}
	t.Logf("sent small chats %d/%d and large chat %d/%d before deadline", smallSent, smallArrivals, bigSent, bigArrivals)
	if smallArrivals != 990 || bigArrivals != 3000 || smallSent*100 < smallArrivals*99 || bigSent == 0 {
		t.Fail()
	}
}

func TestQueueFIFOAndCapacity(t *testing.T) {
	q := testQueue(256)
	now := time.Unix(10, 0)
	retry := pushItems(t, q, Decision, -1, 1, time.Time{})[0]
	if got := mustTake(t, q, now, nil); got != retry {
		t.Fatalf("first take = %p, want %p", got, retry)
	}
	want := pushItems(t, q, Decision, -1, 256, time.Time{})
	rejected := &item{request: Request{Class: Decision, ChatID: -1}}
	if err := q.push(rejected); !errors.Is(err, ErrBusy) {
		t.Fatalf("257th push = %v, want ErrBusy", err)
	}
	if depth, chatLen := q.depth(Decision), q.chatLen(Decision, -1); depth != 256 || chatLen != 256 {
		t.Fatalf("after rejected push: depth = %d, chatLen = %d, want 256 each", depth, chatLen)
	}
	q.requeue(retry)
	if depth, chatLen := q.depth(Decision), q.chatLen(Decision, -1); depth != 257 || chatLen != 257 {
		t.Fatalf("after retry: depth = %d, chatLen = %d, want 257 each", depth, chatLen)
	}
	if err := q.push(rejected); !errors.Is(err, ErrBusy) {
		t.Fatalf("push above capacity = %v, want ErrBusy", err)
	}
	if depth, chatLen := q.depth(Decision), q.chatLen(Decision, -1); depth != 257 || chatLen != 257 {
		t.Fatalf("after push above capacity: depth = %d, chatLen = %d, want 257 each", depth, chatLen)
	}
	want = append([]*item{retry}, want...)
	for n, expected := range want {
		if got := mustTake(t, q, now, nil); got != expected {
			t.Fatalf("take %d = %p, want %p", n, got, expected)
		}
	}
	if got, expired := q.next(now, nil); got != nil || len(expired) != 0 {
		t.Fatalf("after drain next = %p, %v, want empty", got, expired)
	}
}

func TestQueueRequeuePreservesAdmissionOrder(t *testing.T) {
	for _, tc := range []struct {
		name        string
		admitted    int
		returnOrder []int
	}{
		{"head", 3, []int{0}},
		{"middle", 4, []int{0, 2, 1}},
		{"empty chat rejoins ring", 3, []int{1, 0, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(10, 0)
			q := testQueue(256)
			items := pushItems(t, q, Decision, 1, tc.admitted, time.Time{})
			for i, want := range items[:len(tc.returnOrder)] {
				if got := mustTake(t, q, now, nil); got != want {
					t.Fatalf("initial take %d = %p, want %p", i, got, want)
				}
			}
			for _, i := range tc.returnOrder {
				q.requeue(items[i])
			}
			for i, want := range items {
				if got := mustTake(t, q, now, nil); got != want {
					t.Fatalf("take %d after requeue = %p, want %p", i, got, want)
				}
			}
			if got, expired := q.next(now, nil); got != nil || len(expired) != 0 {
				t.Fatalf("after drain next = %p, %v, want empty", got, expired)
			}
		})
	}
}

func TestQueueEmptyChatLeavesRing(t *testing.T) {
	q := testQueue(256)
	now := time.Unix(10, 0)
	deadline := now.Add(time.Second)
	first := pushItems(t, q, Deadline, 1, 1, deadline)[0]
	other := pushItems(t, q, Deadline, 2, 3, deadline)
	if got := mustTake(t, q, now, nil); got != first {
		t.Fatalf("first item = %p, want %p", got, first)
	}
	if got := q.queueWait(Deadline, 2, now, 4); got != time.Second {
		t.Fatalf("wait after chat left ring = %v, want 1s", got)
	}
	refill := pushItems(t, q, Deadline, 1, 4, deadline)
	for n, want := range []*item{other[0], refill[0], other[1], refill[1], other[2], refill[2], refill[3]} {
		if got := mustTake(t, q, now, nil); got != want {
			t.Fatalf("take %d after refill = %p, want %p", n, got, want)
		}
	}
}

func TestQueueSkipsBlockedHead(t *testing.T) {
	q := testQueue(256)
	now := time.Unix(10, 0)
	first := pushItems(t, q, Decision, 1, 2, time.Time{})
	other := pushItems(t, q, Decision, 2, 1, time.Time{})[0]
	canSend := func(i *item) bool {
		if i == first[1] {
			t.Fatal("predicate saw an item behind a blocked head")
		}
		return i != first[0]
	}
	if got := mustTake(t, q, now, canSend); got != other {
		t.Fatalf("next = %p, want other chat %p", got, other)
	}
	for range 3 {
		if got, expired := q.next(now, canSend); got != nil || len(expired) != 0 {
			t.Fatalf("blocked scan = %p, %v, want empty", got, expired)
		}
	}
	for _, want := range first {
		if got := mustTake(t, q, now, nil); got != want {
			t.Fatalf("unblocked item = %p, want %p", got, want)
		}
	}
}

func TestQueueBlockedTrafficCannotBankCredit(t *testing.T) {
	now := time.Unix(10, 0)
	deadline := now.Add(time.Second)
	for _, tt := range []struct {
		name       string
		otherClass Class
		maxBurst   int
	}{
		{"class", Decision, 8},
		{"chat", Deadline, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q := testQueue(256)
			pushItems(t, q, Deadline, 1, 128, deadline)
			pushItems(t, q, tt.otherClass, 2, 96, deadline)
			canSend := func(i *item) bool { return i.request.ChatID == 2 }
			for range 60 {
				if got := mustTake(t, q, now, canSend).request.ChatID; got != 2 {
					t.Fatalf("sent blocked chat %d", got)
				}
			}
			for range 60 {
				if got, expired := q.next(now, func(*item) bool { return false }); got != nil || len(expired) != 0 {
					t.Fatalf("blocked scan = %p, %v, want empty", got, expired)
				}
			}
			burst := 0
			for range 36 {
				if mustTake(t, q, now, nil).request.ChatID == 1 {
					burst++
					if burst > tt.maxBurst {
						t.Fatalf("recovered chat got %d consecutive sends, want at most %d", burst, tt.maxBurst)
					}
				} else {
					burst = 0
				}
			}
		})
	}
}

func TestQueueSuccessfulScanForfeitsSkippedClassCredit(t *testing.T) {
	now := time.Unix(10, 0)
	deadline := now.Add(time.Second)
	q := testQueue(256)
	pushItems(t, q, Deadline, 1, 10, deadline)
	pushItems(t, q, Decision, 2, 2, deadline)

	if got := mustTake(t, q, now, nil).request.Class; got != Deadline {
		t.Fatalf("first class = %v, want deadline", got)
	}
	onlyDecision := func(i *item) bool { return i.request.Class == Decision }
	if got := mustTake(t, q, now, onlyDecision).request.Class; got != Decision {
		t.Fatalf("class while deadline blocked = %v, want decision", got)
	}
	onlyDeadline := func(i *item) bool { return i.request.Class == Deadline }
	if got := mustTake(t, q, now, onlyDeadline).request.Class; got != Deadline {
		t.Fatalf("first class after unblocking = %v, want deadline", got)
	}
	for i := range 7 {
		if got := mustTake(t, q, now, nil).request.Class; got != Deadline {
			t.Fatalf("class at slot %d of the renewed deadline turn = %v, want deadline", i+2, got)
		}
	}
	if got := mustTake(t, q, now, nil).request.Class; got != Decision {
		t.Fatalf("class after deadline turn = %v, want decision", got)
	}
}

func TestQueueWeightedChatResumesAfterSkip(t *testing.T) {
	now := time.Unix(10, 0)
	deadline := now.Add(time.Second)
	for _, tt := range []struct {
		name     string
		eligible int64
		want     []int64
	}{
		{"chat skipped", 2, []int64{2, 2, 2, 1}},
		{"class skipped", 3, []int64{2, 1, 2, 2}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q := testQueue(256)
			if !q.setWeight(1, 0.25, deadline) {
				t.Fatal("could not set chat weight")
			}
			pushItems(t, q, Deadline, 1, 2, deadline)
			pushItems(t, q, Deadline, 2, 8, deadline)
			pushItems(t, q, Decision, 3, 1, deadline)

			for i := range 2 {
				if got := mustTake(t, q, now, nil).request.ChatID; got != 2 {
					t.Fatalf("chat at slot %d before blocking = %d, want 2", i+1, got)
				}
			}
			canSend := func(i *item) bool { return i.request.ChatID == tt.eligible }
			if got := mustTake(t, q, now, canSend).request.ChatID; got != tt.eligible {
				t.Fatalf("chat while blocked = %d, want %d", got, tt.eligible)
			}
			for i, want := range tt.want {
				if got := mustTake(t, q, now, nil).request.ChatID; got != want {
					t.Fatalf("send %d after unblocking = %d, want %d", i+1, got, want)
				}
			}
		})
	}
}

func TestQueueChatSharesWithAlternatingEligibleClasses(t *testing.T) {
	const scans = 2000
	const deadlineSends = scans / 2
	now := time.Unix(10, 0)
	deadline := now.Add(time.Second)
	q := testQueue(deadlineSends + 1)
	if !q.setWeight(1, 0.25, deadline) {
		t.Fatal("could not set chat weight")
	}
	pushItems(t, q, Deadline, 1, deadlineSends+1, deadline)
	pushItems(t, q, Deadline, 2, deadlineSends+1, deadline)
	pushItems(t, q, Decision, 3, deadlineSends+1, time.Time{})

	lightSends := 0
	for range deadlineSends {
		entry := mustTake(t, q, now, func(i *item) bool { return i.request.Class == Deadline })
		if entry.request.ChatID == 1 {
			lightSends++
		}
		mustTake(t, q, now, func(i *item) bool { return i.request.Class == Decision })
	}
	const wantLight = deadlineSends / 5
	if lightSends < wantLight-1 || lightSends > wantLight+1 {
		t.Fatalf("chat 1 sent %d of %d deadline requests, want [%d, %d]", lightSends, deadlineSends, wantLight-1, wantLight+1)
	}
}

func TestQueueExpirationOnlyScanPreservesTurn(t *testing.T) {
	now := time.Unix(10, 0)
	deadline := now.Add(time.Second)
	q := testQueue(256)
	if !q.setWeight(1, 0.25, deadline) {
		t.Fatal("could not set chat weight")
	}
	pushItems(t, q, Deadline, 1, 3, deadline)
	pushItems(t, q, Deadline, 2, 8, deadline)
	pushItems(t, q, Decision, 4, 1, deadline)
	for i := range 3 {
		if got := mustTake(t, q, now, nil).request.ChatID; got != 2 {
			t.Fatalf("chat at slot %d before blocked scan = %d, want 2", i+1, got)
		}
	}
	wantExpired := pushItems(t, q, Deadline, 3, 1, now)
	got, expired := q.next(now, func(*item) bool { return false })
	if got != nil || !slices.Equal(expired, wantExpired) {
		t.Fatalf("blocked next = %p, %v, want nil and %v", got, expired, wantExpired)
	}
	for i, want := range []int64{1, 2, 2, 2, 2, 4} {
		if got := mustTake(t, q, now, nil).request.ChatID; got != want {
			t.Fatalf("send %d after blocked scan = %d, want %d", i+1, got, want)
		}
	}
}

func TestQueueExpiration(t *testing.T) {
	now := time.Unix(10, 0)
	for _, tt := range []struct {
		name     string
		deadline time.Time
		expires  bool
	}{
		{"past", now.Add(-time.Nanosecond), true},
		{"equal", now, true},
		{"one nanosecond left", now.Add(time.Nanosecond), false},
		{"zero is infinite", time.Time{}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q := newQueue(256, [Background + 1]int{Deadline: 1, Decision: 1, Interactive: 1, Background: 1})
			first := pushItems(t, q, Decision, 1, 2, tt.deadline)
			live := pushItems(t, q, Decision, 1, 1, time.Time{})[0]
			other := pushItems(t, q, Background, 2, 1, time.Time{})[0]
			want := first[0]
			var wantExpired []*item
			if tt.expires {
				want = live
				wantExpired = first
			}
			calls := 0
			got, expired := q.next(now, func(i *item) bool {
				calls++
				if i != want {
					t.Errorf("predicate called with %p, want %p", i, want)
				}
				return true
			})
			if got != want || !slices.Equal(expired, wantExpired) || calls != 1 {
				t.Fatalf("next = %p, %v (%d predicate calls), want %p, %v (1 call)", got, expired, calls, want, wantExpired)
			}
			if got := mustTake(t, q, now, nil); got != other {
				t.Fatalf("next class item = %p, want %p", got, other)
			}
		})
	}
}

func TestQueueContinuesAfterExpiredChatLeavesRing(t *testing.T) {
	q := testQueue(256)
	now := time.Unix(10, 0)
	wantExpired := pushItems(t, q, Deadline, 1, 1, now)
	want := pushItems(t, q, Deadline, 2, 1, now.Add(time.Second))[0]
	got, expired := q.next(now, nil)
	if got != want || !slices.Equal(expired, wantExpired) {
		t.Fatalf("next = %p, %v, want %p, %v", got, expired, want, wantExpired)
	}
}

func TestQueueParkAndUnpark(t *testing.T) {
	q := testQueue(256)
	now := time.Unix(10, 0)
	retry := pushItems(t, q, Background, 0, 1, time.Time{})[0]
	if got := mustTake(t, q, now, nil); got != retry {
		t.Fatalf("first take = %p, want %p", got, retry)
	}
	expiredItems := pushItems(t, q, Deadline, 0, 1, now)
	decision := pushItems(t, q, Decision, 0, 1, time.Time{})[0]
	interactive := pushItems(t, q, Interactive, 0, 1, now.Add(time.Second))[0]
	background := pushItems(t, q, Background, 0, 1, time.Time{})[0]
	q.park(0)
	q.park(0)
	q.park(2)
	newWhileParked := pushItems(t, q, Decision, 2, 1, time.Time{})[0]
	live := pushItems(t, q, Deadline, 0, 1, now.Add(time.Second))[0]
	q.requeue(retry)
	other := pushItems(t, q, Deadline, 1, 1, now.Add(time.Second))[0]
	if got := mustTake(t, q, now, nil); got != other {
		t.Fatalf("while parked next = %p, want %p", got, other)
	}
	if got, expired := q.next(now, nil); got != nil || len(expired) != 0 {
		t.Fatalf("while parked next = %p, %v, want empty without expiring entries", got, expired)
	}
	if depth, chatLen := q.depth(Deadline), q.chatLen(Deadline, 0); depth != 2 || chatLen != 2 {
		t.Fatalf("parked depth = %d, chatLen = %d, want 2 each", depth, chatLen)
	}
	q.unpark(0)
	q.unpark(0)
	want := map[*item]bool{live: true, decision: true, interactive: true, retry: true, background: true}
	var expiredAfterUnpark []*item
	for range len(want) {
		got, expired := q.next(now, nil)
		if !want[got] {
			t.Fatalf("unexpected unparked item %p", got)
		}
		delete(want, got)
		expiredAfterUnpark = append(expiredAfterUnpark, expired...)
	}
	if !slices.Equal(expiredAfterUnpark, expiredItems) {
		t.Fatalf("expired after unpark = %v, want %v", expiredAfterUnpark, expiredItems)
	}
	q.unpark(2)
	if got := mustTake(t, q, now, nil); got != newWhileParked {
		t.Fatalf("item enqueued while parked = %p, want %p", got, newWhileParked)
	}
	if got, expired := q.next(now, nil); got != nil || len(expired) != 0 {
		t.Fatalf("after processing unparked entries next = %p, %v, want empty", got, expired)
	}
}

func TestQueueUnparkRestartsWeightedChatShare(t *testing.T) {
	q := testQueue(256)
	now := time.Unix(10, 0)
	until := now.Add(time.Minute)
	if !q.setWeight(1, 0.25, until) {
		t.Fatal("could not set chat weight")
	}
	pushItems(t, q, Deadline, 1, 1, until)
	pushItems(t, q, Deadline, 2, 8, until)
	for i := range 3 {
		if got := mustTake(t, q, now, nil).request.ChatID; got != 2 {
			t.Fatalf("send %d before park = chat %d, want 2", i+1, got)
		}
	}

	q.park(1)
	q.unpark(1)
	resumedAt := now.Add(time.Second)
	for i, want := range []int64{2, 2, 2, 2, 1} {
		if got := mustTake(t, q, resumedAt, nil).request.ChatID; got != want {
			t.Fatalf("send %d after unpark = chat %d, want %d", i+1, got, want)
		}
	}
}

func TestQueueRemoveChatAndDrain(t *testing.T) {
	q := testQueue(256)
	now := time.Unix(10, 0)
	resume := now.Add(time.Second)
	q.park(1)
	first := pushItems(t, q, Deadline, 1, 1, now)[0]
	kept := pushItems(t, q, Deadline, 1, 1, resume)[0]
	last := pushItems(t, q, Deadline, 1, 1, now)[0]
	other := pushItems(t, q, Deadline, 2, 1, resume)[0]
	background := pushItems(t, q, Background, 1, 2, time.Time{})
	if got := q.removeChat(Deadline, 1, func(i *item) bool { return i.request.Deadline.Before(resume) }); !slices.Equal(got, []*item{first, last}) {
		t.Fatalf("removed %v, want [%p %p]", got, first, last)
	}
	if depth, chatLen := q.depth(Deadline), q.chatLen(Deadline, 1); depth != 2 || chatLen != 1 {
		t.Fatalf("after removal: depth = %d, chatLen = %d, want 2, 1", depth, chatLen)
	}
	if got := q.drain(Background); !slices.Equal(got, background) || q.depth(Background) != 0 {
		t.Fatalf("drain parked background = %v, depth = %d, want %v, 0", got, q.depth(Background), background)
	}
	q.unpark(1)
	for _, want := range []*item{other, kept} {
		if got := mustTake(t, q, now, nil); got != want {
			t.Fatalf("surviving item = %p, want %p", got, want)
		}
	}
	if got := q.drain(Deadline); len(got) != 0 || q.depth(Deadline) != 0 {
		t.Fatalf("empty class drain = %v, depth = %d", got, q.depth(Deadline))
	}
}

func TestQueueRemoveClass(t *testing.T) {
	q := testQueue(256)
	now := time.Unix(10, 0)
	resume := now.Add(2 * time.Second)
	early := resume.Add(-time.Nanosecond)
	fullyRemoved := pushItems(t, q, Deadline, 2, 2, early)
	first := pushItems(t, q, Deadline, 1, 1, resume)[0]
	middle := pushItems(t, q, Deadline, 1, 1, early)[0]
	last := pushItems(t, q, Deadline, 1, 1, resume)[0]
	q.park(3)
	parkedRemoved := pushItems(t, q, Deadline, 3, 1, early)
	parkedKept := pushItems(t, q, Deadline, 3, 1, resume)[0]
	otherClass := pushItems(t, q, Decision, 1, 1, early)[0]

	removed := q.removeClass(Deadline, func(i *item) bool { return i.request.Deadline.Before(resume) })
	if len(removed) != 4 {
		t.Fatalf("removeClass removed %d items, want 4", len(removed))
	}
	removedByChat := make(map[int64][]*item)
	for _, entry := range removed {
		chatID := entry.request.ChatID
		removedByChat[chatID] = append(removedByChat[chatID], entry)
	}
	for chatID, want := range map[int64][]*item{1: {middle}, 2: fullyRemoved, 3: parkedRemoved} {
		if got := removedByChat[chatID]; !slices.Equal(got, want) {
			t.Errorf("chat %d removed items = %v, want %v", chatID, got, want)
		}
	}
	if got := q.depth(Deadline); got != 3 {
		t.Errorf("deadline depth = %d, want 3", got)
	}
	if got := q.depth(Decision); got != 1 {
		t.Errorf("decision depth = %d, want 1", got)
	}
	for chatID, want := range map[int64]int{1: 2, 2: 0, 3: 1} {
		if got := q.chatLen(Deadline, chatID); got != want {
			t.Errorf("chat %d length = %d, want %d", chatID, got, want)
		}
	}

	requeued := pushItems(t, q, Deadline, 2, 1, resume)[0]
	for _, want := range []*item{first, requeued, last, otherClass} {
		if got := mustTake(t, q, now, nil); got != want {
			t.Fatalf("next after removal = %p, want %p", got, want)
		}
	}
	if got, expired := q.next(now, nil); got != nil || len(expired) != 0 {
		t.Fatalf("next while chat 3 is parked = %p, %v, want empty", got, expired)
	}
	q.unpark(3)
	if got := mustTake(t, q, now, nil); got != parkedKept {
		t.Fatalf("next after unpark = %p, want %p", got, parkedKept)
	}
}

func TestQueueDrainClearsActiveChats(t *testing.T) {
	q := testQueue(256)
	now := time.Unix(10, 0)
	active := pushItems(t, q, Decision, 1, 4, time.Time{})
	parked := pushItems(t, q, Decision, 2, 2, time.Time{})
	q.park(2)
	if got := mustTake(t, q, now, nil); got != active[0] {
		t.Fatalf("first item = %p, want %p", got, active[0])
	}
	drained := q.drain(Decision)
	if len(drained) != 5 {
		t.Fatalf("drain returned %d items, want 5", len(drained))
	}
	wantByChat := map[int64][]*item{1: active[1:], 2: parked}
	for _, got := range drained {
		chatID := got.request.ChatID
		want := wantByChat[chatID]
		if len(want) == 0 || got != want[0] {
			t.Fatalf("unexpected item %p for chat %d, remaining expected items %v", got, chatID, want)
		}
		wantByChat[chatID] = want[1:]
	}
	if got := q.depth(Decision); got != 0 {
		t.Fatalf("depth after drain = %d, want 0", got)
	}
	for _, chatID := range []int64{1, 2} {
		if got := q.chatLen(Decision, chatID); got != 0 {
			t.Fatalf("chat %d length after drain = %d, want 0", chatID, got)
		}
	}
	q.unpark(2)
	if got, expired := q.next(now, nil); got != nil || len(expired) != 0 {
		t.Fatalf("next after drain = %p, %v, want empty", got, expired)
	}
}

func TestQueueRejectsInvalidWeights(t *testing.T) {
	now := time.Unix(10, 0)
	for _, tt := range []struct {
		name   string
		weight float64
	}{
		{"above one", 1.5},
		{"zero", 0},
		{"negative", -0.25},
		{"nan", math.NaN()},
		{"positive infinity", math.Inf(1)},
		{"negative infinity", math.Inf(-1)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q := testQueue(256)
			until := now.Add(time.Second)
			pushItems(t, q, Deadline, 2, 1, until)
			if !q.setWeight(1, 0.25, until) {
				t.Fatal("could not set initial weight")
			}
			if q.setWeight(1, tt.weight, until) {
				t.Fatalf("accepted invalid weight %g", tt.weight)
			}
			if got := q.queueWait(Deadline, 1, now, 1); got != 5*time.Second {
				t.Fatalf("invalid weight changed wait to %v, want 5s", got)
			}
		})
	}
}

func TestQueueWeightCapacityAndExpiry(t *testing.T) {
	const capacity = 10000
	now := time.Unix(10, 0)
	q := testQueue(256)
	pushItems(t, q, Deadline, 0, 1, now.Add(time.Hour))
	for chatID := int64(1); chatID <= capacity+3; chatID++ {
		if !q.setWeight(chatID, 0.25, now.Add(time.Duration(chatID)*time.Second)) {
			t.Fatalf("could not set weight for chat %d", chatID)
		}
	}
	for chatID := int64(1); chatID <= capacity+3; chatID++ {
		want := 5 * time.Second
		if chatID <= 3 {
			want = 2 * time.Second
		}
		if got := q.queueWait(Deadline, chatID, now, 1); got != want {
			t.Fatalf("wait for chat %d = %v, want %v", chatID, got, want)
		}
	}
	if got := q.queueWait(Deadline, 4, now.Add(4*time.Second), 1); got != 2*time.Second {
		t.Fatalf("wait at weight expiry = %v, want 2s", got)
	}
	if got := q.queueWait(Deadline, 5, now.Add(4*time.Second), 1); got != 5*time.Second {
		t.Fatalf("wait with live weight = %v, want 5s", got)
	}
	if got := len(q.weights); got != capacity-1 {
		t.Fatalf("retained weights after expiry = %d, want %d", got, capacity-1)
	}
}

func TestQueueWeightRenewalReschedulesExpiry(t *testing.T) {
	now := time.Unix(10, 0)
	q := testQueue(256)
	pushItems(t, q, Deadline, 0, 1, now.Add(time.Minute))
	if !q.setWeight(1, 0.25, now.Add(time.Second)) ||
		!q.setWeight(2, 0.25, now.Add(2*time.Second)) ||
		!q.setWeight(1, 0.25, now.Add(10*time.Second)) {
		t.Fatal("could not set and renew chat weights")
	}
	at := now.Add(2 * time.Second)
	if got := q.queueWait(Deadline, 2, at, 1); got != 2*time.Second {
		t.Errorf("expired chat wait = %v, want 2s", got)
	}
	if got := q.queueWait(Deadline, 1, at, 1); got != 5*time.Second {
		t.Errorf("renewed chat wait = %v, want 5s", got)
	}
}

func TestQueueWait(t *testing.T) {
	now := time.Unix(10, 0)
	deadline := now.Add(time.Second)
	for _, tt := range []struct {
		name      string
		class     Class
		chatID    int64
		rps       float64
		weight    float64
		other     float64
		parkChat  int64
		addClass  Class
		parkClass bool
		empty     bool
		want      time.Duration
	}{
		{name: "target active", class: Deadline, chatID: 1, rps: 8, want: time.Second},
		{name: "target absent", class: Deadline, chatID: 3, rps: 8, want: 375 * time.Millisecond},
		{name: "target parked", class: Deadline, chatID: 1, rps: 8, parkChat: 1, want: time.Second},
		{name: "other chat parked", class: Deadline, chatID: 1, rps: 8, parkChat: 2, want: 500 * time.Millisecond},
		{name: "decision backlog", class: Deadline, chatID: 1, rps: 8, addClass: Decision, want: 1500 * time.Millisecond},
		{name: "parked decision backlog", class: Deadline, chatID: 1, rps: 8, addClass: Decision, parkClass: true, want: 1500 * time.Millisecond},
		{name: "target downweighted", class: Deadline, chatID: 1, rps: 8, weight: 0.25, want: 2500 * time.Millisecond},
		{name: "other active chat downweighted", class: Deadline, chatID: 1, rps: 8, other: 0.25, want: 625 * time.Millisecond},
		{name: "absent target downweighted", class: Deadline, chatID: 3, rps: 8, weight: 0.25, want: 1125 * time.Millisecond},
		{name: "other class ignores chat weight", class: Interactive, chatID: 1, rps: 10, weight: 0.25, addClass: Interactive, want: time.Second},
		{name: "empty target class shares rate", class: Background, chatID: 1, rps: 9, want: time.Second},
		{name: "zero rate", class: Deadline, chatID: 1, rps: 0, want: time.Duration(math.MaxInt64)},
		{name: "negative rate", class: Deadline, chatID: 1, rps: -1, want: time.Duration(math.MaxInt64)},
		{name: "wait overflows duration", class: Deadline, chatID: 1, rps: math.SmallestNonzeroFloat64, want: time.Duration(math.MaxInt64)},
		{name: "below and at minimum weight", class: Deadline, chatID: 1, rps: 8, weight: 1.0 / 128, other: 1.0 / 64, want: time.Second},
		{name: "empty queue with downweighted target", class: Deadline, chatID: 1, rps: 25, weight: 0.29, empty: true, want: 40 * time.Millisecond},
		{name: "wait below half a nanosecond", class: Deadline, chatID: 1, rps: math.MaxFloat64, want: 0},
		{name: "fraction above half a nanosecond rounds up", class: Deadline, chatID: 1, rps: 3e9, want: 3 * time.Nanosecond},
		{name: "fraction below half a nanosecond rounds down", class: Deadline, chatID: 1, rps: 6e9, want: time.Nanosecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q := testQueue(256)
			if !tt.empty {
				pushItems(t, q, Deadline, 1, 3, deadline)
				pushItems(t, q, Deadline, 2, 2, deadline)
			}
			if tt.weight != 0 && !q.setWeight(tt.chatID, tt.weight, deadline) {
				t.Fatal("could not set chat weight")
			}
			if tt.other != 0 && !q.setWeight(2, tt.other, deadline) {
				t.Fatal("could not set other chat weight")
			}
			if tt.parkChat != 0 {
				q.park(tt.parkChat)
			}
			if tt.addClass != 0 {
				pushItems(t, q, tt.addClass, 4, 1, deadline)
				if tt.parkClass {
					q.park(4)
				}
			}
			if got := q.queueWait(tt.class, tt.chatID, now, tt.rps); got != tt.want {
				t.Fatalf("queueWait = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueueWaitWithSevenDownweightedChats(t *testing.T) {
	q := testQueue(256)
	now := time.Unix(10, 0)
	until := now.Add(time.Second)
	for chatID := range int64(7) {
		if !q.setWeight(chatID, 0.7, until) {
			t.Fatalf("could not set weight for chat %d", chatID)
		}
		pushItems(t, q, Deadline, chatID, 1, until)
	}
	if got, want := q.queueWait(Deadline, 0, now, 25), 560*time.Millisecond; got != want {
		t.Fatalf("queueWait = %v, want %v", got, want)
	}
}

func TestQueueCommonPathsDoNotAllocate(t *testing.T) {
	const runs = 100
	now := time.Unix(10, 0)
	until := now.Add(time.Second)
	q := testQueue(runs + 1)
	if !q.setWeight(1, 0.25, until) {
		t.Fatal("could not set chat weight")
	}
	for _, chatID := range []int64{1, 2} {
		pushItems(t, q, Deadline, chatID, runs+1, until)
	}
	blocked := func(*item) bool { return false }
	for _, tt := range []struct {
		name string
		run  func()
	}{
		{"blocked next", func() { q.next(now, blocked) }},
		{"successful next", func() { q.next(now, nil) }},
		{"weighted queueWait", func() { q.queueWait(Deadline, 1, now, 25) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := testing.AllocsPerRun(runs, tt.run); got != 0 {
				t.Fatalf("allocations per run = %g, want 0", got)
			}
		})
	}
}
