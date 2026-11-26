package sender

import (
	"container/heap"
	"container/list"
	"math"
	"time"
)

const (
	chatWeightCap = 10_000
	minChatWeight = 1.0 / 64
)

type item struct {
	request           Request
	method            methodClass
	enqueuedAt        time.Time
	retryAt           time.Time
	result            Result
	expireOnAdmission bool
}

// queue needs external synchronization. Callers must validate classes and keep queued requests unchanged.
type queue struct {
	classes [Background + 1]classQueue
	current Class
	chatCap int
	parked  map[int64]struct{}
	weights map[int64]*chatWeight
	expiry  weightHeap
}

type classQueue struct {
	quantum int
	credit  int
	depth   int
	chats   map[int64]*chatQueue
	active  list.List
}

type chatQueue struct {
	id       int64
	items    list.List
	active   *list.Element
	weight   float64
	credit   float64
	eligible bool
}

func newQueue(chatCap int, classWeights [Background + 1]int) *queue {
	if chatCap <= 0 {
		panic("sender: chat capacity must be positive")
	}
	q := &queue{
		current: Deadline,
		chatCap: chatCap,
		parked:  make(map[int64]struct{}),
		weights: make(map[int64]*chatWeight),
	}
	for class := Deadline; class <= Background; class++ {
		if classWeights[class] <= 0 {
			panic("sender: class weights must be positive")
		}
		q.classes[class].quantum = classWeights[class]
		q.classes[class].chats = make(map[int64]*chatQueue)
	}
	return q
}

func (q *queue) push(entry *item) error {
	class, chatID := entry.request.Class, entry.request.ChatID
	if q.chatLen(class, chatID) >= q.chatCap {
		return ErrBusy
	}
	q.chatForPush(class, chatID).items.PushBack(entry)
	return nil
}

func (q *queue) pushFront(entry *item) {
	q.chatForPush(entry.request.Class, entry.request.ChatID).items.PushFront(entry)
}

func (q *queue) chatForPush(class Class, chatID int64) *chatQueue {
	lane := &q.classes[class]
	chat := lane.chats[chatID]
	if chat == nil {
		chat = &chatQueue{id: chatID}
		lane.chats[chatID] = chat
		if _, parked := q.parked[chatID]; !parked {
			chat.active = lane.active.PushBack(chat)
		}
	}
	lane.depth++
	return chat
}

// A nil canSend permits every live head. The predicate only checks eligibility, without
// mutating the queue, spending tokens, or reserving capacity. The caller claims resources after selection.
func (q *queue) next(now time.Time, canSend func(*item) bool) (selected *item, expired []*item) {
	class := q.current
	for range int(Background) {
		selected = q.nextChat(class, now, canSend, &expired)
		if selected == nil {
			class = nextClass(class)
			continue
		}
		for q.current != class {
			q.advanceClass()
		}
		lane := &q.classes[class]
		if lane.credit == 0 {
			lane.credit = lane.quantum
		}
		lane.credit--
		if lane.credit == 0 || lane.active.Len() == 0 {
			q.advanceClass()
		}
		return selected, expired
	}
	return nil, expired
}

func (q *queue) advanceClass() {
	q.classes[q.current].credit = 0
	q.current = nextClass(q.current)
}

func nextClass(class Class) Class {
	if class == Background {
		return Deadline
	}
	return class + 1
}

func (q *queue) nextChat(class Class, now time.Time, canSend func(*item) bool, expired *[]*item) *item {
	lane := &q.classes[class]
	var winner *chatQueue
	var rounds float64
	for elem := lane.active.Front(); elem != nil; {
		chat := elem.Value.(*chatQueue)
		elem = elem.Next()
		head := lane.dropExpired(chat, now, expired)
		if head == nil || (canSend != nil && !canSend(head)) {
			continue
		}

		// The first eligible head guarantees a selection, even if it takes extra rounds.
		if winner == nil {
			q.expireWeights(now)
		}
		chat.addQuantum(q.weightFor(class, chat.id))
		if chat.credit >= 1 {
			return lane.finishChatScan(chat, 0)
		}
		chat.eligible = true
		needed := math.Ceil((1 - chat.credit) / chat.weight)
		if winner == nil || needed < rounds {
			winner, rounds = chat, needed
		}
	}

	if winner == nil {
		return nil
	}
	return lane.finishChatScan(winner, rounds)
}

func (chat *chatQueue) addQuantum(weight float64) {
	if chat.weight != weight {
		chat.weight = weight
		chat.credit = 0
	}
	if chat.credit < 1 {
		chat.credit += weight
	}
}

func (lane *classQueue) dropExpired(chat *chatQueue, now time.Time, expired *[]*item) *item {
	for head := chat.items.Front(); head != nil; head = chat.items.Front() {
		entry := head.Value.(*item)
		deadline := entry.request.Deadline
		if deadline.IsZero() || now.Before(deadline) {
			return entry
		}
		*expired = append(*expired, lane.remove(chat, head))
	}
	return nil
}

func (lane *classQueue) finishChatScan(winner *chatQueue, rounds float64) *item {
	for elem := lane.active.Front(); elem != nil; elem = elem.Next() {
		chat := elem.Value.(*chatQueue)
		if chat.eligible {
			chat.credit = math.FMA(rounds, chat.weight, chat.credit)
			chat.eligible = false
		} else if chat != winner {
			chat.credit = 0
		}
		if chat == winner {
			if rounds == 0 {
				break
			}
			// Chats behind the winner have not started the final round.
			rounds--
		}
	}
	for lane.active.Front() != winner.active {
		lane.active.MoveToBack(lane.active.Front())
	}
	winner.credit = max(0, winner.credit-1)
	entry := lane.remove(winner, winner.items.Front())
	if winner.active != nil && winner.credit < 1 {
		lane.active.MoveToBack(winner.active)
	}
	return entry
}

func (lane *classQueue) remove(chat *chatQueue, elem *list.Element) *item {
	entry := chat.items.Remove(elem).(*item)
	lane.depth--
	if chat.items.Len() == 0 {
		lane.deactivate(chat)
		delete(lane.chats, chat.id)
	}
	return entry
}

func (lane *classQueue) deactivate(chat *chatQueue) {
	if chat.active != nil {
		lane.active.Remove(chat.active)
		chat.active = nil
		chat.credit = 0
	}
}

func (q *queue) park(chatID int64) {
	if _, parked := q.parked[chatID]; parked {
		return
	}
	q.parked[chatID] = struct{}{}
	for class := Deadline; class <= Background; class++ {
		lane := &q.classes[class]
		if chat := lane.chats[chatID]; chat != nil {
			lane.deactivate(chat)
			q.closeEmptyTurn(class)
		}
	}
}

func (q *queue) unpark(chatID int64) {
	if _, parked := q.parked[chatID]; !parked {
		return
	}
	delete(q.parked, chatID)
	for class := Deadline; class <= Background; class++ {
		lane := &q.classes[class]
		if chat := lane.chats[chatID]; chat != nil {
			chat.active = lane.active.PushBack(chat)
		}
	}
}

func (q *queue) closeEmptyTurn(class Class) {
	lane := &q.classes[class]
	if q.current == class && lane.credit > 0 && lane.active.Len() == 0 {
		q.advanceClass()
	}
}

func (q *queue) depth(class Class) int {
	return q.classes[class].depth
}

func (q *queue) empty() bool {
	return q.depth(Deadline)+q.depth(Decision)+q.depth(Interactive)+q.depth(Background) == 0
}

func (q *queue) chatLen(class Class, chatID int64) int {
	if chat := q.classes[class].chats[chatID]; chat != nil {
		return chat.items.Len()
	}
	return 0
}

// drain preserves FIFO order within each chat, including parked chats.
func (q *queue) drain(class Class) []*item {
	return q.removeClass(class, nil)
}

// removeClass visits parked chats and preserves FIFO order within each chat.
// A nil predicate removes every item. The predicate must not mutate the queue.
func (q *queue) removeClass(class Class, remove func(*item) bool) []*item {
	lane := &q.classes[class]
	if lane.depth == 0 {
		return nil
	}
	var removed []*item
	if remove == nil {
		removed = make([]*item, 0, lane.depth)
	}
	for _, chat := range lane.chats {
		removed = lane.removeMatching(chat, remove, removed)
	}
	q.closeEmptyTurn(class)
	return removed
}

// A nil predicate removes every item for the chat. The predicate must not mutate the queue.
func (q *queue) removeChat(class Class, chatID int64, remove func(*item) bool) []*item {
	lane := &q.classes[class]
	chat := lane.chats[chatID]
	if chat == nil {
		return nil
	}
	removed := lane.removeMatching(chat, remove, nil)
	q.closeEmptyTurn(class)
	return removed
}

func (lane *classQueue) removeMatching(chat *chatQueue, remove func(*item) bool, out []*item) []*item {
	for elem := chat.items.Front(); elem != nil; {
		next := elem.Next()
		if remove == nil || remove(elem.Value.(*item)) {
			out = append(out, lane.remove(chat, elem))
		}
		elem = next
	}
	return out
}

type chatWeight struct {
	chatID int64
	value  float64
	until  time.Time
	index  int
}

type weightHeap []*chatWeight

func (h weightHeap) Len() int { return len(h) }

func (h weightHeap) Less(i, j int) bool {
	if h[i].until.Equal(h[j].until) {
		return h[i].chatID < h[j].chatID
	}
	return h[i].until.Before(h[j].until)
}

func (h weightHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *weightHeap) Push(x any) {
	weight := x.(*chatWeight)
	weight.index = len(*h)
	*h = append(*h, weight)
}

func (h *weightHeap) Pop() any {
	last := len(*h) - 1
	popped := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return popped
}

// Evict the earliest expiry when the table exceeds its cap.
func (q *queue) setWeight(chatID int64, w float64, until time.Time) bool {
	if !(w > 0 && w <= 1) {
		return false
	}
	w = max(w, minChatWeight)
	if weight := q.weights[chatID]; weight != nil {
		weight.value = w
		weight.until = until
		heap.Fix(&q.expiry, weight.index)
		return true
	}
	weight := &chatWeight{chatID: chatID, value: w, until: until}
	q.weights[chatID] = weight
	heap.Push(&q.expiry, weight)
	if len(q.weights) > chatWeightCap {
		q.evictWeight()
	}
	return true
}

func (q *queue) evictWeight() {
	evicted := heap.Pop(&q.expiry).(*chatWeight)
	delete(q.weights, evicted.chatID)
}

func (q *queue) expireWeights(now time.Time) {
	for len(q.expiry) > 0 && !now.Before(q.expiry[0].until) {
		q.evictWeight()
	}
}

func (q *queue) weightFor(class Class, chatID int64) float64 {
	if class == Deadline {
		if weight := q.weights[chatID]; weight != nil {
			return weight.value
		}
	}
	return 1
}

func (q *queue) queueWait(class Class, chatID int64, now time.Time, globalRPS float64) time.Duration {
	q.expireWeights(now)
	if !(globalRPS > 0) {
		return time.Duration(math.MaxInt64)
	}
	// The target's weight cancels from its own contribution, including when it is outside the ring.
	chatFactor := 1 + q.otherChatWeight(class, chatID)/q.weightFor(class, chatID)
	return waitAtRate((float64(q.chatLen(class, chatID))+1)*chatFactor, q.classShare(class), globalRPS)
}

func (q *queue) classWait(class Class, globalRPS float64) time.Duration {
	return waitAtRate(float64(q.depth(class)), q.classShare(class), globalRPS)
}

func (q *queue) classShare(class Class) float64 {
	var totalClassWeight float64
	for c := Deadline; c <= Background; c++ {
		if c == class || q.depth(c) > 0 {
			totalClassWeight += float64(q.classes[c].quantum)
		}
	}
	return float64(q.classes[class].quantum) / totalClassWeight
}

func waitAtRate(work, share, globalRPS float64) time.Duration {
	ns := math.Round(work * float64(time.Second) / share / globalRPS)
	if ns >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(ns)
}

func (q *queue) otherChatWeight(class Class, chatID int64) float64 {
	lane := &q.classes[class]
	if class != Deadline || len(q.weights) == 0 {
		others := lane.active.Len()
		if chat := lane.chats[chatID]; chat != nil && chat.active != nil {
			others--
		}
		return float64(others)
	}
	var total float64
	for elem := lane.active.Front(); elem != nil; elem = elem.Next() {
		chat := elem.Value.(*chatQueue)
		if chat.id != chatID {
			total += q.weightFor(class, chat.id)
		}
	}
	return total
}
