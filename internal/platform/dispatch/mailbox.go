package dispatch

import (
	"context"
	"fmt"
	"time"
)

// Item requires both callbacks. The shard calls either Run or Abort once per accepted item;
// if Run panics, Abort also receives a *PanicError and must tolerate partial execution.
type Item struct {
	// Name labels panic reports and defaults to "mailbox" when empty.
	Name string
	Run  Task
	// Items Abort enqueues during shutdown are aborted too, so it must not re-enqueue unconditionally.
	Abort func(error)
}

type queuedItem struct {
	Item
	external bool
}

type mailbox struct {
	queue           []queuedItem
	replay          Item
	external        int
	busy            bool
	readyGeneration uint64
}

type readyItem struct {
	key        Key
	box        *mailbox
	generation uint64
}

// Hold panics if the entity is already busy.
func (sh *Shard) Hold(k Key) {
	sh.checkMailboxKey(k)
	box := sh.mailboxFor(k)
	if box.busy {
		panic(fmt.Sprintf("dispatch: entity %v is already held", k))
	}
	box.busy = true
}

func (sh *Shard) Busy(k Key) bool {
	sh.checkMailboxKey(k)
	box := sh.mailboxes[k]
	return box != nil && box.busy
}

// Defer returns false when the entity's external item limit is reached. The caller retains the item on rejection.
func (sh *Shard) Defer(k Key, it Item) bool {
	sh.checkMailboxKey(k)
	it.validate(k)
	box := sh.mailboxFor(k)
	if box.external >= sh.dispatcher.deps.MailboxPerEntity {
		if counter := sh.dispatcher.mailboxOverflow; counter != nil {
			counter.Inc()
		}
		return false
	}
	box.external++
	sh.enqueueMailboxItem(k, box, queuedItem{Item: it, external: true})
	return true
}

// DeferInternal queues an item without consuming the entity's external item allowance.
func (sh *Shard) DeferInternal(k Key, it Item) {
	sh.checkMailboxKey(k)
	it.validate(k)
	sh.enqueueMailboxItem(k, sh.mailboxFor(k), queuedItem{Item: it})
}

// Release makes held items ready at the tail of the shard queue, in mailbox order.
// Items for an already unheld entity keep their ready positions.
func (sh *Shard) Release(k Key) {
	sh.checkMailboxKey(k)
	box := sh.mailboxes[k]
	if box == nil || !box.busy {
		return
	}
	box.busy = false
	delete(sh.retries, k)
	if box.empty() {
		delete(sh.mailboxes, k)
		return
	}
	// Any tokens still queued from before the hold lose their earlier positions.
	box.readyGeneration++
	count := len(box.queue)
	if box.replay.Run != nil {
		count++
	}
	ready := readyItem{key: k, box: box, generation: box.readyGeneration}
	for range count {
		sh.ready = append(sh.ready, ready)
	}
}

// Requeue holds the entity and schedules a replay ahead of its FIFO. It panics while a replay is still queued.
func (sh *Shard) Requeue(k Key, it Item, attempt int) {
	sh.checkMailboxKey(k)
	it.validate(k)
	if box := sh.mailboxes[k]; box != nil && box.replay.Run != nil {
		panic(fmt.Sprintf("dispatch: entity %v already has a queued replay", k))
	}
	deadline := sh.dispatcher.deps.Now().Add(requeueBackoff(attempt))
	box := sh.mailboxFor(k)
	box.busy = true
	box.replay = it
	sh.retries[k] = deadline
}

// Drop passes reason unchanged to each pending item's Abort, in queue order.
func (sh *Shard) Drop(k Key, reason error) {
	sh.checkMailboxKey(k)
	sh.dropMailbox(k, reason)
}

func (it Item) validate(k Key) {
	if it.Run == nil || it.Abort == nil {
		panic(fmt.Sprintf("dispatch: mailbox item for %v requires Run and Abort callbacks (Run nil: %t, Abort nil: %t)", k, it.Run == nil, it.Abort == nil))
	}
}

func (it Item) panicRoute() string {
	if it.Name == "" {
		return "mailbox"
	}
	return it.Name
}

func (sh *Shard) checkMailboxKey(k Key) {
	if index := sh.dispatcher.shardIndex(k); index != sh.index {
		panic(fmt.Sprintf("dispatch: key %v belongs to shard %d and was used on shard %d", k, index, sh.index))
	}
}

func (sh *Shard) mailboxFor(k Key) *mailbox {
	box := sh.mailboxes[k]
	if box == nil {
		box = new(mailbox)
		sh.mailboxes[k] = box
	}
	return box
}

func (sh *Shard) enqueueMailboxItem(k Key, box *mailbox, item queuedItem) {
	box.queue = append(box.queue, item)
	if !box.busy {
		sh.ready = append(sh.ready, readyItem{key: k, box: box, generation: box.readyGeneration})
	}
}

func (sh *Shard) runReadyItem(ctx context.Context, ready readyItem) {
	box := ready.box
	if sh.mailboxes[ready.key] != box || ready.generation != box.readyGeneration || box.busy {
		return
	}
	it := box.pop()
	if box.empty() {
		delete(sh.mailboxes, ready.key)
	}
	// A hold acquired by Run survives a recovered panic.
	sh.execute(ctx, work{key: ready.key, route: it.panicRoute(), task: it.Run, onPanic: it.Abort})
}

func (sh *Shard) dropMailbox(k Key, reason error) {
	box := sh.mailboxes[k]
	if box == nil {
		return
	}
	delete(sh.mailboxes, k)
	delete(sh.retries, k)
	for !box.empty() {
		it := box.pop()
		sh.execute(sh.runCtx, work{key: k, route: it.panicRoute(), task: func(context.Context, *Shard) {
			it.Abort(reason)
		}})
	}
}

func (sh *Shard) abortMailboxes() {
	sh.exiting = true
	for len(sh.mailboxes) > 0 {
		for k := range sh.mailboxes {
			sh.dropMailbox(k, ErrClosed)
		}
	}
	sh.ready = nil
}

func (sh *Shard) releaseDueReplays() {
	if len(sh.retries) == 0 {
		return
	}
	// Stop does not wait out backoffs; replays still held at exit are aborted with ErrClosed.
	select {
	case <-sh.dispatcher.closing:
		return
	default:
	}
	now := sh.dispatcher.deps.Now()
	for k, deadline := range sh.retries {
		if !now.Before(deadline) {
			sh.Release(k)
		}
	}
}

func (box *mailbox) empty() bool {
	return box.replay.Run == nil && len(box.queue) == 0
}

func (box *mailbox) pop() Item {
	if box.replay.Run != nil {
		it := box.replay
		box.replay = Item{}
		return it
	}
	entry := box.queue[0]
	box.queue[0] = queuedItem{}
	box.queue = box.queue[1:]
	if entry.external {
		box.external--
	}
	return entry.Item
}

func requeueBackoff(attempt int) time.Duration {
	const initial = 100 * time.Millisecond
	const maximum = 5 * time.Second
	const firstCappedAttempt = 6
	return min(initial<<min(max(attempt, 0), firstCappedAttempt), maximum)
}
