package ingress

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/go-telegram/bot/models"
)

type batchState struct {
	dedupe    dedupeRing
	poison    map[int64]int
	transient map[int64]time.Time
}

func newBatchState() *batchState {
	return &batchState{
		dedupe:    dedupeRing{seen: make(map[int64]struct{}, DedupeCapacity)},
		poison:    make(map[int64]int, BatchLimit),
		transient: make(map[int64]time.Time, BatchLimit),
	}
}

func (s *batchState) settled(id int64) {
	s.dedupe.add(id)
	delete(s.poison, id)
	delete(s.transient, id)
}

func (s *batchState) prune(offset int64) {
	for id := range s.poison {
		if id < offset {
			delete(s.poison, id)
		}
	}
	for id := range s.transient {
		if id < offset {
			delete(s.transient, id)
		}
	}
}

type dedupeRing struct {
	ids  [DedupeCapacity]int64
	seen map[int64]struct{}
	next int
}

func (r *dedupeRing) has(id int64) bool {
	_, ok := r.seen[id]
	return ok
}

func (r *dedupeRing) add(id int64) {
	if r.has(id) {
		return
	}
	if len(r.seen) == len(r.ids) {
		delete(r.seen, r.ids[r.next])
	}
	r.ids[r.next] = id
	r.seen[id] = struct{}{}
	r.next = (r.next + 1) % len(r.ids)
}

func (i *Ingress) settleBatch(ctx context.Context, state *batchState, updates []models.Update, received time.Time) (bool, error) {
	type routedUpdate struct {
		update  *models.Update
		receipt dispatch.Receipt
	}
	deadline := received.Add(i.deps.SettleTimeout)
	pending := make([]routedUpdate, 0, len(updates))
	for idx := range updates {
		update := &updates[idx]
		if !state.dedupe.has(update.ID) {
			pending = append(pending, routedUpdate{update: update, receipt: i.deps.Router.Accept(update, received)})
		}
	}
	batchOK := true
	for _, item := range pending {
		ok, err := i.settle(ctx, state, item.update, item.receipt.Wait(time.Until(deadline)))
		if err != nil {
			return false, err
		}
		if ok {
			state.settled(item.update.ID)
		} else {
			batchOK = false
		}
	}
	return batchOK, nil
}

func (i *Ingress) settle(ctx context.Context, state *batchState, update *models.Update, err error) (bool, error) {
	id := update.ID
	switch {
	case err == nil:
		return true, nil
	case !dispatch.IsPermanent(err):
		return false, i.recordTransient(ctx, state, id, err)
	}

	if i.metrics.poison != nil {
		i.metrics.poison.Inc()
	}
	state.poison[id]++
	if state.poison[id] < i.deps.PoisonThreshold {
		i.deps.Log.InfoContext(ctx, "retry permanently failed update", "update_id", id, "attempt", state.poison[id], "error", err)
		return false, nil
	}

	kind, _ := updateKind(update)
	summary := errorSummary(err)
	if err := i.writeDeadLetter(ctx, id, kind, summary); err != nil {
		err = fmt.Errorf("write dead letter for update %d: %w", id, err)
		var opErr *store.OpError
		if errors.As(err, &opErr) {
			return false, err
		}
		return false, i.recordTransient(ctx, state, id, err)
	}
	if i.metrics.deadLetters != nil {
		i.metrics.deadLetters.Inc()
	}
	i.deps.Log.ErrorContext(ctx, "update moved to dead letter", "update_id", id, "kind", kind, "error", summary)
	return true, nil
}

func (i *Ingress) recordTransient(ctx context.Context, state *batchState, id int64, err error) error {
	now := i.deps.Now()
	if first, seen := state.transient[id]; !seen {
		state.transient[id] = now
	} else if now.Sub(first) > i.deps.TransientBudget {
		return fmt.Errorf("ingress transient failure budget exceeded for update %d: %w", id, err)
	}
	i.deps.Log.InfoContext(ctx, "retry transient update failure", "update_id", id, "error", err)
	return nil
}
