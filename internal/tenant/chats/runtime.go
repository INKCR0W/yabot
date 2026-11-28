package chats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

const maxOpReplays = 3

type eventWork struct {
	event      Event
	reply      func(error)
	entered    func()
	caller     context.Context
	claim      ClaimToken
	attempt    int
	opFailures int
}

type pendingWrite struct {
	entity    *chatEntity
	recorded  bool
	decision  Decision
	proposed  time.Time
	operation string
}

func (c *Chats) handleMemberUpdate(ctx context.Context, sh *dispatch.Shard, u *models.Update, _ time.Time, ack dispatch.Ack) {
	update, ok := MemberUpdateOf(u)
	newState, _ := update.New.state()
	if !ok || u.MyChatMember.Chat.ID == 0 || update.Order.ID <= 0 || update.Order.Date <= 0 ||
		update.Old.Status == "" || newState == NoRecord {
		c.count(`yabot_chats_events_total{outcome="invalid_update"}`)
		c.log.Error("malformed chat membership update", "update_id", update.Order.ID)
		ack.Done(dispatch.Permanent(errors.New("chats: invalid membership update")))
		return
	}
	chatID := u.MyChatMember.Chat.ID
	c.enqueueExternal(ctx, sh, keyOf(chatID), &eventWork{
		event: update, reply: ack.Done,
	})
}

// SetOwner publishes a verified claim after commit. It returns ErrBusy when the token is stale.
func (c *Chats) SetOwner(ctx context.Context, chatID, userID int64, token ClaimToken) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	result := make(chan error, 1)
	err := c.dispatch.Submit(keyOf(chatID), func(runCtx context.Context, sh *dispatch.Shard) {
		c.enqueueExternal(runCtx, sh, keyOf(chatID), &eventWork{
			event:  OwnerSet{UserID: userID},
			reply:  func(err error) { result <- err },
			caller: ctx, claim: token,
		})
	})
	if errors.Is(err, dispatch.ErrBusy) {
		c.count(`yabot_chats_overload_total{source="owner_inbox"}`)
		return ErrBusy
	}
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-result:
		return err
	}
}

func (c *Chats) enqueueExternal(ctx context.Context, sh *dispatch.Shard, k dispatch.Key, work *eventWork) {
	item := c.eventItem(k, work)
	if sh.Busy(k) {
		if !sh.Defer(k, item) {
			c.count(`yabot_chats_overload_total{source="mailbox"}`)
			work.answer(ErrBusy)
		}
		return
	}
	if _, member := work.event.(MemberUpdate); member {
		item.Run(ctx, sh)
		return
	}
	dispatch.SettleOnPanic(item.Run, item.Abort)(ctx, sh)
}

func (c *Chats) enqueueInternal(sh *dispatch.Shard, k dispatch.Key, event Event) {
	c.enqueueInternalWork(sh, k, &eventWork{event: event})
}

func (c *Chats) enqueueInternalWork(sh *dispatch.Shard, k dispatch.Key, work *eventWork) {
	sh.DeferInternal(k, c.eventItem(k, work))
}

func (c *Chats) completeEvent(ticket dispatch.Ticket, k dispatch.Key, event Event) {
	ticket.Complete(func(_ context.Context, sh *dispatch.Shard) {
		c.enqueueInternal(sh, k, event)
	})
}

func (c *Chats) eventItem(k dispatch.Key, work *eventWork) dispatch.Item {
	var name string
	if _, member := work.event.(MemberUpdate); member {
		name = memberRoute
	}
	return dispatch.Item{
		Name: name,
		Run: func(ctx context.Context, sh *dispatch.Shard) {
			work.enter()
			c.runEvent(ctx, sh, k, work)
		},
		Abort: func(err error) {
			work.enter()
			var panicErr *dispatch.PanicError
			if _, member := work.event.(MemberUpdate); member && !errors.As(err, &panicErr) && !errors.Is(err, dispatch.ErrClosed) {
				err = nil
			}
			work.answer(err)
		},
	}
}

func (w *eventWork) enter() {
	if w.entered != nil {
		entered := w.entered
		w.entered = nil
		entered()
	}
}

func (w *eventWork) answer(err error) {
	if w.reply != nil {
		reply := w.reply
		w.reply = nil
		reply(err)
	}
}

func (c *Chats) runEvent(ctx context.Context, sh *dispatch.Shard, k dispatch.Key, work *eventWork) {
	if err := ctx.Err(); err != nil {
		work.answer(err)
		return
	}
	now := c.now()
	local := c.local(sh)
	entity := c.entity(sh, local, k.A, now)
	if trigger, ok := work.event.(ProbeTriggered); ok && trigger.Trigger == ProbePeriodic &&
		within(now, entity.state.Probe.LastStarted, c.probePeriodic) {
		return
	}
	event := work.event
	switch ev := event.(type) {
	case OwnerSet:
		if err := work.caller.Err(); err != nil {
			work.answer(err)
			return
		}
		if reason := c.invalidClaimReason(work.claim, k.A, ev.UserID, entity.state.MemberEpoch); reason != "" {
			work.answer(c.rejectClaim(k.A, ev.UserID, reason))
			return
		}
	case ProbeFinished:
		if ev.Admins.CreatorID != 0 && c.erasure.changedSince(ev.Admins.CreatorID, ev.Request.ErasureSequence) {
			ev.SkipOwner = true
			event = ev
		}
	}
	decision := Step(entity.state, event, now, c.policyFor(k.A))
	if decision.Stale {
		c.observeStale(k.A, work.event)
		if _, clear := event.(ClearFinished); clear {
			work.answer(ErrMembershipChanged)
			return
		}
	}
	if len(decision.Effects) != 0 {
		switch effect := decision.Effects[0].(type) {
		case Persist:
			c.persist(ctx, sh, k, work, entity, decision, now, "upsert_chat", writeRecord(effect.Record))
			return
		case DeleteRecord:
			c.persist(ctx, sh, k, work, entity, decision, now, "delete_chat", deleteRecord(effect.ChatID))
			return
		}
	}
	if !c.install(sh, local, entity, decision.Next, now) {
		work.answer(ErrBusy)
		return
	}
	c.runEffects(ctx, sh, k, entity, work, decision.Effects, 0)
	work.answer(nil)
}

func (c *Chats) policyFor(chatID int64) Policy {
	policy := c.policy
	policy.Language = c.general.Get(chatID).Language
	return policy
}

func (c *Chats) persist(ctx context.Context, sh *dispatch.Shard, k dispatch.Key, work *eventWork, entity *chatEntity, decision Decision, now time.Time, operation string, op store.Op) {
	if !c.ensureRecordlessMemory(sh, entity) {
		work.answer(ErrBusy)
		return
	}
	ticket, ok := sh.Admit()
	if !ok {
		c.writeFailed(ctx, sh, k, work, entity, operation, ErrBusy, false)
		return
	}
	local := c.local(sh)
	// The writer owns the submitted value. Keep only effects that run after commit in the shard.
	decision.Effects[0] = nil
	decision.Effects = decision.Effects[1:]
	pending := &pendingWrite{entity: entity, recorded: local.records[k.A] != nil, decision: decision, proposed: now, operation: operation}
	sh.Hold(k)
	local.pending[k.A] = pending
	handedOff := false
	defer func() {
		if !handedOff {
			ticket.Cancel()
			delete(local.pending, k.A)
			sh.Release(k)
		}
	}()
	err := c.store.Write(op, func(err error) {
		ticket.Complete(dispatch.SettleOnPanic(func(ctx context.Context, sh *dispatch.Shard) {
			c.finishWrite(ctx, sh, k, work, pending, err)
		}, work.answer))
	})
	if err == nil {
		handedOff = true
		return
	}
	ticket.Cancel()
	delete(local.pending, k.A)
	handedOff = true
	c.finishSyncWriteFailure(ctx, sh, k, work, entity, operation, err)
}

func (c *Chats) finishWrite(ctx context.Context, sh *dispatch.Shard, k dispatch.Key, work *eventWork, pending *pendingWrite, err error) {
	local := c.local(sh)
	if local.pending[k.A] != pending {
		work.answer(dispatch.ErrClosed)
		return
	}
	delete(local.pending, k.A)
	want := pending.entity
	if !pending.recorded {
		want = nil
	}
	if local.records[k.A] != want {
		work.answer(dispatch.ErrClosed)
		return
	}
	requeued := false
	defer func() {
		if !requeued {
			sh.Release(k)
		}
	}()
	claim, isClaim := work.event.(OwnerSet)
	claimErased := isClaim && pending.decision.Next.Record.OwnerID != claim.UserID
	if err != nil {
		if claimErased {
			work.answer(c.rejectClaim(k.A, claim.UserID, "erasure"))
		}
		requeued = c.writeFailed(ctx, sh, k, work, pending.entity, pending.operation, err, true)
		return
	}
	decision := pending.decision
	if decision.NewMemberEpoch {
		local.epoch++
		decision.Next.MemberEpoch = local.epoch
	}
	c.install(sh, local, pending.entity, decision.Next, pending.proposed)
	if claimErased {
		work.answer(c.rejectClaim(k.A, claim.UserID, "erasure"))
	}
	requeued = c.runEffects(ctx, sh, k, pending.entity, work, decision.Effects, 0)
	work.answer(nil)
}

func (c *Chats) finishSyncWriteFailure(ctx context.Context, sh *dispatch.Shard, k dispatch.Key, work *eventWork, entity *chatEntity, operation string, err error) {
	requeued := false
	defer func() {
		if !requeued {
			sh.Release(k)
		}
	}()
	requeued = c.writeFailed(ctx, sh, k, work, entity, operation, err, false)
}

func (c *Chats) writeFailed(ctx context.Context, sh *dispatch.Shard, k dispatch.Key, work *eventWork, entity *chatEntity, operation string, err error, async bool) bool {
	var opErr *store.OpError
	permanent := async && errors.As(err, &opErr)
	c.observeWriteFailure(k.A, operation, err, permanent)
	switch work.event.(type) {
	case MemberUpdate:
		err = redactWriteReceiptError(operation, err)
		if permanent {
			err = dispatch.Permanent(&store.OpError{Err: err})
		}
		work.answer(err)
		return false
	case OwnerSet:
		if errors.Is(err, store.ErrBusy) {
			err = ErrBusy
		}
		work.answer(err)
		return false
	}
	if permanent {
		work.opFailures++
		if work.opFailures > maxOpReplays {
			c.count("yabot_chats_write_abandoned_total")
			c.log.Error("chat event abandoned after write failures", "chat_id", k.A, "event", fmt.Sprintf("%T", work.event),
				"operation", operation, "sqlite_code", sqliteErrorCode(err))
			if event, ok := work.event.(ProbeFinished); ok {
				now := c.now()
				decision := AbandonProbe(entity.state, event.Request, now, c.policyFor(k.A))
				c.install(sh, c.local(sh), entity, decision.Next, now)
				return c.runEffects(ctx, sh, k, entity, work, decision.Effects, 0)
			}
			work.answer(&store.OpError{Err: redactWriteReceiptError(operation, err)})
			return false
		}
	}
	work.attempt++
	requeue(sh, k, entity, c.eventItem(k, work), work.attempt)
	return true
}

func requeue(sh *dispatch.Shard, k dispatch.Key, entity *chatEntity, item dispatch.Item, attempt int) {
	if entity.state.Record.State != NoRecord {
		sh.Requeue(k, item, attempt)
		return
	}
	// A due replay can be ready but not yet running after the dispatcher releases its hold.
	unpin := func() {
		if entity.replay == &item {
			entity.replay = nil
		}
	}
	sh.Requeue(k, dispatch.Item{
		Name: item.Name,
		Run: func(ctx context.Context, sh *dispatch.Shard) {
			defer unpin()
			item.Run(ctx, sh)
		},
		Abort: func(err error) {
			unpin()
			item.Abort(err)
		},
	}, attempt)
	entity.replay = &item
}

func (c *Chats) runEffects(ctx context.Context, sh *dispatch.Shard, k dispatch.Key, entity *chatEntity, work *eventWork, effects []Effect, attempt int) bool {
	for i, effect := range effects {
		switch effect := effect.(type) {
		case UpdateWatermark:
		case Metric:
			c.observeMetric(k.A, effect)
		case Reply:
			work.answer(replyError(effect))
		case StartProbe, OnboardingMessage, DowngradeMessage, LeaveChat:
			if !c.ensureRecordlessMemory(sh, entity) {
				continue
			}
			if !c.sendEffect(sh, k, entity, effect) {
				c.log.Info("chat effect waiting for sender admission", "chat_id", k.A, "effect", fmt.Sprintf("%T", effect))
				requeue(sh, k, entity, dispatch.Item{
					Run: func(ctx context.Context, sh *dispatch.Shard) {
						if ctx.Err() == nil {
							c.runEffects(ctx, sh, k, entity, work, effects[i:], attempt+1)
							return
						}
						work.answer(ctx.Err())
					},
					Abort: work.answer,
				}, attempt+1)
				return true
			}
		default:
			panic("chats: unexpected effect")
		}
	}
	return false
}

func (c *Chats) sendEffect(sh *dispatch.Shard, k dispatch.Key, entity *chatEntity, effect Effect) bool {
	if message, onboarding := effect.(OnboardingMessage); onboarding &&
		c.onboardingBlocked(sh, k.A, entity.state.MemberEpoch, message.Language.Actor) {
		return true
	}
	var permit *periodicProbePermit
	if probe, ok := effect.(StartProbe); ok && probe.Trigger == ProbePeriodic {
		permit = c.periodicStart.reserve(c.now())
		if permit == nil {
			c.observeMetric(k.A, Metric{Kind: ProbeLimited, Trigger: ProbePeriodic})
			return true
		}
	}
	handedOff := false
	defer func() {
		if !handedOff {
			permit.finish(time.Time{})
		}
	}()
	ticket, ok := sh.Admit()
	if !ok {
		c.count(`yabot_chats_overload_total{source="sender_ticket"}`)
		return false
	}
	defer func() {
		if !handedOff {
			ticket.Cancel()
		}
	}()
	switch effect := effect.(type) {
	case StartProbe:
		now := c.now()
		state, request := BeginProbe(entity.state, effect, now, c.erasure.sequence.Load())
		c.install(sh, c.local(sh), entity, state, now)
		c.sendProbe(k, ticket, request, permit)
	case LeaveChat:
		if !c.sendLeave(k, ticket, effect) {
			return false
		}
	case OnboardingMessage:
		c.sendOnboarding(sh, k, ticket, effect)
	case DowngradeMessage:
		c.sendDowngrade(k, ticket, effect)
	}
	handedOff = true
	return true
}

func replyError(reply Reply) error {
	switch reply.Status {
	case ReplyOK:
		return nil
	case ReplyForbidden:
		return ErrForbidden
	case ReplyMembershipChanged:
		return ErrMembershipChanged
	case ReplyNotLeft:
		return &telegram.Error{Outcome: reply.Result.Outcome, Err: reply.Result.Err}
	default:
		panic("chats: unexpected reply")
	}
}
