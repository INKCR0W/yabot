package chats

import (
	"context"
	"errors"
	"time"

	"github.com/go-telegram/bot"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

const purgeLeaveTimeout = 5 * time.Second
const errPurgeLeaveCanceled chatError = "chat purge leave canceled before execution"

// Purge returns success after any change is committed and visible in the snapshot.
// Once leaveChat is enqueued, settlement continues even if the caller cancels.
func (c *Chats) Purge(ctx context.Context, chatID int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	membership := c.purgeMembership(chatID)
	deadline := c.now().Add(purgeLeaveTimeout)
	k := keyOf(chatID)
	result := make(chan error, 1)
	reply := func(err error) { result <- err }
	entry := dispatch.Item{
		Run: func(runCtx context.Context, sh *dispatch.Shard) {
			if err := ctx.Err(); err != nil {
				reply(err)
				return
			}
			if err := runCtx.Err(); err != nil {
				reply(err)
				return
			}
			c.startPurge(sh, k, membership, deadline, reply)
		},
		Abort: reply,
	}
	if err := c.submitPurge(k, dispatch.SettleOnPanic(func(runCtx context.Context, sh *dispatch.Shard) {
		if !sh.Busy(k) {
			entry.Run(runCtx, sh)
			return
		}
		if !sh.Defer(k, entry) {
			c.count(`yabot_chats_overload_total{source="mailbox"}`)
			c.log.Info("chat purge mailbox full", "chat_id", k.A)
			reply(ErrBusy)
		}
	}, reply)); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-result:
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}
}

func (c *Chats) purgeMembership(chatID int64) MembershipSnapshot {
	snapshot, exists := c.State(chatID)
	return MembershipSnapshot{Exists: exists, Epoch: snapshot.MemberEpoch}
}

func (c *Chats) startPurge(sh *dispatch.Shard, k dispatch.Key, membership MembershipSnapshot, deadline time.Time, reply func(error)) {
	ticket, ok := sh.Admit()
	if !ok {
		c.count(`yabot_chats_overload_total{source="purge_sender_admission"}`)
		c.log.Info("chat purge sender admission unavailable", "chat_id", k.A)
		reply(ErrBusy)
		return
	}
	done := func(result sender.Result) {
		work := &eventWork{
			event: LeaveFinished{Source: LeavePurge, Membership: membership, Result: result},
			reply: reply,
		}
		ticket.Complete(dispatch.SettleOnPanic(func(_ context.Context, sh *dispatch.Shard) {
			if errors.Is(result.Err, errPurgeLeaveCanceled) {
				c.count(`yabot_chats_effects_total{effect="leave_outcome",outcome="canceled"}`)
				c.log.Info(string(errPurgeLeaveCanceled), "chat_id", k.A)
			}
			c.enqueueInternalWork(sh, k, work)
		}, work.answer))
	}
	request := telegramRequest(sender.Request{
		Class: sender.Interactive, ChatID: k.A, Method: "leaveChat", Deadline: deadline, NoRetry: true,
		Do: func(ctx context.Context, b *bot.Bot) error {
			current := c.purgeMembership(k.A)
			if current != membership {
				return &telegram.Error{Outcome: telegram.Outcome{Class: telegram.Unknown}, Err: errPurgeLeaveCanceled}
			}
			return leaveChat(ctx, b, k.A)
		},
	}, done, done)
	if err := c.submitTelegram(request); err != nil {
		// Settle the rejection against the committed membership through the same completion path.
		reason := sender.Busy
		if errors.Is(err, sender.ErrClosed) {
			reason = sender.Shutdown
		}
		request.Done(sender.DroppedResult(reason, err))
	}
}

func (c *Chats) submitPurge(k dispatch.Key, task dispatch.Task) error {
	err := c.dispatch.Submit(k, task)
	switch {
	case errors.Is(err, dispatch.ErrBusy):
		c.count(`yabot_chats_overload_total{source="purge_inbox"}`)
		c.log.Info("chat purge inbox full", "chat_id", k.A)
		return ErrBusy
	case errors.Is(err, dispatch.ErrClosed):
		c.count(`yabot_chats_events_total{outcome="purge_dispatch_closed"}`)
		c.log.Info("chat purge could not be submitted", "chat_id", k.A)
	}
	return err
}
