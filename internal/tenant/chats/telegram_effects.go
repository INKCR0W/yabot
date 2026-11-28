package chats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/i18n"
	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

type periodicProbeGate struct {
	interval time.Duration
	mu       sync.Mutex
	next     time.Time
	queued   *periodicProbePermit
}

type periodicProbePermit struct{ gate *periodicProbeGate }

func (g *periodicProbeGate) reserve(now time.Time) *periodicProbePermit {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.queued != nil || now.Before(g.next) {
		return nil
	}
	// A queued request owns the permit until the sender first runs or drops it.
	g.queued = &periodicProbePermit{gate: g}
	return g.queued
}

func (p *periodicProbePermit) started(now time.Time) {
	p.finish(now.Add(p.gate.interval))
}

func (p *periodicProbePermit) finish(next time.Time) {
	if p == nil {
		return
	}
	g := p.gate
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.queued == p {
		g.queued = nil
		g.next = next
	}
}

type probeDelivery struct {
	chats  *Chats
	key    dispatch.Key
	ticket dispatch.Ticket
	result ProbeFinished
	permit *periodicProbePermit
}

func (c *Chats) sendProbe(k dispatch.Key, ticket dispatch.Ticket, request ProbeRequest, permit *periodicProbePermit) {
	p := probeDelivery{chats: c, key: k, ticket: ticket, result: ProbeFinished{Request: request}, permit: permit}
	p.getChat()
}

func (p *probeDelivery) complete() {
	p.chats.completeEvent(p.ticket, p.key, p.result)
}

func (p *probeDelivery) getChat() {
	var detail ProbeChat
	p.chats.enqueueTelegram(sender.Request{
		Class: sender.Background, ChatID: p.key.A, Method: "getChat",
		Do: func(ctx context.Context, b *bot.Bot) error {
			if p.permit != nil {
				p.permit.started(p.chats.now())
			}
			chat, err := b.GetChat(ctx, &bot.GetChatParams{ChatID: p.key.A})
			if err != nil {
				return safeTelegramError(err)
			}
			if chat == nil {
				return errors.New("getChat returned no chat")
			}
			detail = ProbeChat{
				Type: chat.Type, Title: chat.Title, JoinByRequest: chat.JoinByRequest,
				GuardVisible:  chat.GuardBot != nil,
				GuardAssigned: chat.GuardBot != nil && chat.GuardBot.ID == p.chats.botID,
			}
			return nil
		},
	}, func(result sender.Result) {
		p.permit.finish(time.Time{})
		detail.Result = result
		p.result.Chat = detail
		if !probeOK(result) {
			p.complete()
			return
		}
		p.getMember()
	}, func(result sender.Result) {
		p.result.Chat = ProbeChat{Result: result}
		p.complete()
	})
}

func (p *probeDelivery) getMember() {
	var membership Membership
	p.chats.enqueueTelegram(sender.Request{
		Class: sender.Background, ChatID: p.key.A, Method: "getChatMember",
		Do: func(ctx context.Context, b *bot.Bot) error {
			member, err := b.GetChatMember(ctx, &bot.GetChatMemberParams{ChatID: p.key.A, UserID: p.chats.botID})
			if err != nil {
				return safeTelegramError(err)
			}
			if member == nil {
				return errors.New("getChatMember returned no member")
			}
			membership = MembershipOf(*member)
			return nil
		},
	}, func(result sender.Result) {
		p.result.Member = ProbeMember{Result: result, Membership: membership}
		state, _ := membership.state()
		if !probeOK(result) || !state.isPresent() {
			p.complete()
			return
		}
		p.getAdmins()
	}, func(result sender.Result) {
		p.result.Member = ProbeMember{Result: result}
		p.complete()
	})
}

func (p *probeDelivery) getAdmins() {
	var creatorID int64
	p.chats.enqueueTelegram(sender.Request{
		Class: sender.Background, ChatID: p.key.A, Method: "getChatAdministrators",
		Do: func(ctx context.Context, b *bot.Bot) error {
			admins, err := b.GetChatAdministrators(ctx, &bot.GetChatAdministratorsParams{ChatID: p.key.A})
			if err != nil {
				return safeTelegramError(err)
			}
			if admins == nil {
				return errors.New("getChatAdministrators returned no administrator list")
			}
			for _, admin := range admins {
				if admin.Type != models.ChatMemberTypeOwner {
					continue
				}
				if admin.Owner == nil || admin.Owner.User == nil || admin.Owner.User.ID == 0 {
					return errors.New("getChatAdministrators returned a creator without a user ID")
				}
				creatorID = admin.Owner.User.ID
				break
			}
			return nil
		},
	}, func(result sender.Result) {
		p.result.Admins = ProbeAdmins{Result: result, CreatorID: creatorID}
		p.complete()
	}, func(result sender.Result) {
		p.result.Admins = ProbeAdmins{Result: result}
		p.complete()
	})
}

const errIdleLeaveCanceled chatError = "idle chat leave canceled before execution"

func (c *Chats) sendLeave(k dispatch.Key, ticket dispatch.Ticket, effect LeaveChat) bool {
	done := func(result sender.Result) {
		ticket.Complete(func(_ context.Context, sh *dispatch.Shard) {
			if errors.Is(result.Err, errIdleLeaveCanceled) {
				c.count(`yabot_chats_effects_total{effect="leave_outcome",outcome="canceled"}`)
				c.log.Info("idle chat leave canceled before execution", "chat_id", k.A)
			}
			c.enqueueInternal(sh, k, LeaveFinished{
				Source: effect.Source, Membership: effect.Membership, FactVersion: effect.FactVersion, Result: result,
			})
			if effect.Source == LeaveRejected && hasLeft(result) && result.Outcome.Class != telegram.OK {
				c.enqueueMigrationResult(sh, k, result)
				c.observeMetric(k.A, Metric{Kind: LeaveOutcome, Outcome: result.Outcome})
			}
		})
	}
	request := telegramRequest(sender.Request{
		Class: effect.Class, ChatID: k.A, Method: "leaveChat", NoRetry: effect.NoRetry,
		Do: func(ctx context.Context, b *bot.Bot) error {
			if effect.Source == LeaveIdle && !c.canExecuteIdleLeave(k.A, effect) {
				// There is no Telegram outcome for a request canceled locally.
				return &telegram.Error{Outcome: telegram.Outcome{Class: telegram.Unknown}, Err: errIdleLeaveCanceled}
			}
			left, err := b.LeaveChat(ctx, &bot.LeaveChatParams{ChatID: k.A})
			if err == nil && !left {
				return errors.New("leaveChat returned false")
			}
			return safeTelegramError(err)
		},
	}, done, done)
	if err := c.submitTelegram(request); err != nil {
		if effect.Source == LeaveRejected && errors.Is(err, sender.ErrBusy) {
			c.count(`yabot_chats_overload_total{source="sender_queue"}`)
			return false
		}
		request.Done(sender.DroppedResult(sender.Busy, err))
	}
	return true
}

func (c *Chats) canExecuteIdleLeave(chatID int64, effect LeaveChat) bool {
	entry, exists := c.view.lookup(chatID)
	return exists && entry.snapshot.MemberEpoch == effect.Membership.Epoch && entry.factVersion == effect.FactVersion &&
		entry.snapshot.State.canLeaveIdle() && !entry.snapshot.EverActive
}

func (c *Chats) sendOnboarding(sh *dispatch.Shard, k dispatch.Key, ticket dispatch.Ticket, effect OnboardingMessage) {
	c.local(sh).onboarding.reserve(k.A)
	c.sendChatMessage(k.A, func() bot.SendMessageParams {
		return c.onboardingParams(k.A, effect)
	}, func(result sender.Result) {
		completedAt := c.now()
		ticket.Complete(func(_ context.Context, sh *dispatch.Shard) {
			c.enqueueMigrationResult(sh, k, result)
			next := c.local(sh).onboarding.finish(k.A, result.Outcome.Class == telegram.Permission, completedAt)
			c.queueOnboarding(sh, k, next)
			c.observeDelivery(k.A, "onboarding", result)
		})
	})
}

func (c *Chats) onboardingParams(chatID int64, effect OnboardingMessage) bot.SendMessageParams {
	lang := i18n.Resolve(c.general.Get(chatID).Language, effect.Language.Actor)
	guardKey := "chats.onboarding.guard_unknown"
	if effect.GuardKnown {
		guardKey = "chats.onboarding.guard_unassigned"
		if effect.GuardAssigned {
			guardKey = "chats.onboarding.guard_assigned"
		}
	}
	parts := []string{
		c.catalog.HTML(lang, "chats.onboarding.intro", nil),
		c.catalog.HTML(lang, guardKey, nil),
	}
	if effect.JoinByRequest {
		parts = append(parts, c.catalog.HTML(lang, "chats.onboarding.join_by_request", nil))
	}
	buttons := [][]models.InlineKeyboardButton{{{
		Text: c.catalog.T(lang, "chats.buttons.settings", nil),
		URL:  "https://t.me/" + c.botUsername + "?startapp=c" + strconv.FormatInt(chatID, 10),
	}}}
	if effect.NeedsInviteUsers {
		parts = append(parts, c.catalog.HTML(lang, "chats.onboarding.missing_invite", nil))
		buttons = append(buttons, []models.InlineKeyboardButton{c.inviteButton(lang)})
	} else {
		parts = append(parts, c.catalog.HTML(lang, "chats.onboarding.has_invite", nil))
	}
	return bot.SendMessageParams{
		ChatID: chatID, Text: strings.Join(parts, "\n\n"), ParseMode: models.ParseModeHTML,
		ReplyMarkup: models.InlineKeyboardMarkup{InlineKeyboard: buttons}, DisableNotification: true,
	}
}

func (c *Chats) sendDowngrade(k dispatch.Key, ticket dispatch.Ticket, effect DowngradeMessage) {
	c.sendChatMessage(k.A, func() bot.SendMessageParams {
		lang := i18n.Resolve(c.general.Get(k.A).Language, effect.Language.Actor)
		return bot.SendMessageParams{
			ChatID: k.A, Text: c.catalog.HTML(lang, "chats.downgrade.notice", nil), ParseMode: models.ParseModeHTML,
			ReplyMarkup: models.InlineKeyboardMarkup{InlineKeyboard: [][]models.InlineKeyboardButton{{c.inviteButton(lang)}}}, DisableNotification: true,
		}
	}, func(result sender.Result) {
		ticket.Complete(func(_ context.Context, sh *dispatch.Shard) {
			c.enqueueMigrationResult(sh, k, result)
			c.observeDelivery(k.A, "downgrade", result)
		})
	})
}

func (c *Chats) inviteButton(lang string) models.InlineKeyboardButton {
	return models.InlineKeyboardButton{
		Text: c.catalog.T(lang, "chats.buttons.grant_invite", nil),
		URL:  "https://t.me/" + c.botUsername + "?startgroup&admin=invite_users",
	}
}

func (c *Chats) enqueueMigrationResult(sh *dispatch.Shard, k dispatch.Key, result sender.Result) {
	if result.Dropped == 0 && result.Outcome.Class == telegram.Migrated {
		c.enqueueInternal(sh, k, MigrationReported{To: result.Outcome.MigrateTo})
	}
}

func (c *Chats) sendChatMessage(chatID int64, params func() bot.SendMessageParams, done func(sender.Result)) {
	c.enqueueTelegram(sender.Request{
		Class: sender.Background, ChatID: chatID, Method: "sendMessage", NoRetry: true,
		Do: func(ctx context.Context, b *bot.Bot) error {
			message := params()
			_, err := b.SendMessage(ctx, &message)
			return safeTelegramError(err)
		},
	}, func(result sender.Result) {
		if result.Dropped == sender.Busy && errors.Is(result.Err, sender.ErrClosed) {
			result.Dropped = sender.Shutdown
		}
		done(result)
	}, done)
}

func (c *Chats) enqueueTelegram(request sender.Request, done, failure func(sender.Result)) {
	request = telegramRequest(request, done, failure)
	if err := c.submitTelegram(request); err != nil {
		request.Done(sender.DroppedResult(sender.Busy, err))
	}
}

func telegramRequest(request sender.Request, done, failure func(sender.Result)) sender.Request {
	// A panic while chaining requests still has to return the caller's ticket.
	request.Done = func(result sender.Result) {
		returned := false
		defer func() {
			if !returned {
				recover()
				failure(telegramCallbackFailure("telegram completion callback panicked"))
			}
		}()
		done(result)
		returned = true
	}
	return request
}

func (c *Chats) submitTelegram(request sender.Request) error {
	returned := false
	defer func() {
		if !returned {
			recover()
			request.Done(telegramCallbackFailure("telegram enqueue panicked"))
		}
	}()
	err := c.sender.Enqueue(request)
	returned = true
	return err
}

func telegramCallbackFailure(message string) sender.Result {
	return sender.Result{Outcome: telegram.Outcome{Class: telegram.Fatal}, Err: errors.New(message)}
}

func safeTelegramError(err error) error {
	if err == nil {
		return nil
	}
	// The SDK can include the response body or API description in its errors.
	outcome := telegram.Classify(err)
	cause := err
	for inner := errors.Unwrap(cause); inner != nil; inner = errors.Unwrap(cause) {
		cause = inner
	}
	return &telegram.Error{Outcome: outcome, Err: fmt.Errorf("telegram request failed (%s, %T)", outcome.Class, cause)}
}
