package chats

import (
	"container/heap"
	"context"
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
)

const onboardingCooldownLimit = 100_000

type onboardingCooldowns struct {
	entries map[int64]*onboardingAttempt
	expiry  onboardingExpiry
}

type onboardingAttempt struct {
	chatID int64
	until  time.Time
	next   onboardingIntent
}

// Sender callbacks can reach the shard in a different order than their timestamps.
type onboardingExpiry []*onboardingAttempt

func (h onboardingExpiry) Len() int           { return len(h) }
func (h onboardingExpiry) Less(i, j int) bool { return h[i].until.Before(h[j].until) }
func (h onboardingExpiry) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *onboardingExpiry) Push(entry any)    { *h = append(*h, entry.(*onboardingAttempt)) }
func (h *onboardingExpiry) Pop() any {
	last := len(*h) - 1
	entry := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return entry
}

type onboardingIntent struct {
	memberEpoch  uint64
	languageCode string
}

func (c *Chats) onboardingBlocked(sh *dispatch.Shard, chatID int64, memberEpoch uint64, languageCode string) bool {
	cooldowns := &c.local(sh).onboarding
	cooldowns.expire(c.now())
	if attempt := cooldowns.entries[chatID]; attempt != nil {
		if attempt.until.IsZero() {
			attempt.next = onboardingIntent{memberEpoch: memberEpoch, languageCode: languageCode}
		}
		return true
	}
	// At capacity, preserve existing cooldowns and shed new messages.
	if len(cooldowns.entries) >= onboardingCooldownLimit {
		c.count(`yabot_chats_overload_total{source="onboarding_cooldown"}`)
		c.log.Warn("chat onboarding cooldown is full", "chat_id", chatID)
		return true
	}
	return false
}

func (c *onboardingCooldowns) reserve(chatID int64) {
	if c.entries == nil {
		c.entries = make(map[int64]*onboardingAttempt)
	}
	c.entries[chatID] = &onboardingAttempt{chatID: chatID}
}

func (c *onboardingCooldowns) finish(chatID int64, permissionFailure bool, completedAt time.Time) onboardingIntent {
	attempt := c.entries[chatID]
	if permissionFailure {
		delete(c.entries, chatID)
		return attempt.next
	}
	attempt.until = completedAt.Add(MessageCooldown)
	attempt.next = onboardingIntent{}
	heap.Push(&c.expiry, attempt)
	return onboardingIntent{}
}

func (c *Chats) queueOnboarding(sh *dispatch.Shard, k dispatch.Key, intent onboardingIntent) {
	if intent.memberEpoch == 0 {
		return
	}
	work := &eventWork{}
	sh.DeferInternal(k, dispatch.Item{
		Run: func(ctx context.Context, sh *dispatch.Shard) {
			if ctx.Err() != nil {
				return
			}
			entity := c.local(sh).records[k.A]
			if entity == nil || !entity.state.Record.State.isPresent() || entity.state.Record.Type == models.ChatTypeChannel ||
				entity.state.MemberEpoch != intent.memberEpoch {
				return
			}
			effect := entity.state.onboardingMessage(LanguageCandidates{Actor: intent.languageCode})
			c.runEffects(ctx, sh, k, entity, work, []Effect{effect}, 0)
		},
		Abort: work.answer,
	})
}

func (c *onboardingCooldowns) expire(now time.Time) {
	for len(c.expiry) != 0 && !now.Before(c.expiry[0].until) {
		attempt := heap.Pop(&c.expiry).(*onboardingAttempt)
		delete(c.entries, attempt.chatID)
	}
}
