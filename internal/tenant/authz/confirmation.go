package authz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

const ownerCheckTimeout = 5 * time.Second

type ownership struct {
	creator   bool
	anonymous bool
}

type ownerCheck struct {
	owner   ownership
	failure string
	level   slog.Level
}

func (c ownerCheck) result() (ownership, error) {
	if c.failure != "" {
		return ownership{}, ErrUnavailable
	}
	return c.owner, nil
}

// ConfirmOwner submits an independent live check. Claim callers must obtain their claim token first.
func (z *Authz) ConfirmOwner(ctx context.Context, userID, chatID int64) (creator, anonymous bool, err error) {
	logCtx := context.WithoutCancel(ctx)
	done, err := z.submitOwnerCheck(userID, chatID)
	if err != nil {
		go z.observeOwnerCheck(logCtx, userID, chatID, rejectedOwnerCheck(err))
		if err := ctx.Err(); err != nil {
			return false, false, err
		}
		return false, false, ErrUnavailable
	}
	select {
	case checked := <-done:
		go z.observeOwnerCheck(logCtx, userID, chatID, checked)
		if ctx.Err() == nil {
			owner, err := checked.result()
			return owner.creator, owner.anonymous, err
		}
	case <-ctx.Done():
		go func() { z.observeOwnerCheck(logCtx, userID, chatID, <-done) }()
	}
	go z.observeConfirmationFailure(logCtx, userID, chatID, "canceled", slog.LevelInfo)
	return false, false, ctx.Err()
}

func (z *Authz) submitOwnerCheck(userID, chatID int64) (<-chan ownerCheck, error) {
	var owner ownership
	done := make(chan ownerCheck, 1)
	err := z.deps.Sender.Enqueue(sender.Request{
		Class:    sender.Interactive,
		ChatID:   chatID,
		Method:   "getChatMember",
		Deadline: z.deps.Now().Add(ownerCheckTimeout),
		Do: func(callCtx context.Context, b *bot.Bot) error {
			var err error
			owner, err = fetchOwner(callCtx, b, userID, chatID)
			return err
		},
		Done: func(result sender.Result) {
			if result.Dropped != 0 || result.Outcome.Class != telegram.OK {
				outcome, level := confirmationFailure(result)
				done <- ownerCheck{failure: outcome, level: level}
				return
			}
			done <- ownerCheck{owner: owner}
		},
	})
	return done, err
}

func fetchOwner(ctx context.Context, b *bot.Bot, userID, chatID int64) (ownership, error) {
	member, err := b.GetChatMember(ctx, &bot.GetChatMemberParams{ChatID: chatID, UserID: userID})
	if err != nil {
		return ownership{}, safeOwnerCheckError(err, b.Token())
	}
	if member == nil {
		return ownership{}, errors.New("getChatMember returned no member")
	}
	if member.Type != models.ChatMemberTypeOwner {
		return ownership{}, nil
	}
	if member.Owner == nil {
		return ownership{}, errors.New("getChatMember returned no creator data")
	}
	return ownership{creator: true, anonymous: member.Owner.IsAnonymous}, nil
}

func safeOwnerCheckError(err error, token string) error {
	outcome := telegram.Classify(err)
	cause := errors.Unwrap(err)
	if description := ownerCheckDescription(err, cause); description != "" {
		if token != "" {
			escaped := url.QueryEscape(token)
			description = strings.NewReplacer(token, "***", url.PathEscape(token), "***", escaped, "***",
				strings.ReplaceAll(escaped, "%3A", "%3a"), "***").Replace(description)
		}
		return &telegram.Error{Outcome: outcome, Err: errors.New(description)}
	}
	if cause == nil {
		cause = err
	}
	return &telegram.Error{Outcome: outcome, Err: fmt.Errorf("getChatMember failed (%T)", cause)}
}

func ownerCheckDescription(err, cause error) string {
	// The SDK wraps known response codes with sentinels and returns structured rate-limit and migration errors.
	switch cause {
	case bot.ErrorBadRequest, bot.ErrorUnauthorized, bot.ErrorForbidden, bot.ErrorNotFound, bot.ErrorConflict:
		_, description, _ := strings.Cut(err.Error(), ", ")
		return description
	}
	// The platform HTTP client can return a typed API error through the SDK's HTTP error wrapper.
	if classified, ok := cause.(*telegram.Error); ok {
		cause = classified.Err
	}
	for _, candidate := range []error{err, cause} {
		switch apiErr := candidate.(type) {
		case *bot.TooManyRequestsError:
			_, description, _ := strings.Cut(apiErr.Message, ", ")
			return description
		case *bot.MigrateError:
			_, description, _ := strings.Cut(apiErr.Message, ": ")
			return description
		}
	}
	return ""
}

func rejectedOwnerCheck(err error) ownerCheck {
	switch {
	case errors.Is(err, sender.ErrBusy):
		return ownerCheck{failure: "busy", level: slog.LevelInfo}
	case errors.Is(err, sender.ErrWouldMiss):
		return ownerCheck{failure: "would_miss", level: slog.LevelInfo}
	case errors.Is(err, sender.ErrClosed):
		return ownerCheck{failure: "shutdown", level: slog.LevelInfo}
	default:
		return ownerCheck{failure: "enqueue_error", level: slog.LevelError}
	}
}

func confirmationFailure(result sender.Result) (string, slog.Level) {
	if result.Dropped != 0 {
		return result.Dropped.String(), slog.LevelInfo
	}
	switch result.Outcome.Class {
	case telegram.Fatal, telegram.Unknown, telegram.Unauthorized, telegram.Conflict:
		return result.Outcome.Class.String(), slog.LevelError
	}
	if result.Outcome.Reason != telegram.ReasonNone {
		return result.Outcome.Reason.String(), slog.LevelInfo
	}
	return result.Outcome.Class.String(), slog.LevelInfo
}

func (z *Authz) countConfirmation(outcome string) {
	z.count(fmt.Sprintf(`yabot_authz_owner_checks_total{outcome=%q}`, outcome))
}

func (z *Authz) observeOwnerCheck(ctx context.Context, userID, chatID int64, checked ownerCheck) {
	if checked.failure != "" {
		z.observeConfirmationFailure(ctx, userID, chatID, checked.failure, checked.level)
		return
	}
	outcome := "not_creator"
	if checked.owner.creator {
		outcome = "creator"
	}
	z.countConfirmation(outcome)
}

func (z *Authz) observeConfirmationFailure(ctx context.Context, userID, chatID int64, outcome string, level slog.Level) {
	z.countConfirmation(outcome)
	z.deps.Log.Log(ctx, level, "ownership check failed", "user_id", userID, "chat_id", chatID, "outcome", outcome)
}
