package ingress

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/INKCR0W/yabot/internal/platform/telegram"
	"github.com/go-telegram/bot"
)

type telegramSource struct {
	*telegram.Client
}

func TelegramSource(c *telegram.Client) Source {
	if c == nil || c.Bot == nil {
		return nil
	}
	return telegramSource{Client: c}
}

func (s telegramSource) DeleteWebhook(ctx context.Context) error {
	token := s.Bot.Token()
	ok, err := s.Bot.DeleteWebhook(ctx, &bot.DeleteWebhookParams{DropPendingUpdates: false})
	if err != nil {
		return &telegram.Error{Outcome: telegram.Classify(err), Err: webhookDiagnostic(err, token)}
	}
	if !ok {
		return errors.New("Telegram deleteWebhook returned false")
	}
	return nil
}

func webhookDiagnostic(err error, token string) error {
	message := err.Error()
	// The library includes the entire response body when it cannot decode the envelope.
	if strings.HasPrefix(message, "error decode response body for method deleteWebhook, ") {
		return errors.New("decode Telegram deleteWebhook response body")
	}
	if token != "" {
		queryToken := url.QueryEscape(token)
		message = strings.NewReplacer(
			token, "***",
			url.PathEscape(token), "***",
			queryToken, "***",
			strings.ReplaceAll(queryToken, "%3A", "%3a"), "***",
		).Replace(message)
	}
	if code := webhookErrorCode(err); code != 0 {
		return fmt.Errorf("Telegram deleteWebhook failed (error code %d): %s", code, message)
	}
	return fmt.Errorf("Telegram deleteWebhook failed: %s", message)
}

func webhookErrorCode(err error) int {
	var migrated *bot.MigrateError
	var rateLimited *bot.TooManyRequestsError
	switch {
	case errors.Is(err, bot.ErrorBadRequest), errors.As(err, &migrated):
		return http.StatusBadRequest
	case errors.Is(err, bot.ErrorUnauthorized):
		return http.StatusUnauthorized
	case errors.Is(err, bot.ErrorForbidden):
		return http.StatusForbidden
	case errors.Is(err, bot.ErrorNotFound):
		return http.StatusNotFound
	case errors.Is(err, bot.ErrorConflict):
		return http.StatusConflict
	case errors.As(err, &rateLimited):
		return http.StatusTooManyRequests
	default:
		return 0 // Other Telegram error codes are already included in the error text.
	}
}
