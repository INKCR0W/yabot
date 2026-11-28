package chats

import (
	"errors"
	"fmt"

	"modernc.org/sqlite"

	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

func (c *Chats) count(name string) {
	if c.metrics != nil {
		c.metrics.GetOrCreateCounter(name).Inc()
	}
}

func (c *Chats) rejectClaim(chatID, userID int64, reason string) error {
	c.count(fmt.Sprintf(`yabot_chats_claim_rejections_total{reason=%q}`, reason))
	c.log.Info("chat owner claim rejected", "chat_id", chatID, "user_id", userID, "reason", reason)
	return ErrBusy
}

func (c *Chats) observeWriteFailure(chatID int64, operation string, err error, permanent bool) {
	outcome := "retry"
	if permanent {
		outcome = "op_error"
	}
	c.count(fmt.Sprintf(`yabot_chats_write_failures_total{outcome=%q}`, outcome))
	if permanent {
		c.log.Error("chat write failed", "chat_id", chatID, "operation", operation,
			"error_type", fmt.Sprintf("%T", err), "sqlite_code", sqliteErrorCode(err))
	} else {
		c.log.Info("chat write will be retried or rejected", "chat_id", chatID, "operation", operation,
			"error_type", fmt.Sprintf("%T", err), "sqlite_code", sqliteErrorCode(err))
	}
}

func sqliteErrorCode(err error) int {
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		return sqliteErr.Code()
	}
	return 0
}

type writeReceiptError struct {
	operation  string
	sqliteCode int
	cause      error
}

func (e writeReceiptError) Error() string {
	return fmt.Sprintf("chats: %s failed (sqlite_code=%d)", e.operation, e.sqliteCode)
}

func (e writeReceiptError) Unwrap() error { return e.cause }

func redactWriteReceiptError(operation string, err error) error {
	redacted := writeReceiptError{operation: operation, sqliteCode: sqliteErrorCode(err)}
	// Keep safe sentinel identities without passing driver text to ingress.
	for _, cause := range []error{ErrBusy, store.ErrBusy, store.ErrClosed, store.ErrFaulted, store.ErrAborted} {
		if errors.Is(err, cause) {
			redacted.cause = cause
			break
		}
	}
	return redacted
}

func (c *Chats) observeStale(chatID int64, event Event) {
	if update, ok := event.(MemberUpdate); ok {
		c.count(`yabot_dispatch_stale_updates_total{route="my_chat_member"}`)
		c.log.Info("stale chat membership update", "chat_id", chatID, "update_id", update.Order.ID)
		return
	}
	c.count(`yabot_chats_events_total{outcome="stale_completion"}`)
	c.log.Info("stale chat completion", "chat_id", chatID, "event", fmt.Sprintf("%T", event))
}

func outcomeLabel(outcome telegram.Outcome, dropped sender.DropReason) string {
	if dropped != 0 {
		return dropped.String()
	}
	if outcome.Reason != 0 {
		return outcome.Reason.String()
	}
	return outcome.Class.String()
}

func (c *Chats) observeMetric(chatID int64, m Metric) {
	var outcome string
	switch m.Kind {
	case ProbeOutcome, LeaveOutcome:
		outcome = outcomeLabel(m.Outcome, m.Dropped)
	case ProbeInFlight:
		outcome = "in_flight"
	case ProbeLimited:
		outcome = "rate_limited"
	case ProbeStaleResult:
		outcome = "stale"
	case InvalidMember:
		outcome = "invalid_member"
	}
	switch m.Kind {
	case ProbeOutcome, ProbeInFlight, ProbeLimited, ProbeStaleResult:
		c.count(fmt.Sprintf(`yabot_chats_probe_total{trigger=%q,outcome=%q}`, m.Trigger, outcome))
	case InvalidMember:
		c.count(`yabot_chats_events_total{outcome="invalid_member"}`)
	case LeaveOutcome:
		c.count(fmt.Sprintf(`yabot_chats_effects_total{effect=%q,outcome=%q}`, m.Kind, outcome))
	}
	if m.Error && (m.Kind != LeaveOutcome || unexpectedOutcome(sender.Result{Outcome: m.Outcome, Dropped: m.Dropped})) {
		c.log.Error("chat operation failed", "chat_id", chatID, "operation", m.Kind, "outcome", outcome)
	} else if m.Kind != ProbeOutcome || outcome != "ok" {
		c.log.Info("chat operation result", "chat_id", chatID, "operation", m.Kind, "outcome", outcome)
	}
}

func (c *Chats) observeDelivery(chatID int64, kind string, result sender.Result) {
	outcome := outcomeLabel(result.Outcome, result.Dropped)
	c.count(fmt.Sprintf(`yabot_chats_effects_total{effect=%q,outcome=%q}`, kind, outcome))
	if unexpectedOutcome(result) {
		c.log.Error("chat message failed", "chat_id", chatID, "operation", kind, "outcome", outcome)
	} else if !probeOK(result) {
		c.log.Info("chat message was not sent", "chat_id", chatID, "operation", kind, "outcome", outcome)
	}
}
