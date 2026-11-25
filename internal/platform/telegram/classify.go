package telegram

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-telegram/bot"
)

type Class uint8

const (
	Unknown Class = iota
	OK
	Done
	Retry
	Forbidden
	Permission
	Migrated
	Conflict
	Unauthorized
	Fatal
)

func (c Class) String() string {
	switch c {
	case OK:
		return "ok"
	case Done:
		return "done"
	case Retry:
		return "retry"
	case Forbidden:
		return "forbidden"
	case Permission:
		return "permission"
	case Migrated:
		return "migrated"
	case Conflict:
		return "conflict"
	case Unauthorized:
		return "unauthorized"
	case Fatal:
		return "fatal"
	default:
		return "unknown"
	}
}

type Reason uint8

const (
	ReasonNone Reason = iota
	AlreadyParticipant
	RequestMissing
	QueryExpired
	ChatGone
	BotBlocked
)

func (r Reason) String() string {
	switch r {
	case ReasonNone:
		return "none"
	case AlreadyParticipant:
		return "already_participant"
	case RequestMissing:
		return "request_missing"
	case QueryExpired:
		return "query_expired"
	case ChatGone:
		return "chat_gone"
	case BotBlocked:
		return "bot_blocked"
	default:
		return "unknown"
	}
}

type Outcome struct {
	Class      Class
	Reason     Reason
	RetryAfter time.Duration
	MigrateTo  int64
}

type Error struct {
	Outcome Outcome
	Err     error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Outcome.Class.String()
}

func (e *Error) Unwrap() error { return e.Err }

func (e *Error) outcome() Outcome { return e.Outcome }

func (*transportError) outcome() Outcome { return Outcome{Class: Retry} }

func Classify(err error) Outcome {
	if err == nil {
		return Outcome{Class: OK}
	}
	classified, _ := errors.AsType[interface {
		error
		outcome() Outcome
	}](err)
	if _, isTransport := classified.(*transportError); classified != nil && !isTransport {
		return classified.outcome()
	}
	var rateLimited *bot.TooManyRequestsError
	if errors.As(err, &rateLimited) {
		return Outcome{Class: Retry, RetryAfter: retryAfterDuration(uint64(max(0, rateLimited.RetryAfter)))}
	}
	var migrated *bot.MigrateError
	if errors.As(err, &migrated) {
		return Outcome{Class: Migrated, MigrateTo: int64(migrated.MigrateToChatID)}
	}

	description, code := apiErrorDetails(err)
	if outcome := classifyDescription(description); outcome.Class != Unknown {
		return outcome
	}
	switch {
	case errors.Is(err, bot.ErrorForbidden):
		return Outcome{Class: Forbidden}
	case errors.Is(err, bot.ErrorConflict):
		return Outcome{Class: Conflict}
	case errors.Is(err, bot.ErrorUnauthorized):
		return Outcome{Class: Unauthorized}
	case code >= 500 && code <= 599:
		return Outcome{Class: Retry}
	case code != 0, errors.Is(err, bot.ErrorBadRequest), errors.Is(err, bot.ErrorNotFound):
		return Outcome{Class: Fatal}
	}
	if classified != nil {
		return classified.outcome()
	}
	if isTransientError(err) {
		return Outcome{Class: Retry}
	}
	return Outcome{Class: Fatal}
}

func retryAfterDuration(seconds uint64) time.Duration {
	if seconds > uint64(math.MaxInt64/time.Second) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(seconds) * time.Second
}

const apiErrorPrefix = "error response from telegram for method "

func apiErrorDetails(err error) (description string, code int) {
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		if joined, ok := cause.(interface{ Unwrap() []error }); ok {
			for _, child := range joined.Unwrap() {
				if description, code := apiErrorDetails(child); description != "" || code != 0 {
					return description, code
				}
			}
			continue
		}
		// rawRequest wraps known codes as "<sentinel>, <description>".
		switch sentinel := errors.Unwrap(cause); sentinel {
		case bot.ErrorBadRequest, bot.ErrorUnauthorized, bot.ErrorForbidden, bot.ErrorNotFound, bot.ErrorConflict:
			if description, ok := strings.CutPrefix(cause.Error(), sentinel.Error()+", "); ok {
				return description, 0
			}
		}

		// Other codes are available only in rawRequest's formatted error.
		rest, ok := strings.CutPrefix(cause.Error(), apiErrorPrefix)
		if !ok {
			continue
		}
		_, status, _ := strings.Cut(rest, ", ")
		codeText, description, _ := strings.Cut(status, " ")
		if code, err := strconv.Atoi(codeText); err == nil {
			return description, code
		}
	}
	return "", 0
}

// Chat access descriptions come from Client.cpp and the TDLib errors it forwards.
func classifyDescription(description string) Outcome {
	description = strings.ToLower(description)
	switch {
	case strings.Contains(description, "hide_requester_missing"):
		return Outcome{Class: Done, Reason: RequestMissing}
	case strings.Contains(description, "user_already_participant"):
		return Outcome{Class: Done, Reason: AlreadyParticipant}
	// Upstream Client.cpp rewrites QUERY_ID_INVALID to "query is too old and response timeout expired or query ID is invalid",
	// which supports the code and both text fragments, while the other matches are candidates.
	case containsAny(description, "query_id_invalid", "query is too old", "query id is invalid", "query has expired",
		"join_request_query_invalid", "join_request_query_expired", "join_request_query_id_expired"):
		return Outcome{Class: Done, Reason: QueryExpired}
	case containsAny(description, "chat_admin_required", "not enough rights", "need administrator rights", "have no rights"):
		return Outcome{Class: Permission}
	case containsAny(description, "chat not found", "channel_private", "bot was kicked", "bot is not a member",
		": chat info not found", ": group not found", ": basic group not found", ": supergroup not found",
		": channel not found", "group chat was deleted",
		"chat is deactivated", "chat is already deactivated"):
		return Outcome{Class: Done, Reason: ChatGone}
	case strings.Contains(description, "bot was blocked by the user"):
		return Outcome{Class: Forbidden, Reason: BotBlocked}
	case strings.Contains(description, "user is deactivated"):
		return Outcome{Class: Forbidden}
	default:
		return Outcome{}
	}
}

func containsAny(text string, parts ...string) bool {
	for _, part := range parts {
		if strings.Contains(text, part) {
			return true
		}
	}
	return false
}

func isTransientError(err error) bool {
	var networkError net.Error
	// Shutdown can cancel an in-flight request without a business failure.
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.As(err, &networkError) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE)
}
