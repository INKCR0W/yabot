package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"syscall"
	"testing"
	"time"

	"github.com/go-telegram/bot"
)

func TestClassifyTelegramResponses(t *testing.T) {
	cases := []struct {
		name        string
		status      int
		description string
		parameters  map[string]int64
		want        Outcome
	}{
		{"rate limit", 429, "Too Many Requests: retry after 12", map[string]int64{"retry_after": 12}, Outcome{Class: Retry, RetryAfter: 12 * time.Second}},
		{"migration", 400, "Bad Request: group chat was upgraded to a supergroup chat", map[string]int64{"migrate_to_chat_id": -1001234567890}, Outcome{Class: Migrated, MigrateTo: -1001234567890}},
		{"migration before description", 400, "Bad Request: chat is deactivated", map[string]int64{"migrate_to_chat_id": -1001234567890}, Outcome{Class: Migrated, MigrateTo: -1001234567890}},
		{"deleted group", 403, "Forbidden: the group chat was deleted", nil, Outcome{Class: Done, Reason: ChatGone}},
		{"deactivated chat", 400, "Bad Request: chat is deactivated", nil, Outcome{Class: Done, Reason: ChatGone}},
		{"already deactivated chat", 400, "Bad Request: chat is already deactivated", nil, Outcome{Class: Done, Reason: ChatGone}},
		{"kicked from group", 403, "Forbidden: bot was kicked from the group chat", nil, Outcome{Class: Done, Reason: ChatGone}},
		{"kicked from supergroup", 403, "Forbidden: bot was kicked from the supergroup chat", nil, Outcome{Class: Done, Reason: ChatGone}},
		{"kicked from channel", 403, "Forbidden: bot was kicked from the channel chat", nil, Outcome{Class: Done, Reason: ChatGone}},
		{"not a group member", 403, "Forbidden: bot is not a member of the group chat", nil, Outcome{Class: Done, Reason: ChatGone}},
		{"not a supergroup member", 403, "Forbidden: bot is not a member of the supergroup chat", nil, Outcome{Class: Done, Reason: ChatGone}},
		{"not a channel member", 403, "Forbidden: bot is not a member of the channel chat", nil, Outcome{Class: Done, Reason: ChatGone}},
		{"blocked", 403, "Forbidden: bot was blocked by the user", nil, Outcome{Class: Forbidden, Reason: BotBlocked}},
		{"deactivated user", 403, "Forbidden: user is deactivated", nil, Outcome{Class: Forbidden}},
		{"other forbidden", 403, "Forbidden: operation is forbidden", nil, Outcome{Class: Forbidden}},
		{"request missing", 400, "Bad Request: HIDE_REQUESTER_MISSING", nil, Outcome{Class: Done, Reason: RequestMissing}},
		{"already participant", 400, "Bad Request: USER_ALREADY_PARTICIPANT", nil, Outcome{Class: Done, Reason: AlreadyParticipant}},
		{"invalid query code", 400, "Bad Request: QUERY_ID_INVALID", nil, Outcome{Class: Done, Reason: QueryExpired}},
		{"old query", 400, "Bad Request: query is too old", nil, Outcome{Class: Done, Reason: QueryExpired}},
		{"invalid query text", 400, "Bad Request: query ID is invalid", nil, Outcome{Class: Done, Reason: QueryExpired}},
		{"expired join request query", 400, "Bad Request: JOIN_REQUEST_QUERY_EXPIRED", nil, Outcome{Class: Done, Reason: QueryExpired}},
		{"administrator required", 400, "Bad Request: CHAT_ADMIN_REQUIRED", nil, Outcome{Class: Permission}},
		{"insufficient join request rights", 400, "Bad Request: not enough rights to manage chat join requests", nil, Outcome{Class: Permission}},
		{"insufficient custom title rights", 400, "Bad Request: not enough rights to change custom title of the user", nil, Outcome{Class: Permission}},
		{"mixed-case permission", 400, "Bad Request: Not EnOuGh RiGhTs to manage chat", nil, Outcome{Class: Permission}},
		{"need administrator rights", 400, "Bad Request: need administrator rights in the channel chat", nil, Outcome{Class: Permission}},
		{"no rights", 400, "Bad Request: you have no rights to manage the chat", nil, Outcome{Class: Permission}},
		{"target is not an administrator", 400, "Bad Request: user is not an administrator", nil, Outcome{Class: Fatal}},
		{"target is an administrator", 400, "Bad Request: user is an administrator of the chat", nil, Outcome{Class: Fatal}},
		{"ambiguous write access", 400, "Bad Request: CHAT_WRITE_FORBIDDEN", nil, Outcome{Class: Fatal}},
		{"chat not found", 400, "Bad Request: chat not found", nil, Outcome{Class: Done, Reason: ChatGone}},
		{"private chat not found", 400, "Bad Request: private chat not found", nil, Outcome{Class: Done, Reason: ChatGone}},
		{"chat info not found", 400, "Bad Request: chat info not found", nil, Outcome{Class: Done, Reason: ChatGone}},
		{"group not found", 400, "Bad Request: group not found", nil, Outcome{Class: Done, Reason: ChatGone}},
		{"basic group not found", 400, "Bad Request: basic group not found", nil, Outcome{Class: Done, Reason: ChatGone}},
		{"supergroup not found", 400, "Bad Request: supergroup not found", nil, Outcome{Class: Done, Reason: ChatGone}},
		{"channel not found", 400, "Bad Request: channel not found", nil, Outcome{Class: Done, Reason: ChatGone}},
		{"discussion chat info not found", 400, "Bad Request: discussion chat info not found", nil, Outcome{Class: Fatal}},
		{"message thread not found", 400, "Bad Request: message thread not found", nil, Outcome{Class: Fatal}},
		{"private channel", 400, "Bad Request: CHANNEL_PRIVATE", nil, Outcome{Class: Done, Reason: ChatGone}},
		{"other bad request", 400, "Bad Request: malformed chat ID", nil, Outcome{Class: Fatal}},
		{"method not found", 404, "Not Found", nil, Outcome{Class: Fatal}},
		{"unauthorized", 401, "Unauthorized", nil, Outcome{Class: Unauthorized}},
		{"conflict", 409, "Conflict: terminated by other getUpdates request", nil, Outcome{Class: Conflict}},
		{"internal server error", 500, "Internal Server Error", nil, Outcome{Class: Retry}},
		{"bad gateway", 502, "Bad Gateway", nil, Outcome{Class: Retry}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := botResponseError(t, tc.status, tc.description, tc.parameters)
			if got := Classify(err); got != tc.want {
				t.Errorf("Classify(%v) = %+v, want %+v", err, got, tc.want)
			}
			wrapped := fmt.Errorf("approve chat join request: %w", err)
			if got := Classify(wrapped); got != tc.want {
				t.Errorf("Classify(%v) = %+v, want %+v", wrapped, got, tc.want)
			}
			joined := errors.Join(wrapped, errors.New("chat not found in local cache"))
			if got := Classify(joined); got != tc.want {
				t.Errorf("Classify(%v) = %+v, want %+v", joined, got, tc.want)
			}
		})
	}
}

func TestClassifyDistinguishesChatAndRequestErrors(t *testing.T) {
	cases := []struct {
		description string
		want        Outcome
	}{
		{"Forbidden: CHAT_ADMIN_REQUIRED to handle the join request", Outcome{Class: Permission}},
		{"Forbidden: HIDE_REQUESTER_MISSING in the chat", Outcome{Class: Done, Reason: RequestMissing}},
		{"Forbidden: chat not found when handling the request", Outcome{Class: Done, Reason: ChatGone}},
	}
	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			err := botResponseError(t, http.StatusForbidden, tc.description, nil)
			// The caller's context is separate from the Telegram description.
			err = fmt.Errorf("checking whether the bot was blocked by the user: %w", err)
			if got := Classify(err); got != tc.want {
				t.Errorf("Classify(%v) = %+v, want %+v", err, got, tc.want)
			}
		})
	}
}

func TestClassifyNonResponseErrors(t *testing.T) {
	classified := Outcome{Class: Done, Reason: ChatGone, RetryAfter: 2 * time.Second, MigrateTo: -1234}
	cases := []struct {
		name string
		err  error
		want Outcome
	}{
		{"nil", nil, Outcome{Class: OK}},
		{"classified", &Error{Outcome: classified, Err: context.Canceled}, classified},
		{"wrapped classified", fmt.Errorf("leave chat: %w", &Error{Outcome: classified, Err: context.Canceled}), classified},
		{"classified unknown", &Error{Err: context.Canceled}, Outcome{}},
		{"network error", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, Outcome{Class: Retry}},
		{"URL error", &url.Error{Op: "Post", URL: "http://example.test", Err: errors.New("connection failed")}, Outcome{Class: Retry}},
		{"unexpected EOF", fmt.Errorf("read response: %w", io.ErrUnexpectedEOF), Outcome{Class: Retry}},
		{"connection reset", syscall.ECONNRESET, Outcome{Class: Retry}},
		{"deadline", context.DeadlineExceeded, Outcome{Class: Retry}},
		{"wrapped deadline", fmt.Errorf("request: %w", context.DeadlineExceeded), Outcome{Class: Retry}},
		{"canceled", context.Canceled, Outcome{Class: Retry}},
		{"unknown", errors.New("ordinary error"), Outcome{Class: Fatal}},
		{"code outside 5xx", errors.New("error response from telegram for method getMe, 5000 unexpected"), Outcome{Class: Fatal}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.err); got != tc.want {
				t.Errorf("Classify(%v) = %+v, want %+v", tc.err, got, tc.want)
			}
		})
	}
}

func TestOutcomeZeroValueAndErrorCause(t *testing.T) {
	var outcome Outcome
	if outcome.Class != Unknown || outcome.Class == OK || outcome.Reason != ReasonNone {
		t.Errorf("zero Outcome = %+v, want Unknown with no reason", outcome)
	}
	cause := errors.New("telegram request failed")
	err := &Error{Outcome: Outcome{Class: Fatal}, Err: cause}
	if err.Error() != cause.Error() || !errors.Is(err, cause) {
		t.Errorf("Error = %q, cause = %v, want %q and original cause", err, errors.Unwrap(err), cause)
	}
}

func botResponseError(t *testing.T, status int, description string, parameters map[string]int64) error {
	t.Helper()
	transport := testResponseTransport(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bot"+testToken+"/approveChatJoinRequest" {
			t.Fatal("expected an approveChatJoinRequest call")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if err := json.NewEncoder(w).Encode(map[string]any{
			"ok":          false,
			"error_code":  status,
			"description": description,
			"parameters":  parameters,
		}); err != nil {
			t.Error(err)
		}
	})
	upstream := newUpstreamTestBot(t, transport)
	_, err := upstream.ApproveChatJoinRequest(t.Context(), &bot.ApproveChatJoinRequestParams{
		ChatID: -123, UserID: 456,
	})
	if err == nil {
		t.Fatal("ApproveChatJoinRequest succeeded for a Telegram error response")
	}
	return err
}
