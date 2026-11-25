package telegram

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

func TestGetUpdatesRequestAndDecode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		allowed []string
		timeout time.Duration
		want    string
	}{
		{"explicit", []string{"chat_join_request", "my_chat_member"}, 25*time.Second + time.Millisecond, `{"offset":6,"limit":100,"timeout":25,"allowed_updates":["chat_join_request","my_chat_member"]}`},
		{"nil", nil, -2 * time.Second, `{"offset":6,"limit":100,"timeout":0}`},
		{"empty", []string{}, 500 * time.Millisecond, `{"offset":6,"limit":100,"timeout":0,"allowed_updates":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/bot"+testToken+"/getUpdates" || r.Header.Get("Content-Type") != "application/json" {
					t.Error("expected a JSON POST to the getUpdates API path")
				}
				var got, want map[string]any
				if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
					t.Error(err)
				}
				if err := json.Unmarshal([]byte(tc.want), &want); err != nil {
					t.Error(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("request = %v, want %v", got, want)
				}
				writeTestResponse(t, w, `{"ok":true,"result":[
					{"update_id":6,"chat_join_request":{"query_id":"query-42","chat":{"id":-1002345},"from":{"id":123},"date":1760000000}},
					{"update_id":7,"my_chat_member":{"chat":{"id":-1005678},"from":{"id":123},"date":1760000001,
					  "old_chat_member":{"status":"left","user":{"id":123456}},"new_chat_member":{"status":"member","user":{"id":123456}}}}
				]}`)
			}, nil)

			updates, err := client.GetUpdates(t.Context(), 6, 100, tc.timeout, tc.allowed)
			if err != nil || len(updates) != 2 {
				t.Fatalf("GetUpdates = (%+v, %v)", updates, err)
			}
			join := updates[0].ChatJoinRequest
			if updates[0].ID != 6 || join == nil || join.QueryID != "query-42" || join.Chat.ID != -1002345 || join.From.ID != 123 || join.Date != 1760000000 {
				t.Errorf("unexpected join update: %+v, join=%+v", updates[0], join)
			}
			member := updates[1].MyChatMember
			if updates[1].ID != 7 || member == nil || member.Chat.ID != -1005678 || member.Date != 1760000001 || member.NewChatMember.Type != models.ChatMemberTypeMember {
				t.Errorf("unexpected member update: %+v, member=%+v", updates[1], member)
			}
		})
	}
}

func TestGetUpdatesRejectsInvalidLimit(t *testing.T) {
	transport := testResponseTransport(t, func(http.ResponseWriter, *http.Request) {
		t.Error("invalid limit reached the Bot API")
	})
	client := newMemoryTestClient(t, transport)
	for _, limit := range []int{0, 101} {
		if _, err := client.GetUpdates(t.Context(), 0, limit, 0, nil); err == nil || !strings.Contains(err.Error(), "limit") || strings.Contains(err.Error(), testToken) {
			t.Errorf("limit %d: got %v, want a safe limit error", limit, err)
		}
	}
}

func TestGetUpdatesErrorParity(t *testing.T) {
	for _, tc := range []struct {
		code     int
		fields   string
		want     Outcome
		sentinel error
	}{
		{400, `"description":"HIDE_REQUESTER_MISSING"`, Outcome{Class: Done, Reason: RequestMissing}, bot.ErrorBadRequest},
		{400, `"description":"group migrated","parameters":{"migrate_to_chat_id":-1001234567890}`, Outcome{Class: Migrated, MigrateTo: -1001234567890}, nil},
		{401, `"description":"Unauthorized"`, Outcome{Class: Unauthorized}, bot.ErrorUnauthorized},
		{403, `"description":"generic forbidden"`, Outcome{Class: Forbidden}, bot.ErrorForbidden},
		{404, `"description":"unknown resource"`, Outcome{Class: Fatal}, bot.ErrorNotFound},
		{409, `"description":"terminated by another getUpdates request"`, Outcome{Class: Conflict}, bot.ErrorConflict},
		{429, `"description":"Too Many Requests","parameters":{"retry_after":12}`, Outcome{Class: Retry, RetryAfter: 12 * time.Second}, nil},
		{500, `"description":"Internal Server Error"`, Outcome{Class: Retry}, nil},
		{500, `"description":"CHAT_ADMIN_REQUIRED"`, Outcome{Class: Permission}, nil},
		{502, `"description":"Bad Gateway"`, Outcome{Class: Retry}, nil},
	} {
		t.Run(fmt.Sprintf("%d_%s", tc.code, tc.want.Class), func(t *testing.T) {
			body := fmt.Sprintf(`{"ok":false,"error_code":%d,%s}`, tc.code, tc.fields)
			err, upstream := getUpdatesErrors(t, tc.code, body)
			if reflect.TypeOf(err) != reflect.TypeOf(upstream) ||
				err.Error() != strings.Replace(upstream.Error(), "for method getMe,", "for method getUpdates,", 1) {
				t.Errorf("GetUpdates: %T %v, library: %T %v", err, err, upstream, upstream)
			}
			if got := Classify(upstream); got != tc.want {
				t.Errorf("library error %v classifies as %+v, want %+v", upstream, got, tc.want)
			}
			if got := Classify(err); got != tc.want || (tc.sentinel != nil && !errors.Is(err, tc.sentinel)) {
				t.Errorf("error %v classifies as %+v, want %+v and sentinel %v", err, got, tc.want, tc.sentinel)
			}
		})
	}
}

func TestGetUpdatesNonJSONGatewayErrors(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusBadGateway} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			err, upstream := getUpdatesErrors(t, status, "upstream unavailable")
			wantLocal := fmt.Sprintf("error response from telegram for method getUpdates, %d %s", status, http.StatusText(status))
			if err.Error() != wantLocal || errors.Unwrap(err) != nil || Classify(err).Class != Retry {
				t.Errorf("GetUpdates error = %T %v, want a retryable status error %q", err, err, wantLocal)
			}
			wantUpstream := "error decode response body for method getMe, upstream unavailable, invalid character 'u' looking for beginning of value"
			if _, ok := errors.AsType[*json.SyntaxError](upstream); !ok || upstream.Error() != wantUpstream ||
				reflect.TypeOf(err) == reflect.TypeOf(upstream) || Classify(upstream).Class != Fatal {
				t.Errorf("native error = %T %v, want a distinct JSON syntax error %q", upstream, upstream, wantUpstream)
			}
		})
	}
}

func getUpdatesErrors(t *testing.T, status int, body string) (raw, native error) {
	t.Helper()
	var requests []string
	transport := testResponseTransport(t, func(w http.ResponseWriter, req *http.Request) {
		requests = append(requests, req.URL.Path)
		w.WriteHeader(status)
		writeTestResponse(t, w, body)
	})
	client := newMemoryTestClient(t, transport)
	upstream := newUpstreamTestBot(t, transport)
	_, raw = client.GetUpdates(t.Context(), 0, 100, 0, nil)
	// The pinned library's polling method is private.
	_, native = upstream.GetMe(t.Context())
	if want := []string{"/bot" + testToken + "/getUpdates", "/bot" + testToken + "/getMe"}; !slices.Equal(requests, want) {
		t.Fatalf("transport received %d requests, want getUpdates followed by native getMe", len(requests))
	}
	if raw == nil || native == nil || strings.Contains(raw.Error(), testToken) {
		t.Fatalf("GetUpdates error=%v, library error=%v", raw, native)
	}
	return raw, native
}

func TestGetUpdatesResponseFailure(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		oversize   bool
		want       Class
	}{
		{"echoed token", fmt.Sprintf(`{"ok":false,"error_code":403,"description":"request rejected: %s"}`, testToken), 403, false, Forbidden},
		{"encoded token", fmt.Sprintf(`{"ok":false,"error_code":403,"description":"request rejected: %s"}`, url.QueryEscape(testToken)), 403, false, Forbidden},
		{"non-JSON 502", "upstream timed out: " + testToken, 502, false, Retry},
		{"oversize", "", 200, true, Fatal},
		{"oversize 502", "", 502, true, Retry},
		{"null result", `{"ok":true,"result":null}`, 200, false, Fatal},
		{"object result", `{"ok":true,"result":{}}`, 200, false, Fatal},
		{"no update ID", `{"ok":true,"result":[{"chat_join_request":"bad"}]}`, 200, false, Fatal},
		{"invalid update ID", `{"ok":true,"result":[{"update_id":"bad","chat_join_request":"bad"}]}`, 200, false, Fatal},
		{"too many entries", `{"ok":true,"result":[{"update_id":1},{"update_id":2}]}`, 200, false, Fatal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := testResponseTransport(t, func(w http.ResponseWriter, _ *http.Request) {
				if tc.oversize {
					w.Header().Set("Content-Length", "67108865")
				}
				w.WriteHeader(tc.status)
				writeTestResponse(t, w, tc.body)
			})
			client := newMemoryTestClient(t, transport)
			updates, err := client.GetUpdates(t.Context(), 0, 1, 0, nil)
			if updates != nil || err == nil || strings.Contains(err.Error(), testToken) || strings.Contains(err.Error(), url.QueryEscape(testToken)) || Classify(err).Class != tc.want {
				t.Fatalf("GetUpdates = (%+v, %v), want safe %s error", updates, err, tc.want)
			}
			if tc.oversize && !strings.Contains(err.Error(), "exceeds 64 MiB") {
				t.Errorf("size error = %v", err)
			}
		})
	}
}

func TestGetUpdatesFallsBackToHTTPStatus(t *testing.T) {
	for _, tc := range []struct {
		status   int
		body     string
		want     Outcome
		sentinel error
	}{
		{400, "invalid request", Outcome{Class: Fatal}, bot.ErrorBadRequest},
		{401, "invalid credentials: " + testToken, Outcome{Class: Unauthorized}, bot.ErrorUnauthorized},
		{403, "proxy denied access: chat not found", Outcome{Class: Forbidden}, bot.ErrorForbidden},
		{404, "route not found", Outcome{Class: Fatal}, bot.ErrorNotFound},
		{409, "upstream conflict", Outcome{Class: Conflict}, bot.ErrorConflict},
		{429, "rate limit exceeded", Outcome{Class: Retry, RetryAfter: time.Second}, nil},
		{401, `{"ok":false,"error_code":401,"description":123}`, Outcome{Class: Unauthorized}, bot.ErrorUnauthorized},
		{409, `{"error":"another worker owns this route"}`, Outcome{Class: Conflict}, bot.ErrorConflict},
		{403, `{"ok":true,"result":[]}`, Outcome{Class: Forbidden}, bot.ErrorForbidden},
		{403, `{"ok":true,"error_code":403,"result":[]}`, Outcome{Class: Forbidden}, bot.ErrorForbidden},
	} {
		t.Run(fmt.Sprintf("%d_%s", tc.status, tc.want.Class), func(t *testing.T) {
			transport := testResponseTransport(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				writeTestResponse(t, w, tc.body)
			})
			client := newMemoryTestClient(t, transport)
			updates, err := client.GetUpdates(t.Context(), 0, 1, 0, nil)
			if updates != nil || err == nil || strings.Contains(err.Error(), testToken) {
				t.Fatal("expected a safe HTTP status error")
			}
			if got := Classify(err); got != tc.want {
				t.Errorf("Classify(%v) = %+v, want %+v", err, got, tc.want)
			}
			if tc.sentinel != nil && !errors.Is(err, tc.sentinel) {
				t.Errorf("error = %v, want sentinel %v", err, tc.sentinel)
			}
			if tc.status == http.StatusTooManyRequests {
				var rateLimited *bot.TooManyRequestsError
				if !errors.As(err, &rateLimited) || rateLimited.RetryAfter != 1 {
					t.Errorf("error = %T %v, want TooManyRequestsError with a one-second delay", err, err)
				}
			}
		})
	}
}

func TestGetUpdatesPreservesHTTPStatusWhenBodyIsTruncated(t *testing.T) {
	for _, tc := range []struct {
		status   int
		want     Outcome
		sentinel error
	}{
		{http.StatusOK, Outcome{Class: Retry}, nil},
		{http.StatusFound, Outcome{Class: Fatal}, nil},
		{http.StatusBadRequest, Outcome{Class: Fatal}, bot.ErrorBadRequest},
		{http.StatusUnauthorized, Outcome{Class: Unauthorized}, bot.ErrorUnauthorized},
		{http.StatusForbidden, Outcome{Class: Forbidden}, bot.ErrorForbidden},
		{http.StatusNotFound, Outcome{Class: Fatal}, bot.ErrorNotFound},
		{http.StatusConflict, Outcome{Class: Conflict}, bot.ErrorConflict},
		{http.StatusTeapot, Outcome{Class: Fatal}, nil},
		{http.StatusTooManyRequests, Outcome{Class: Retry, RetryAfter: time.Second}, nil},
		{http.StatusBadGateway, Outcome{Class: Retry}, nil},
	} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			client, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Length", "32")
				w.Header().Set("Connection", "close")
				w.WriteHeader(tc.status)
				writeTestResponse(t, w, `{"ok":`)
			}, nil)

			_, err := client.GetUpdates(t.Context(), 0, 1, 0, nil)
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("GetUpdates error = %v, want unexpected EOF", err)
			}
			if tc.sentinel != nil && !errors.Is(err, tc.sentinel) {
				t.Errorf("GetUpdates error = %v, want sentinel %v", err, tc.sentinel)
			}
			if got := Classify(err); got != tc.want {
				t.Errorf("Classify(%v) = %+v, want %+v", err, got, tc.want)
			}
		})
	}
}

func TestRateLimitResponse(t *testing.T) {
	const maxDuration = time.Duration(math.MaxInt64)
	for _, call := range []struct {
		name string
		do   func(context.Context, *Client) error
	}{
		{"GetUpdates", func(ctx context.Context, c *Client) error {
			_, err := c.GetUpdates(ctx, 0, 1, 0, nil)
			return err
		}},
		{"GetMe", func(ctx context.Context, c *Client) error {
			_, err := c.GetMe(ctx)
			return err
		}},
		{"Bot.GetMe", func(ctx context.Context, c *Client) error {
			_, err := c.Bot.GetMe(ctx)
			return err
		}},
	} {
		for _, tc := range []struct {
			name, body, header string
			truncated          bool
			want               time.Duration
		}{
			{"malformed JSON", `{"ok":`, "37", false, 37 * time.Second},
			{"truncated body", `{"ok":`, "37", true, 37 * time.Second},
			{"Telegram retry_after", `{"ok":false,"error_code":429,"parameters":{"retry_after":12}}`, "37", false, 12 * time.Second},
			{"missing retry_after", `{"ok":false,"error_code":429}`, "37", false, 37 * time.Second},
			{"zero retry_after", `{"ok":false,"error_code":429,"parameters":{"retry_after":0}}`, "37", false, 37 * time.Second},
			{"negative retry_after", `{"ok":false,"error_code":429,"parameters":{"retry_after":-5}}`, "37", false, 37 * time.Second},
			{"default", `{"ok":false,"error_code":429}`, "", false, time.Second},
			{"zero header", `{"ok":false,"error_code":429}`, "0", false, 0},
			{"largest whole seconds", `{"ok":false,"error_code":429,"parameters":{"retry_after":9223372036}}`, "37", false, 9223372036 * time.Second},
			{"first overflowing body", `{"ok":false,"error_code":429,"parameters":{"retry_after":9223372037}}`, "37", false, maxDuration},
			{"huge body", `{"ok":false,"error_code":429,"parameters":{"retry_after":18446744073709551616}}`, "37", false, maxDuration},
			{"first overflowing header", `{"ok":false,"error_code":429}`, "9223372037", false, maxDuration},
			{"huge header", `{"ok":false,"error_code":429}`, "18446744073709551616", false, maxDuration},
		} {
			t.Run(call.name+"/"+tc.name, func(t *testing.T) {
				handler := func(w http.ResponseWriter, _ *http.Request) {
					if tc.header != "" {
						w.Header().Set("Retry-After", tc.header)
					}
					if tc.truncated {
						w.Header().Set("Content-Length", "32")
						w.Header().Set("Connection", "close")
					}
					w.WriteHeader(http.StatusTooManyRequests)
					writeTestResponse(t, w, tc.body)
				}
				var client *Client
				if tc.truncated {
					client, _ = newTestClient(t, handler, nil)
				} else {
					client = newMemoryTestClient(t, testResponseTransport(t, handler))
				}

				err := call.do(t.Context(), client)
				var rateLimited *bot.TooManyRequestsError
				if !errors.As(err, &rateLimited) {
					t.Errorf("error = %T %v, want TooManyRequestsError", err, err)
				} else if seconds := tc.want / time.Second; tc.want != maxDuration && seconds <= math.MaxInt &&
					rateLimited.RetryAfter != int(seconds) {
					t.Errorf("RetryAfter = %d seconds, want %d", rateLimited.RetryAfter, seconds)
				}
				if tc.truncated && !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Errorf("error = %v, want unexpected EOF", err)
				}
				want := Outcome{Class: Retry, RetryAfter: tc.want}
				if got := Classify(err); got != want {
					t.Errorf("Classify(%v) = %+v, want %+v", err, got, want)
				}
			})
		}
	}
}

func TestRetryAfterSeconds(t *testing.T) {
	now := time.Date(2026, time.November, 25, 12, 0, 0, 250_000_000, time.UTC)
	for _, tc := range []struct {
		name, header string
		want         uint64
	}{
		{"seconds", "37", 37},
		{"zero", "0", 0},
		{"negative", "-5", 1},
		{"fractional", "0.25", 1},
		{"future date", "Wed, 25 Nov 2026 12:00:37 GMT", 37},
		{"past date", "Wed, 25 Nov 2026 12:00:00 GMT", 0},
		{"invalid date", "Wed, 32 Nov 2026 12:00:37 GMT", 1},
		{"missing", "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryAfterSeconds(tc.header, now); got != tc.want {
				t.Errorf("Retry-After %q gave %d seconds, want %d", tc.header, got, tc.want)
			}
		})
	}
}

func TestGetUpdatesTimeout(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	client, _ := newTestClient(t, func(_ http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Error(err)
		}
		close(entered)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}, nil)
	t.Cleanup(func() { close(release) })

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	_, err := client.GetUpdates(ctx, 0, 100, 25*time.Second, nil)
	select {
	case <-entered:
	default:
		t.Fatal("timeout occurred before the request reached the test server")
	}
	if !errors.Is(err, context.DeadlineExceeded) || Classify(err).Class != Retry || strings.Contains(err.Error(), testToken) {
		t.Fatalf("GetUpdates error = %v, want a safe retryable deadline error", err)
	}
	var urlErr *url.Error
	if !errors.As(err, &urlErr) || strings.Contains(urlErr.URL, testToken) || !strings.Contains(urlErr.URL, "***") {
		t.Errorf("expected a URL error with a redacted URL, got %v", urlErr)
	}
}

func TestGetUpdatesKeepsIDsOfMalformedEntries(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	client, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		writeTestResponse(t, w, fmt.Sprintf(`{"ok":true,"result":[
			{"update_id":100,"chat_join_request":{"query_id":"good-before"}},
			{"chat_join_request":"private-payload %s","update_id":101},
			{"update_id":102,"chat_join_request":{"query_id":"good-after"}}
		]}`, testToken))
	}, log)
	updates, err := client.GetUpdates(t.Context(), 0, 100, 0, nil)
	if err != nil || len(updates) != 3 {
		t.Fatalf("updates = %+v, error = %v", updates, err)
	}
	if !reflect.DeepEqual(updates[1], models.Update{ID: 101}) ||
		updates[0].ChatJoinRequest == nil || updates[0].ChatJoinRequest.QueryID != "good-before" ||
		updates[2].ChatJoinRequest == nil || updates[2].ChatJoinRequest.QueryID != "good-after" {
		t.Errorf("unexpected batch: %+v", updates)
	}
	var record map[string]any
	if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
		t.Fatalf("expected exactly one error record: %v", err)
	}
	if record["level"] != "ERROR" || record["update_id"] != float64(101) || record["error"] == nil ||
		strings.Contains(logs.String(), testToken) || strings.Contains(logs.String(), "private-payload") {
		t.Errorf("unexpected log record: %s", logs.String())
	}
}

func TestGetMeReadsRawCapability(t *testing.T) {
	for _, tc := range []struct {
		name, field string
		supported   bool
	}{
		{"enabled", `,"supports_join_request_queries":true`, true},
		{"disabled", `,"supports_join_request_queries":false`, false},
		{"missing", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := `{"id":123456, "username":"guard_bot", "unknown_field":42` + tc.field + `}`
			client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/bot"+testToken+"/getMe" || r.Header.Get("Content-Type") != "application/json" {
					t.Error("expected a JSON POST to the getMe API path")
				}
				writeTestResponse(t, w, `{"ok":true,"result":`+raw+`}`)
			}, nil)
			me, err := client.GetMe(t.Context())
			if err != nil || me.ID != 123456 || me.Username != "guard_bot" || me.SupportsJoinRequestQueries != tc.supported || string(me.Raw) != raw {
				t.Fatalf("GetMe = (%+v, %v), want capability %t and original result", me, err, tc.supported)
			}
		})
	}
}

func TestGetMeRejectsInvalidResult(t *testing.T) {
	for _, tc := range []struct {
		name, result string
	}{
		{"null", `null`},
		{"array", `[1]`},
		{"string", `"123456"`},
		{"zero id", `{"id":0,"username":"guard_bot"}`},
		{"missing id", `{"username":"guard_bot"}`},
		{"null id", `{"id":null,"username":"guard_bot"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := testResponseTransport(t, func(w http.ResponseWriter, _ *http.Request) {
				writeTestResponse(t, w, `{"ok":true,"result":`+tc.result+`}`)
			})
			client := newMemoryTestClient(t, transport)
			me, err := client.GetMe(t.Context())
			if !reflect.DeepEqual(me, Me{}) || Classify(err).Class != Fatal {
				t.Fatalf("GetMe = (%+v, %v), want zero Me and a fatal error", me, err)
			}
		})
	}
}

func TestGetMeContextErrors(t *testing.T) {
	for _, tc := range []struct {
		name        string
		newContext  func(context.Context) (context.Context, context.CancelFunc)
		want        error
		wantRequest bool
	}{
		{"already canceled", func(parent context.Context) (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(parent)
			cancel()
			return ctx, cancel
		}, context.Canceled, false},
		{"deadline while awaiting response", func(parent context.Context) (context.Context, context.CancelFunc) {
			return context.WithTimeout(parent, 10*time.Millisecond)
		}, context.DeadlineExceeded, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, err := New(Config{Token: testToken, ServerURL: "http://bot.test", Skew: NewTimeSkew(nil, nil)})
				if err != nil {
					t.Fatal(err)
				}
				clientConn, serverConn := net.Pipe()
				defer func() {
					if err := errors.Join(clientConn.Close(), serverConn.Close()); err != nil {
						t.Error(err)
					}
				}()
				transport := client.http.Transport.(*skewTransport)
				transport.Proxy = nil
				transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
					return clientConn, nil
				}

				ctx, cancel := tc.newContext(t.Context())
				defer cancel()
				entered := make(chan struct{})
				go func() {
					req, err := http.ReadRequest(bufio.NewReader(serverConn))
					if err != nil {
						if tc.wantRequest {
							t.Errorf("read getMe request: %v", err)
						}
						return
					}
					if err := req.Body.Close(); err != nil {
						t.Errorf("close getMe request body: %v", err)
					}
					if err := ctx.Err(); err != nil {
						t.Errorf("getMe context ended before the server began waiting: %v", err)
					}
					close(entered)
					<-ctx.Done()
				}()

				_, err = client.GetMe(ctx)
				if class := Classify(err).Class; !errors.Is(err, tc.want) || class != Retry {
					t.Errorf("GetMe error = %v (class %s), want a retryable %v error", err, class, tc.want)
				}
				if tc.wantRequest {
					select {
					case <-entered:
					default:
						t.Error("getMe request did not reach the server before the deadline")
					}
				}
			})
		})
	}
}
