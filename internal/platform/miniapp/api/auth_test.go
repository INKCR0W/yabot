package api

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/miniapp/initdata"
)

func signInitData(t *testing.T, values url.Values) string {
	t.Helper()
	var lines []string
	for _, key := range slices.Sorted(maps.Keys(values)) {
		lines = append(lines, key+"="+values[key][0])
	}
	secret := hmac.New(sha256.New, []byte("WebAppData"))
	if _, err := io.WriteString(secret, testBotToken); err != nil {
		t.Fatal(err)
	}
	signature := hmac.New(sha256.New, secret.Sum(nil))
	if _, err := io.WriteString(signature, strings.Join(lines, "\n")); err != nil {
		t.Fatal(err)
	}
	return values.Encode() + "&hash=" + hex.EncodeToString(signature.Sum(nil))
}

func initDataValues(t *testing.T, userID int64, lang string, authDate time.Time) url.Values {
	t.Helper()
	user, err := json.Marshal(struct {
		ID   int64  `json:"id"`
		Lang string `json:"language_code"`
	}{userID, lang})
	if err != nil {
		t.Fatal(err)
	}
	return url.Values{
		"auth_date": {strconv.FormatInt(authDate.Unix(), 10)},
		"user":      {string(user)},
	}
}

func validAuthorization(t *testing.T, userID int64, lang string) string {
	t.Helper()
	return "tma " + signInitData(t, initDataValues(t, userID, lang, testNow()))
}

func TestAuthenticationPassesVerifiedIdentity(t *testing.T) {
	a, mux, _, logs := newTestAPI(t)
	const userID = int64(4_000_123_456_789)
	values := initDataValues(t, userID, "zh-Hans", testNow())
	values.Set("chat", `{"id":-100123}`)
	values.Set("chat_join_request_query_id", "query+42")
	values.Set("start_param", "entry")
	raw := signInitData(t, values)
	want := initdata.Identity{
		UserID: userID, Lang: "zh-Hans", AuthDate: testNow(), ChatID: -100123,
		JoinQueryID: "query+42", StartParam: "entry",
	}
	calls := 0
	a.Mount(testRoutes{{http.MethodGet, "/identity", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		got, ok := IdentityFrom(r.Context())
		if !ok || got.UserID != want.UserID || got.Lang != want.Lang || !got.AuthDate.Equal(want.AuthDate) ||
			got.ChatID != want.ChatID || got.JoinQueryID != want.JoinQueryID || got.StartParam != want.StartParam {
			t.Errorf("identity = %+v, ok = %t, want %+v", got, ok, want)
		}
		w.WriteHeader(http.StatusNoContent)
	})}})
	for _, scheme := range []string{"tma", "TmA"} {
		w := serve(mux, makeRequest(http.MethodGet, "/api/v1/identity", scheme+" "+raw))
		if w.Code != http.StatusNoContent {
			t.Fatalf("request with %q scheme returned %d: %s", scheme, w.Code, w.Body)
		}
	}
	if calls != 2 || logs.Len() != 0 {
		t.Fatalf("got %d handler calls and %d log bytes, want 2 calls and no logs", calls, logs.Len())
	}
}

func TestIdentityFromWithoutAuthentication(t *testing.T) {
	got, ok := IdentityFrom(context.Background())
	if ok || got != (initdata.Identity{}) {
		t.Fatalf("IdentityFrom unverified context = %+v, %t, want zero, false", got, ok)
	}
}

func TestAuthenticationFailuresShareResponseAndCountReason(t *testing.T) {
	now := testNow()
	signed := signInitData(t, initDataValues(t, 42, "en", now))
	invalidHash := strings.Replace(signed, "%3A42", "%3A43", 1)
	noUser := url.Values{"auth_date": {strconv.FormatInt(now.Unix(), 10)}}
	badUser := initDataValues(t, 42, "en", now)
	badUser.Set("user", "{")
	tests := []struct {
		name   string
		header []string
		reason string
	}{
		{"missing authorization", nil, "missing"},
		{"empty authorization", []string{""}, "malformed"},
		{"wrong scheme", []string{"Bearer " + signed}, "malformed"},
		{"scheme only", []string{"tma"}, "malformed"},
		{"empty credentials", []string{"tma "}, "malformed"},
		{"extra separator", []string{"tma  " + signed}, "malformed"},
		{"ambiguous authorization", []string{"tma " + signed, "tma " + signed}, "malformed"},
		{"wrong signature", []string{"tma " + invalidHash}, "bad_hash"},
		{"missing hash", []string{"tma " + initDataValues(t, 42, "en", now).Encode()}, "bad_hash"},
		{"expired", []string{"tma " + signInitData(t, initDataValues(t, 42, "en", now.Add(-testMaxAge-time.Second)))}, "expired"},
		{"future", []string{"tma " + signInitData(t, initDataValues(t, 42, "en", now.Add(121*time.Second)))}, "future"},
		{"no user", []string{"tma " + signInitData(t, noUser)}, "no_user"},
		{"invalid user JSON", []string{"tma " + signInitData(t, badUser)}, "malformed"},
		{"invalid form", []string{"tma private-data=%GG"}, "malformed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, mux, set, logs := newTestAPI(t)
			a.Mount(testRoutes{{http.MethodGet, "/identity", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Error("unauthenticated request reached the handler")
			})}})
			r := makeRequest(http.MethodGet, "/api/v1/identity", "")
			for _, h := range tc.header {
				r.Header.Add("Authorization", h)
			}
			w := serve(mux, r)
			assertAPIError(t, w, http.StatusUnauthorized, "unauthorized", "Authentication failed.")

			for _, reason := range []string{"missing", "malformed", "bad_hash", "expired", "future", "no_user"} {
				var want uint64
				if reason == tc.reason {
					want = 1
				}
				metric := fmt.Sprintf(`yabot_miniapp_auth_failures_total{reason=%q}`, reason)
				if got := set.GetOrCreateCounter(metric).Get(); got != want {
					t.Errorf("%s = %d, want %d", metric, got, want)
				}
			}
			entries := readLogs(t, logs)
			if len(entries) != 1 {
				t.Fatalf("got %d log entries, want 1", len(entries))
			}
			entry := entries[0]
			if len(entry) != 5 || entry["level"] != "INFO" || entry["reason"] != tc.reason || entry["request_id"] != "" {
				t.Error("auth failure log must contain only reason and request ID as extra Info fields")
			}
			sensitive := append([]string{testBotToken}, tc.header...)
			for _, value := range sensitive {
				_, raw, _ := strings.Cut(value, " ")
				for _, secret := range []string{value, raw} {
					if secret != "" && (strings.Contains(w.Body.String(), secret) ||
						strings.Contains(fmt.Sprint(entry), secret)) {
						t.Error("response or log exposed credentials")
					}
				}
			}
		})
	}
}

func TestUserLimitOnlyChargesAuthenticatedUserAcrossRoutes(t *testing.T) {
	d, mux, set, logs := testDeps()
	now := testNow()
	d.Now = func() time.Time { return now }
	a := mustNewAPI(t, d)
	var handlerCalls int
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		handlerCalls++
		w.WriteHeader(http.StatusNoContent)
	})
	a.Mount(testRoutes{{http.MethodGet, "/first", handler}, {http.MethodGet, "/second", handler}})
	const userID = 42
	firstValues := initDataValues(t, userID, "en", now.Add(-time.Second))
	firstValues.Set("chat", `{"id":-100123}`)
	firstValues.Set("chat_join_request_query_id", "first-query")
	firstAuthorization := "tma " + signInitData(t, firstValues)

	renewedValues := initDataValues(t, userID, "zh", now)
	renewedValues.Set("chat", `{"id":-100456}`)
	renewedValues.Set("chat_join_request_query_id", "renewed-query")
	renewedAuthorization := "tma " + signInitData(t, renewedValues)
	otherUserAuthorization := validAuthorization(t, 43, "en")
	forgedAuthorization := strings.Replace(firstAuthorization, "language_code", "language", 1)
	const unauthorized = "Authentication failed."
	for _, tc := range []struct {
		name          string
		path          string
		authorization string
		advance       time.Duration
		want          int
		message       string
	}{
		{"forged", "/first", forgedAuthorization, 0, http.StatusUnauthorized, unauthorized},
		{"forged again", "/first", forgedAuthorization, 0, http.StatusUnauthorized, unauthorized},
		{"forged beyond burst", "/first", forgedAuthorization, 0, http.StatusUnauthorized, unauthorized},
		{"first spend", "/first", firstAuthorization, 0, http.StatusNoContent, ""},
		{"second spend on another route", "/second", firstAuthorization, 0, http.StatusNoContent, ""},
		{"renewed initData shares exhausted bucket", "/first", renewedAuthorization, 0, http.StatusTooManyRequests, "请求过于频繁。"},
		{"other user unaffected", "/first", otherUserAuthorization, 0, http.StatusNoContent, ""},
		{"invalid while exhausted", "/first", forgedAuthorization, 0, http.StatusUnauthorized, unauthorized},
		{"one token refilled", "/first", firstAuthorization, 200 * time.Millisecond, http.StatusNoContent, ""},
		{"refilled token spent", "/first", firstAuthorization, 0, http.StatusTooManyRequests, "Too many requests."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now = now.Add(tc.advance)
			r := makeRequest(http.MethodGet, "/api/v1"+tc.path, tc.authorization)
			r.Header.Set("Accept-Language", "en")
			w := serve(mux, r)
			switch tc.want {
			case http.StatusTooManyRequests:
				assertAPIError(t, w, tc.want, "rate_limited", tc.message)
				if got := w.Result().Header.Get("Retry-After"); got != "1" {
					t.Errorf("Retry-After = %q, want 1", got)
				}
			case http.StatusUnauthorized:
				assertAPIError(t, w, tc.want, "unauthorized", tc.message)
			default:
				if w.Code != tc.want {
					t.Errorf("status = %d, want %d; body = %s", w.Code, tc.want, w.Body)
				}
			}
		})
	}
	if handlerCalls != 4 {
		t.Errorf("route handler ran %d times, want 4 (only the 204 cases)", handlerCalls)
	}
	if got := set.GetOrCreateCounter("yabot_miniapp_rate_limited_total").Get(); got != 2 {
		t.Errorf("rate limited counter = %d, want 2", got)
	}
	if got := len(readLogs(t, logs)); got != 4 {
		t.Errorf("got %d API logs, want one for each of four authentication failures", got)
	}
}

func TestAuthenticationInternalErrorUsesRequestLanguage(t *testing.T) {
	for _, tc := range []struct {
		name, acceptLanguage, message string
	}{
		{"Chinese header", "zh-CN,en;q=0.8", "服务器内部错误。"},
		{"English header", "en,zh-CN;q=0.8", "Internal server error."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, mux, set, logs := newTestAPI(t)
			// Bypass constructor validation to exercise the runtime verifier error.
			a.maxAge = 0
			a.Mount(testRoutes{{http.MethodGet, "/identity", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Error("authentication error reached the handler")
			})}})
			r := makeRequest(http.MethodGet, "/api/v1/identity", validAuthorization(t, 42, "en"))
			r.Header.Set("Accept-Language", tc.acceptLanguage)
			assertAPIError(t, serve(mux, r), http.StatusInternalServerError, "internal", tc.message)

			if got := set.GetOrCreateCounter(`yabot_miniapp_internal_errors_total{reason="auth"}`).Get(); got != 1 {
				t.Errorf("internal authentication failures = %d, want 1", got)
			}
			if entries := readLogs(t, logs); len(entries) != 1 || entries[0]["level"] != "ERROR" {
				t.Error("expected one Error log for the internal authentication failure")
			}
		})
	}
}

func TestUserLimitCountsEvictionsAtCapacity(t *testing.T) {
	d, mux, set, _ := testDeps()
	d.UserBurst = 1
	a := mustNewAPI(t, d)
	a.Mount(testRoutes{{http.MethodGet, "/limit", http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) },
	)}})
	const capacity = 10_000
	evictions := set.GetOrCreateCounter("yabot_miniapp_user_limiter_evictions_total")
	call := func(id int64) {
		t.Helper()
		w := serve(mux, makeRequest(http.MethodGet, "/api/v1/limit", validAuthorization(t, id, "en")))
		if w.Code != http.StatusNoContent {
			t.Fatalf("user %d: status = %d, want 204", id, w.Code)
		}
	}
	for id := int64(1); id <= capacity; id++ {
		call(id)
	}
	if got := evictions.Get(); got != 0 {
		t.Errorf("got %d evictions at capacity, want 0", got)
	}
	call(capacity + 1)
	if got := evictions.Get(); got != 1 {
		t.Errorf("got %d evictions when capacity is exceeded, want 1", got)
	}
	call(1)
	if got := evictions.Get(); got != 2 {
		t.Errorf("got %d evictions after returning user, want 2", got)
	}
}
