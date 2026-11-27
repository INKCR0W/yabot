package api

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/httpx"
	"github.com/INKCR0W/yabot/internal/platform/i18n"
	"github.com/VictoriaMetrics/metrics"
)

const (
	testBotToken = "123456:miniapp-test-token"
	testMaxAge   = 24 * time.Hour
)

func testNow() time.Time {
	return time.Unix(1_800_000_000, 0).UTC()
}

func testDeps() (Deps, *http.ServeMux, *metrics.Set, *bytes.Buffer) {
	mux := http.NewServeMux()
	set := metrics.NewSet()
	logs := new(bytes.Buffer)
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	return Deps{
		Server: mux, BotToken: testBotToken, MaxAge: testMaxAge, UserRPS: 5, UserBurst: 2,
		Catalog: i18n.New(i18n.Deps{Metrics: set, Log: logger}), Metrics: set, Log: logger, Now: testNow,
	}, mux, set, logs
}

func mustNewAPI(t *testing.T, d Deps) *API {
	t.Helper()
	a, err := New(d)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func newTestAPI(t *testing.T) (*API, *http.ServeMux, *metrics.Set, *bytes.Buffer) {
	t.Helper()
	d, mux, set, logs := testDeps()
	return mustNewAPI(t, d), mux, set, logs
}

type testRoutes []Route

func (rs testRoutes) Routes() []Route { return rs }

func makeRequest(method, path, authorization string) *http.Request {
	r := httptest.NewRequest(method, path, nil)
	if authorization != "" {
		r.Header.Set("Authorization", authorization)
	}
	return r
}

func serve(h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func readLogs(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(logs.Bytes()))
	var lines []map[string]any
	for {
		var line map[string]any
		err := decoder.Decode(&line)
		if errors.Is(err, io.EOF) {
			return lines
		}
		if err != nil {
			t.Fatalf("decode log entry: %v", err)
		}
		lines = append(lines, line)
	}
}

func assertAPIError(t *testing.T, w *httptest.ResponseRecorder, status int, code, message string) {
	t.Helper()
	if w.Code != status {
		t.Fatalf("status = %d, want %d; body = %s", w.Code, status, w.Body)
	}
	assertJSONHeaders(t, w.Result().Header)
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	decoder := json.NewDecoder(bytes.NewReader(w.Body.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if body.Error.Code != code || body.Error.Message != message {
		t.Fatalf("body = %s, want code %q and message %q", w.Body, code, message)
	}
}

func TestNewRejectsInvalidDeps(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Deps)
		want string
	}{
		{"missing server", func(d *Deps) { d.Server = nil }, "route registrar"},
		{"missing token", func(d *Deps) { d.BotToken = "" }, "bot token"},
		{"zero age", func(d *Deps) { d.MaxAge = 0 }, "maximum age"},
		{"negative age", func(d *Deps) { d.MaxAge = -time.Second }, "maximum age"},
		{"zero rate", func(d *Deps) { d.UserRPS = 0 }, "user rate"},
		{"negative rate", func(d *Deps) { d.UserRPS = -1 }, "user rate"},
		{"NaN rate", func(d *Deps) { d.UserRPS = math.NaN() }, "user rate"},
		{"infinite rate", func(d *Deps) { d.UserRPS = math.Inf(1) }, "user rate"},
		{"reserved rate", func(d *Deps) { d.UserRPS = math.MaxFloat64 }, "user rate"},
		{"zero burst", func(d *Deps) { d.UserBurst = 0 }, "user burst"},
		{"negative burst", func(d *Deps) { d.UserBurst = -1 }, "user burst"},
		{"missing catalog", func(d *Deps) { d.Catalog = nil }, "translation catalog"},
		{"missing metrics", func(d *Deps) { d.Metrics = nil }, "metrics set"},
		{"missing logger", func(d *Deps) { d.Log = nil }, "logger"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, mux, _, _ := testDeps()
			tc.edit(&d)
			got, err := New(d)
			if got != nil || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("New returned API = %t, error = %v, want error containing %q", got != nil, err, tc.want)
			}
			if err != nil && strings.Contains(err.Error(), testBotToken) {
				t.Fatal("configuration error exposed the bot token")
			}
			_, pattern := mux.Handler(makeRequest(http.MethodGet, "/api/v1/missing", ""))
			if pattern != "" {
				t.Fatalf("invalid configuration registered pattern %q", pattern)
			}
		})
	}
}

func TestMountDispatchesMethodAndPath(t *testing.T) {
	a, mux, set, _ := newTestAPI(t)
	a.Mount(testRoutes{{http.MethodPut, "/chats/{id}/settings/{feature}", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.Pattern != "PUT /api/v1/chats/{id}/settings/{feature}" ||
				r.PathValue("id") != "-100123" || r.PathValue("feature") != "guard" {
				t.Errorf("pattern = %q, id = %q, feature = %q", r.Pattern, r.PathValue("id"), r.PathValue("feature"))
			}
			w.WriteHeader(http.StatusNoContent)
		},
	)}})
	const path = "/api/v1/chats/-100123/settings/guard"
	w := serve(mux, makeRequest(http.MethodPut, path, validAuthorization(t, 42, "en")))
	if w.Code != http.StatusNoContent {
		t.Fatalf("mounted route returned %d: %s", w.Code, w.Body)
	}
	for _, tc := range []struct {
		name, path, authorization, acceptLanguage, message string
	}{
		{"unmatched path", "/api/v1/missing", "", "zh-CN,zh;q=0.9,en;q=0.8", "未找到所请求的资源。"},
		{"unmatched method", path, "Bearer invalid", "", "Not found."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := makeRequest(http.MethodGet, tc.path, tc.authorization)
			if tc.acceptLanguage != "" {
				r.Header.Set("Accept-Language", tc.acceptLanguage)
			}
			assertAPIError(t, serve(mux, r), http.StatusNotFound, "not_found", tc.message)
		})
	}
	if got := set.GetOrCreateCounter(`yabot_miniapp_auth_failures_total{reason="missing"}`).Get(); got != 0 {
		t.Fatalf("missing auth counter = %d after fallback, want 0", got)
	}
	if got := set.GetOrCreateCounter(`yabot_miniapp_auth_failures_total{reason="malformed"}`).Get(); got != 0 {
		t.Fatalf("malformed auth counter = %d after fallback, want 0", got)
	}
}

func TestMountPanicsForInvalidRoutes(t *testing.T) {
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	for _, tc := range []struct {
		name   string
		routes testRoutes
	}{
		{"duplicate", testRoutes{{http.MethodGet, "/same", handler}, {http.MethodGet, "/same", handler}}},
		{"missing slash", testRoutes{{http.MethodGet, "relative", handler}}},
		{"missing method", testRoutes{{"", "/relative", handler}}},
		{"invalid method", testRoutes{{"GET extra", "/relative", handler}}},
		{"missing handler", testRoutes{{http.MethodGet, "/relative", nil}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _, _, _ := newTestAPI(t)
			defer func() {
				if recover() == nil {
					t.Error("Mount did not panic")
				}
			}()
			a.Mount(tc.routes)
		})
	}
}

func TestWithHTTPXRecordsRouteAndRequestID(t *testing.T) {
	d, _, set, logs := testDeps()
	server := httpx.New(httpx.Deps{Metrics: set, Log: d.Log, Now: d.Now})
	d.Server = server
	a := mustNewAPI(t, d)
	a.Mount(testRoutes{{http.MethodGet, "/identity/{label}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, ok := IdentityFrom(r.Context())
		if !ok || identity.UserID != 42 {
			t.Errorf("identity = %+v, ok = %t", identity, ok)
		}
		if r.PathValue("label") != "private-path" || httpx.RequestID(r.Context()) == "" {
			t.Error("missing path value or request ID in handler")
		}
		a.WriteJSON(w, http.StatusOK, map[string]int64{"user_id": identity.UserID})
	})}})

	const path = "/api/v1/identity/private-path?private-query=value"
	valid := makeRequest(http.MethodGet, path, validAuthorization(t, 42, "en"))
	valid.Header.Set("X-Request-Id", "client-provided-id")
	w := serve(server.Handler(), valid)
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"user_id":42}` {
		t.Fatalf("valid request returned %d: %s", w.Code, w.Body)
	}
	requestID := w.Result().Header.Get("X-Request-Id")
	if _, err := hex.DecodeString(requestID); err != nil || len(requestID) != 16 {
		t.Fatalf("invalid generated request ID %q", requestID)
	}

	rejected := httptest.NewRequest(http.MethodGet, path, strings.NewReader("private body"))
	rejected.Header.Set("Authorization", "tma private-credential")
	rejected.Header.Set("Accept-Language", "zh-CN,en;q=0.8")
	failure := serve(server.Handler(), rejected)
	assertAPIError(t, failure, http.StatusUnauthorized, "unauthorized", "身份验证失败。")
	if got := failure.Result().Header.Get("Connection"); got != "close" {
		t.Fatalf("Connection = %q for rejected request with unread body", got)
	}

	entries := readLogs(t, logs)
	if len(entries) != 3 {
		t.Fatalf("got %d log records, want 3", len(entries))
	}
	for _, tc := range []struct {
		entry     map[string]any
		requestID string
		status    float64
	}{
		{entries[0], requestID, http.StatusOK},
		{entries[2], failure.Result().Header.Get("X-Request-Id"), http.StatusUnauthorized},
	} {
		if tc.entry["level"] != "INFO" || tc.entry["route"] != "GET /api/v1/identity/{label}" ||
			tc.entry["request_id"] != tc.requestID || tc.entry["status"] != tc.status {
			t.Errorf("unexpected access log: %v", tc.entry)
		}
	}
	if entry := entries[1]; entry["request_id"] != failure.Result().Header.Get("X-Request-Id") ||
		entry["reason"] != "bad_hash" || entry["level"] != "INFO" {
		t.Errorf("unexpected authentication log: %v", entry)
	}
	for _, secret := range []string{testBotToken, "private-path", "private-query", "private body", "private-credential", "192.0.2.1"} {
		if strings.Contains(logs.String(), secret) {
			t.Error("HTTP logs include sensitive request data")
		}
	}
}
