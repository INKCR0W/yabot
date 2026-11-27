package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/INKCR0W/yabot/internal/platform/httpx"
)

func TestDecodeJSON(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		limit    int64
		wantID   int64
		wantErr  bool
		tooLarge bool
	}{
		{name: "one value", body: "{\"id\":4503599627370495} \n\t", wantID: 4503599627370495},
		{name: "exact limit", body: `{"id":1}`, limit: 8, wantID: 1},
		{name: "unknown field", body: `{"extra":true}`, wantErr: true},
		{name: "extra value", body: "{}{}", wantErr: true},
		{name: "invalid trailing data", body: "{} x", wantErr: true},
		{name: "incomplete trailing value", body: "{} {", wantErr: true},
		{name: "empty", wantErr: true},
		{name: "whitespace only", body: " \t\n", wantErr: true},
		{name: "body too large", body: `{"id":1}`, limit: 7, wantErr: true, tooLarge: true},
		{name: "trailing whitespace too large", body: "{}    ", limit: 4, wantErr: true, tooLarge: true},
		{name: "second value buffered before overflow", body: "{}{} ", limit: 4, wantErr: true, tooLarge: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body))
			if tc.limit > 0 {
				req.Body = http.MaxBytesReader(httptest.NewRecorder(), req.Body, tc.limit)
			}
			var payload struct {
				ID int64 `json:"id"`
			}
			err := DecodeJSON(req, &payload)
			if (err != nil) != tc.wantErr {
				t.Fatalf("DecodeJSON error = %v, want error: %t", err, tc.wantErr)
			}
			if tc.tooLarge {
				var limit *http.MaxBytesError
				if !errors.As(err, &limit) || limit.Limit != tc.limit {
					t.Errorf("DecodeJSON error = %v, want *http.MaxBytesError with limit %d", err, tc.limit)
				}
			}
			if !tc.wantErr && payload.ID != tc.wantID {
				t.Errorf("decoded ID = %d, want %d", payload.ID, tc.wantID)
			}
		})
	}
}

func TestWriteJSON(t *testing.T) {
	const userID int64 = 4503599627370495
	a, _, metrics, logs := newTestAPI(t)
	res := httptest.NewRecorder()
	res.Header().Set("Content-Length", "0")

	a.WriteJSON(res, http.StatusCreated, map[string]int64{"user_id": userID})

	if res.Code != http.StatusCreated {
		t.Errorf("status = %d, want %d", res.Code, http.StatusCreated)
	}
	assertJSONHeaders(t, res.Result().Header)
	var got map[string]int64
	if err := json.Unmarshal(res.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(got) != 1 || got["user_id"] != userID {
		t.Errorf("response = %v, want only user_id %d", got, userID)
	}
	if logs.Len() != 0 {
		t.Errorf("unexpected logs: %s", logs)
	}
	if got := metrics.GetOrCreateCounter(`yabot_miniapp_internal_errors_total{reason="encoding"}`).Get(); got != 0 {
		t.Errorf("encoding failures = %d, want 0", got)
	}
}

func TestWriteJSONEncodingFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		data any
	}{
		{name: "unsupported float", data: math.Inf(1)},
		{name: "sensitive marshaler error", data: brokenJSON{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _, metrics, logs := newTestAPI(t)
			res := httptest.NewRecorder()
			res.Header().Set("Content-Length", "0")

			a.WriteJSON(res, http.StatusCreated, tc.data)

			assertAPIError(t, res, http.StatusInternalServerError, "internal", "Internal server error.")
			if strings.Contains(res.Body.String(), privateMarshalError) || strings.Contains(logs.String(), privateMarshalError) {
				t.Error("marshaler error text reached a response or log")
			}
			var logEntry struct {
				Level string `json:"level"`
			}
			if err := json.Unmarshal(logs.Bytes(), &logEntry); err != nil {
				t.Fatalf("expected one log record: %v", err)
			}
			if logEntry.Level != slog.LevelError.String() {
				t.Errorf("log level = %q, want ERROR", logEntry.Level)
			}
			if got := metrics.GetOrCreateCounter(`yabot_miniapp_internal_errors_total{reason="encoding"}`).Get(); got != 1 {
				t.Errorf("encoding failures = %d, want 1", got)
			}
		})
	}
}

func TestWriteJSONWriteFailureIsCountedByHTTPX(t *testing.T) {
	d, _, set, logs := testDeps()
	server := httpx.New(httpx.Deps{Metrics: set, Log: d.Log, Now: d.Now})
	d.Server = server
	a := mustNewAPI(t, d)
	a.Mount(testRoutes{{http.MethodGet, "/json", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		a.WriteJSON(w, http.StatusOK, true)
	})}})

	res := httptest.NewRecorder()
	r := makeRequest(http.MethodGet, "/api/v1/json", validAuthorization(t, 42, "en"))
	server.Handler().ServeHTTP(failingJSONWriter{res}, r)

	if res.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", res.Code, http.StatusOK)
	}
	if got := set.GetOrCreateCounter("yabot_http_write_errors_total").Get(); got != 1 {
		t.Errorf("HTTP write failures = %d, want 1", got)
	}
	if slices.Contains(set.ListMetricNames(), "yabot_miniapp_write_errors_total") {
		t.Error("API registered a duplicate write failure counter")
	}
	if entries := readLogs(t, logs); len(entries) != 1 || entries[0]["level"] != "INFO" {
		t.Errorf("expected only an Info access log for a write failure, got %v", entries)
	}
}

func assertJSONHeaders(t *testing.T, headers http.Header) {
	t.Helper()
	for key, want := range map[string]string{
		"Content-Type":           "application/json; charset=utf-8",
		"Cache-Control":          "no-store",
		"X-Content-Type-Options": "nosniff",
		"Content-Length":         "",
	} {
		if got := headers.Get(key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
}

const privateMarshalError = "private marshaler failure details"

type brokenJSON struct{}

func (brokenJSON) MarshalJSON() ([]byte, error) {
	return nil, errors.New(privateMarshalError)
}

type failingJSONWriter struct {
	*httptest.ResponseRecorder
}

func (failingJSONWriter) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}
