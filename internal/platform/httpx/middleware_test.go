package httpx

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

func TestInflightLimitRejectsWithoutConsumingIPToken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newMiddlewareHarness(Deps{MaxInflight: 2, IPBurst: 1})
		entered := make(chan struct{}, 2)
		release := make(chan struct{})
		finished := make(chan int, 2)
		defer close(release)
		h.server.Handle("GET /hold", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			entered <- struct{}{}
			<-release
			w.WriteHeader(http.StatusNoContent)
		}))
		h.server.Handle("GET /ready", http.HandlerFunc(middlewareReady))
		for _, peer := range []string{"192.0.2.1:1234", "192.0.2.2:1234"} {
			go func() {
				finished <- h.serve(middlewareRequest(http.MethodGet, "/hold", peer, nil)).Code
			}()
		}
		<-entered
		<-entered
		gauge := h.set.GetOrCreateGauge("yabot_http_inflight", nil)
		if got := gauge.Get(); got != 2 {
			t.Errorf("inflight = %v, want 2", got)
		}
		const peer = "192.0.2.3:1234"
		busy := h.serve(middlewareRequest(http.MethodGet, "/ready", peer, nil))
		assertMiddlewareError(t, busy, http.StatusServiceUnavailable, "busy", "server busy")
		h.assertCounter(t, `yabot_http_rejected_total{reason="inflight"}`, 1)
		h.assertCounter(t, `yabot_http_rejected_total{reason="ip_rate"}`, 0)
		release <- struct{}{}
		if status := <-finished; status != http.StatusNoContent {
			t.Errorf("first released handler returned %d, want 204", status)
		}
		assertMiddlewareStatus(t, h.serve(middlewareRequest(http.MethodGet, "/ready", peer, nil)), http.StatusNoContent)
		release <- struct{}{}
		if status := <-finished; status != http.StatusNoContent {
			t.Errorf("second released handler returned %d, want 204", status)
		}
		if got := gauge.Get(); got != 0 {
			t.Errorf("inflight after both handlers finished = %v, want 0", got)
		}
	})
}

func TestInflightSlotReleasedOnHandlerExit(t *testing.T) {
	tests := []struct {
		name   string
		exit   func()
		panics uint64
	}{
		{name: "return", exit: func() {}},
		{name: "panic", exit: func() { panic("handler failed") }, panics: 1},
		{name: "goexit", exit: runtime.Goexit},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newMiddlewareHarness(Deps{MaxInflight: 1})
				h.server.Handle("GET /exit", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					tc.exit()
					w.WriteHeader(http.StatusNoContent)
				}))
				h.server.Handle("GET /ready", http.HandlerFunc(middlewareReady))
				done := make(chan struct{})
				go func() {
					defer close(done)
					h.serve(middlewareRequest(http.MethodGet, "/exit", "192.0.2.1:1234", nil))
				}()
				<-done
				assertMiddlewareStatus(t, h.serve(middlewareRequest(http.MethodGet, "/ready", "192.0.2.1:1234", nil)), http.StatusNoContent)
				h.assertCounter(t, "yabot_http_panics_total", tc.panics)
			})
		})
	}
}

func TestIPRateLimitRefillsAndKeepsClientsIndependent(t *testing.T) {
	now := time.Unix(1700000000, 0)
	h := newMiddlewareHarness(Deps{IPRPS: 2, IPBurst: 2, Now: func() time.Time { return now }})
	h.server.Handle("GET /ready", http.HandlerFunc(middlewareReady))
	const firstPeer, secondPeer = "192.0.2.1:1234", "192.0.2.2:1234"
	for _, tc := range []struct {
		peer   string
		wait   time.Duration
		status int
	}{
		{firstPeer, 0, http.StatusNoContent},
		{firstPeer, 0, http.StatusNoContent},
		{firstPeer, 0, http.StatusTooManyRequests},
		{secondPeer, 0, http.StatusNoContent},
		{secondPeer, 0, http.StatusNoContent},
		{secondPeer, 0, http.StatusTooManyRequests},
		{firstPeer, 499 * time.Millisecond, http.StatusTooManyRequests},
		{firstPeer, time.Millisecond, http.StatusNoContent},
		{secondPeer, 0, http.StatusNoContent},
	} {
		now = now.Add(tc.wait)
		res := h.serve(middlewareRequest(http.MethodGet, "/ready", tc.peer, nil))
		if tc.status == http.StatusTooManyRequests {
			assertMiddlewareError(t, res, tc.status, "rate_limited", "too many requests")
			if retry := res.Header().Get("Retry-After"); retry != "1" {
				t.Errorf("Retry-After = %q, want 1", retry)
			}
			continue
		}
		assertMiddlewareStatus(t, res, tc.status)
	}
	h.assertCounter(t, `yabot_http_rejected_total{reason="ip_rate"}`, 3)
}

func TestIPRateLimitNormalizesAddressKeys(t *testing.T) {
	for _, tc := range []struct {
		name, first, second string
		status              int
	}{
		{"same IPv6 prefix", "[2001:db8:3:4::1]:1234", "[2001:db8:3:4:ffff::17]:1235", http.StatusTooManyRequests},
		{"different IPv6 prefixes", "[2001:db8:3:4::1]:1234", "[2001:db8:3:5::1]:1234", http.StatusNoContent},
		{"mapped IPv4", "192.0.2.11:1234", "[::ffff:192.0.2.11]:1235", http.StatusTooManyRequests},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newMiddlewareHarness(Deps{IPBurst: 1})
			h.server.Handle("GET /ready", http.HandlerFunc(middlewareReady))
			assertMiddlewareStatus(t, h.serve(middlewareRequest(http.MethodGet, "/ready", tc.first, nil)), http.StatusNoContent)
			assertMiddlewareStatus(t, h.serve(middlewareRequest(http.MethodGet, "/ready", tc.second, nil)), tc.status)
		})
	}
}

func TestMaxBytesLimitsReadsAndCountsOverflowOncePerRequest(t *testing.T) {
	const maxBody = 64 << 10
	for _, tc := range []struct {
		name string
		size int
	}{
		{"empty", 0},
		{"under limit", maxBody - 1},
		{"at limit", maxBody},
		{"over limit", maxBody + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newMiddlewareHarness(Deps{})
			h.server.Handle("POST /upload", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if len(body) != min(maxBody, tc.size) {
					t.Errorf("read %d bytes, want %d", len(body), min(maxBody, tc.size))
				}
				if tc.size > maxBody {
					var maxErr *http.MaxBytesError
					if !errors.As(err, &maxErr) || maxErr.Limit != maxBody {
						t.Errorf("read error = %v, want *http.MaxBytesError at %d", err, maxBody)
					}
					var sample [1]byte
					for range 2 {
						if n, err := r.Body.Read(sample[:]); n != 0 || !errors.As(err, &maxErr) {
							t.Errorf("read after overflow = (%d, %v), want zero bytes and *http.MaxBytesError", n, err)
						}
					}
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if err != nil {
					t.Errorf("read within limit: %v", err)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			body := strings.Repeat("x", tc.size)
			var overflows uint64
			for range 2 {
				res := h.serve(middlewareRequest(http.MethodPost, "/upload", "192.0.2.1:1234", strings.NewReader(body)))
				want := http.StatusNoContent
				if tc.size > maxBody {
					want = http.StatusBadRequest
					overflows++
				}
				assertMiddlewareStatus(t, res, want)
				wantConnection := ""
				if tc.size > maxBody {
					// The rest of an oversized body is never read, so the connection must not wait for it.
					wantConnection = "close"
				}
				if got := res.Result().Header.Get("Connection"); got != wantConnection {
					t.Errorf("Connection = %q, want %q", got, wantConnection)
				}
				h.assertCounter(t, "yabot_http_body_too_large_total", overflows)
			}
		})
	}
}

func TestIPRejectionHasRequestIDAndAccessLogWithoutReadingBody(t *testing.T) {
	h := newMiddlewareHarness(Deps{IPBurst: 1})
	h.server.Handle("POST /upload", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		var maxErr *http.MaxBytesError
		if !errors.As(err, &maxErr) || len(body) != 64<<10 {
			t.Errorf("read %d bytes with error %v, want MaxBytesError after 64 KiB", len(body), err)
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	const peer = "192.0.2.1:1234"
	oversized := strings.Repeat("x", (64<<10)+1)
	allowed := h.serve(middlewareRequest(http.MethodPost, "/upload", peer, strings.NewReader(oversized)))
	assertMiddlewareStatus(t, allowed, http.StatusBadRequest)
	body := &middlewareCountingReader{Reader: strings.NewReader(oversized)}
	req := middlewareRequest(http.MethodPost, "/upload", peer, body)
	req.ContentLength = int64(len(oversized))
	rejected := h.serve(req)
	assertMiddlewareError(t, rejected, http.StatusTooManyRequests, "rate_limited", "too many requests")
	if body.reads != 0 {
		t.Errorf("IP-rejected body was read %d times", body.reads)
	}
	logs := h.records(t)
	if len(logs) != 2 {
		t.Fatalf("logs = %+v, want one access record for each request", logs)
	}
	assertMiddlewareAccess(t, logs[0], allowed, "POST /upload", http.StatusBadRequest)
	assertMiddlewareAccess(t, logs[1], rejected, "unmatched", http.StatusTooManyRequests)
	h.assertCounter(t, `yabot_http_requests_total{route="POST /upload",status="400"}`, 1)
	h.assertCounter(t, `yabot_http_requests_total{route="unmatched",status="429"}`, 1)
	h.assertCounter(t, `yabot_http_rejected_total{reason="ip_rate"}`, 1)
	h.assertCounter(t, "yabot_http_body_too_large_total", 1)
}

func TestRecoveryAndAccessLogKeepRouteTemplateAndOmitRequestDetails(t *testing.T) {
	h := newMiddlewareHarness(Deps{})
	const pattern = "GET /api/v1/chats/{id}"
	h.server.Handle(pattern, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") == "-1001" {
			panic(r.Header.Get("Authorization"))
		}
		w.WriteHeader(http.StatusCreated)
		if _, err := io.WriteString(w, "created"); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	const peer, auth, agent = "192.0.2.123:4321", "tma authorization-secret", "private-user-agent"
	paths := []string{"/api/v1/chats/-1001", "/api/v1/chats/-1002", "/private-unmatched"}
	wantRoute := []string{pattern, pattern, "unmatched"}
	wantStatus := []int{http.StatusInternalServerError, http.StatusCreated, http.StatusNotFound}
	responses := make([]*httptest.ResponseRecorder, len(paths))
	for i, path := range paths {
		req := middlewareRequest(http.MethodGet, path+"?token=query-secret", peer, nil)
		req.Header.Set("Authorization", auth)
		req.Header.Set("User-Agent", agent)
		responses[i] = h.serve(req)
		assertMiddlewareStatus(t, responses[i], wantStatus[i])
	}
	assertMiddlewareError(t, responses[0], http.StatusInternalServerError, "internal", "internal error")
	h.assertCounter(t, "yabot_http_panics_total", 1)
	h.assertCounter(t, `yabot_http_requests_total{route="GET /api/v1/chats/{id}",status="500"}`, 1)
	h.assertCounter(t, `yabot_http_requests_total{route="GET /api/v1/chats/{id}",status="201"}`, 1)
	h.assertCounter(t, `yabot_http_requests_total{route="unmatched",status="404"}`, 1)
	for name, want := range map[string]uint64{
		`yabot_http_request_duration_seconds{route="GET /api/v1/chats/{id}"}`: 2,
		`yabot_http_request_duration_seconds{route="unmatched"}`:              1,
	} {
		var count uint64
		h.set.GetOrCreateHistogram(name).VisitNonZeroBuckets(func(_ string, n uint64) { count += n })
		if count != want {
			t.Errorf("%s has %d observations, want %d", name, count, want)
		}
	}

	var access, loggedErrors []middlewareLogRecord
	for _, record := range h.records(t) {
		switch record.Level {
		case "INFO":
			access = append(access, record)
		case "ERROR":
			loggedErrors = append(loggedErrors, record)
		default:
			t.Errorf("unexpected log: %+v", record)
		}
	}
	if len(access) != len(paths) || len(loggedErrors) != 1 {
		t.Fatalf("got %d access logs and %d error logs, want %d and 1", len(access), len(loggedErrors), len(paths))
	}
	for i, record := range access {
		assertMiddlewareAccess(t, record, responses[i], wantRoute[i], wantStatus[i])
		if record.Method != http.MethodGet || record.Bytes != responses[i].Body.Len() || record.DurationMS == nil || *record.DurationMS < 0 {
			t.Errorf("access[%d] = %+v, want GET, %d bytes and a nonnegative duration", i, record, responses[i].Body.Len())
		}
	}
	if record := loggedErrors[0]; record.RequestID != responses[0].Header().Get("X-Request-Id") || record.Route != pattern || record.Stack == "" || record.Panic == nil {
		t.Errorf("panic log = %+v, want request ID, route, panic and stack", record)
	}
	secrets := append(paths, "query-secret", "192.0.2.123", "authorization-secret", agent)
	for _, secret := range secrets {
		if strings.Contains(h.output.String(), secret) {
			t.Errorf("logs contain private request value %q", secret)
		}
	}
}

func TestRecoverPreservesExplicitAbortWithoutCountingPanic(t *testing.T) {
	h := newMiddlewareHarness(Deps{MaxInflight: 1})
	h.server.Handle("GET /abort", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	h.server.Handle("GET /ready", http.HandlerFunc(middlewareReady))
	const peer = "192.0.2.1:1234"
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		h.serve(middlewareRequest(http.MethodGet, "/abort", peer, nil))
	}()
	if recovered != http.ErrAbortHandler {
		t.Errorf("panic = %v, want http.ErrAbortHandler", recovered)
	}
	h.assertCounter(t, "yabot_http_panics_total", 0)
	for _, record := range h.records(t) {
		if record.Level == "ERROR" || record.Level == "WARN" {
			t.Errorf("explicit abort emitted an unexpected log: %+v", record)
		}
	}
	assertMiddlewareStatus(t, h.serve(middlewareRequest(http.MethodGet, "/ready", peer, nil)), http.StatusNoContent)
}

func TestRequestIDIsGeneratedAndPropagated(t *testing.T) {
	h := newMiddlewareHarness(Deps{})
	var inside string
	h.server.Handle("GET /ready", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inside = RequestID(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	const supplied = "adeafbeef0123456"
	seen := make(map[string]struct{})
	for range 12 {
		req := middlewareRequest(http.MethodGet, "/ready", "192.0.2.1:1234", nil)
		req.Header.Set("X-Request-Id", supplied)
		res := h.serve(req)
		assertMiddlewareStatus(t, res, http.StatusNoContent)
		id := res.Header().Get("X-Request-Id")
		if _, err := hex.DecodeString(id); err != nil || len(id) != 16 || id == supplied || id != inside {
			t.Errorf("header ID = %q, context ID = %q, want the same generated 16-digit hex ID", id, inside)
		}
		if _, exists := seen[id]; exists {
			t.Errorf("duplicate generated request ID %q", id)
		}
		seen[id] = struct{}{}
	}
	if got := RequestID(context.Background()); got != "" {
		t.Errorf("RequestID without request middleware = %q, want empty", got)
	}
}

func TestWriteErrorUsesJSONEnvelopeAndHeaders(t *testing.T) {
	for _, tc := range []struct {
		name, code, message string
		status              int
	}{
		{"service unavailable", "busy", "server busy", http.StatusServiceUnavailable},
		{"escaped content", "quoted\"code", "line one\nline two\\", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := httptest.NewRecorder()
			WriteError(res, tc.status, tc.code, tc.message)
			assertMiddlewareError(t, res, tc.status, tc.code, tc.message)
		})
	}
}

func TestResponseWriteErrorCountedWithoutLeakingErrorText(t *testing.T) {
	h := newMiddlewareHarness(Deps{})
	h.server.Handle("GET /ready", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		WriteError(w, http.StatusBadRequest, "bad_request", "bad request")
	}))
	writer := middlewareFailingWriter{ResponseWriter: httptest.NewRecorder()}
	h.server.Handler().ServeHTTP(writer, middlewareRequest(http.MethodGet, "/ready", "192.0.2.1:1234", nil))
	h.assertCounter(t, "yabot_http_write_errors_total", 1)
	for _, record := range h.records(t) {
		if record.Level != "INFO" {
			t.Errorf("response write failure emitted an unexpected log: %+v", record)
		}
	}
	if strings.Contains(h.output.String(), "sensitive-write-error") {
		t.Error("logs contain the raw write error")
	}
}

type middlewareCountingReader struct {
	io.Reader
	reads int
}

func (r *middlewareCountingReader) Read(p []byte) (int, error) {
	r.reads++
	return r.Reader.Read(p)
}

type middlewareFailingWriter struct{ http.ResponseWriter }

func (middlewareFailingWriter) Write([]byte) (int, error) {
	return 0, errors.New("sensitive-write-error")
}

type middlewareHarness struct {
	server *Server
	set    *metrics.Set
	output *middlewareLogBuffer
}

func newMiddlewareHarness(deps Deps) middlewareHarness {
	output := &middlewareLogBuffer{}
	deps.Log = slog.New(slog.NewJSONHandler(output, nil))
	deps.Metrics = metrics.NewSet()
	if deps.Now == nil {
		deps.Now = func() time.Time { return time.Unix(1700000000, 0) }
	}
	return middlewareHarness{server: New(deps), set: deps.Metrics, output: output}
}

func (h middlewareHarness) serve(req *http.Request) *httptest.ResponseRecorder {
	res := httptest.NewRecorder()
	h.server.Handler().ServeHTTP(res, req)
	return res
}

func (h middlewareHarness) assertCounter(t *testing.T, name string, want uint64) {
	t.Helper()
	if got := h.set.GetOrCreateCounter(name).Get(); got != want {
		t.Errorf("%s = %d, want %d", name, got, want)
	}
}

type middlewareLogRecord struct {
	Level      string          `json:"level"`
	RequestID  string          `json:"request_id"`
	Method     string          `json:"method"`
	Route      string          `json:"route"`
	Status     int             `json:"status"`
	Bytes      int             `json:"bytes"`
	DurationMS *float64        `json:"duration_ms"`
	Stack      string          `json:"stack"`
	Panic      json.RawMessage `json:"panic"`
}

func (h middlewareHarness) records(t *testing.T) []middlewareLogRecord {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(h.output.String()))
	var records []middlewareLogRecord
	for {
		var record middlewareLogRecord
		if err := dec.Decode(&record); err != nil {
			if errors.Is(err, io.EOF) {
				return records
			}
			t.Fatalf("decode log: %v", err)
		}
		records = append(records, record)
	}
}

type middlewareLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (log *middlewareLogBuffer) Write(p []byte) (int, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.b.Write(p)
}

func (log *middlewareLogBuffer) String() string {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.b.String()
}

func middlewareRequest(method, path, peer string, body io.Reader) *http.Request {
	req := httptest.NewRequest(method, path, body)
	req.RemoteAddr = peer
	return req
}

func middlewareReady(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func assertMiddlewareStatus(t *testing.T, res *httptest.ResponseRecorder, status int) {
	t.Helper()
	if res.Code != status {
		t.Errorf("status = %d, want %d (body %q)", res.Code, status, res.Body.String())
	}
}

func assertMiddlewareError(t *testing.T, res *httptest.ResponseRecorder, status int, code, message string) {
	t.Helper()
	assertMiddlewareStatus(t, res, status)
	for name, want := range map[string]string{
		"Content-Type":           "application/json; charset=utf-8",
		"Cache-Control":          "no-store",
		"X-Content-Type-Options": "nosniff",
	} {
		if got := res.Header().Get(name); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	var payload map[string]map[string]string
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("invalid error JSON: %v", err)
	}
	if fields := payload["error"]; len(payload) != 1 || len(fields) != 2 || fields["code"] != code || fields["message"] != message {
		t.Errorf("error response = %v, want code %q and message %q", payload, code, message)
	}
}

func assertMiddlewareAccess(t *testing.T, record middlewareLogRecord, res *httptest.ResponseRecorder, route string, status int) {
	t.Helper()
	id := res.Header().Get("X-Request-Id")
	if id == "" || record.RequestID != id || record.Level != "INFO" || record.Route != route || record.Status != status {
		t.Errorf("access log = %+v, want INFO with request ID %q, route %q and status %d", record, id, route, status)
	}
}
