package httpx

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAccessLogReplacesExtensionMethods(t *testing.T) {
	h := newMiddlewareHarness(Deps{})
	for _, method := range []string{http.MethodGet, "PROPFIND", strings.Repeat("X", 4096)} {
		h.serve(middlewareRequest(method, "/ready", "192.0.2.1:1234", nil))
	}
	records := h.records(t)
	if len(records) != 3 {
		t.Fatalf("access log records = %d, want 3", len(records))
	}
	for i, want := range []string{http.MethodGet, "OTHER", "OTHER"} {
		if records[i].Method != want {
			t.Errorf("record %d method = %q, want %q", i, records[i].Method, want)
		}
	}
}

func TestResponseWriterMisuseIsLoggedAsError(t *testing.T) {
	h := newMiddlewareHarness(Deps{})
	h.server.Handle("GET /short", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "1")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ab"))
	}))
	h.server.Handler().ServeHTTP(contentLengthWriter{httptest.NewRecorder()}, middlewareRequest(http.MethodGet, "/short", "192.0.2.1:1234", nil))
	h.assertCounter(t, "yabot_http_write_errors_total", 1)
	var errorsLogged int
	for _, record := range h.records(t) {
		if record.Level == "ERROR" {
			errorsLogged++
		}
	}
	if errorsLogged != 1 {
		t.Errorf("error log records = %d, want 1; logs = %s", errorsLogged, h.output.String())
	}
}

func TestGeneralOptionsRequestUsesMiddlewareChain(t *testing.T) {
	s, _ := newLifecycleServer(t, "127.0.0.1:0")
	startLifecycleServer(t, s)
	req, err := http.NewRequest(http.MethodOptions, "http://"+s.Addr().String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.URL.Opaque = "*"
	res, err := newLifecycleClient(t).Do(req)
	if err != nil {
		t.Fatalf("OPTIONS *: %v", err)
	}
	res.Body.Close()
	if res.Header.Get(requestIDHeader) == "" {
		t.Error("OPTIONS * response has no request ID, so it bypassed the middleware chain")
	}
}

// contentLengthWriter reports the error net/http returns when a handler writes past its declared length.
type contentLengthWriter struct{ http.ResponseWriter }

func (contentLengthWriter) Write([]byte) (int, error) { return 0, http.ErrContentLength }
