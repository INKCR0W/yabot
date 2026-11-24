package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	runtimemetrics "runtime/metrics"
	"runtime/trace"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

func TestAdminMetricsAndPprof(t *testing.T) {
	const metricName = "yabot_admin_test_total"
	const defaultMetricName = "yabot_admin_test_default_set_only_total"
	set := metrics.NewSet()
	set.NewCounter(metricName).Inc()
	defaultSet := metrics.GetDefaultSet()
	defaultSet.NewCounter(defaultMetricName).Inc()
	t.Cleanup(func() {
		if !defaultSet.UnregisterMetric(defaultMetricName) {
			t.Errorf("unregister default set metric %q: metric not found", defaultMetricName)
		}
	})
	pushTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(pushTarget.Close)
	pushCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := set.PushMetrics(pushCtx, pushTarget.URL, nil); err != nil {
		t.Fatalf("seed push metrics: %v", err)
	}
	var pushMetrics bytes.Buffer
	metrics.WritePushMetrics(&pushMetrics)
	if !bytes.Contains(pushMetrics.Bytes(), []byte(`metrics_push_total{url="`+pushTarget.URL+`"}`)) {
		t.Fatal("push metrics fixture did not register a counter")
	}
	admin := startTestAdmin(t, set, NewHealth())
	client := newAdminTestClient()

	metricLines := []string{metricName + " 1\n", "yabot_admin_metrics_write_errors_total 0\n", "go_goroutines "}
	switch runtime.GOOS {
	case "linux", "windows":
		metricLines = append(metricLines, "process_resident_memory_bytes ", "process_cpu_seconds_total ")
	}
	for _, tc := range []struct {
		path        string
		status      int
		contentType string
		contains    []string
	}{
		{"/metrics", http.StatusOK, "text/plain; version=0.0.4; charset=utf-8", metricLines},
		{"/debug/pprof/", http.StatusOK, "text/html; charset=utf-8", []string{"heap?debug=1"}},
		{"/debug/pprof/cmdline", http.StatusOK, "text/plain; charset=utf-8", nil},
		{"/debug/pprof/symbol", http.StatusOK, "text/plain; charset=utf-8", []string{"num_symbols: 1\n"}},
		{"/debug/pprof/heap?debug=1", http.StatusOK, "text/plain; charset=utf-8", []string{"heap profile:"}},
		{"/debug/pprof/goroutine?debug=2", http.StatusOK, "text/plain; charset=utf-8", nil},
		{"/debug/pprof/unknown", http.StatusNotFound, "text/plain; charset=utf-8", nil},
		{"/debug/pprof/mutex?seconds=0", http.StatusBadRequest, "text/plain; charset=utf-8", nil},
		{"/debug/pprof/profile?seconds=1", http.StatusOK, "application/octet-stream", nil},
	} {
		t.Run(tc.path, func(t *testing.T) {
			resp, body := getAdmin(t, client, admin, tc.path)
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.status)
			}
			if got := resp.Header.Get("Content-Type"); got != tc.contentType {
				t.Errorf("Content-Type = %q, want %q", got, tc.contentType)
			}
			if len(body) == 0 {
				t.Error("response body is empty")
			}
			for _, want := range tc.contains {
				if !bytes.Contains(body, []byte(want)) {
					t.Errorf("response does not contain %q", want)
				}
			}
			if tc.path == "/metrics" {
				for _, unwanted := range []string{"metrics_push_", defaultMetricName} {
					if bytes.Contains(body, []byte(unwanted)) {
						t.Errorf("response contains %q", unwanted)
					}
				}
			}
		})
	}
}

func TestAdminPprofSnapshotOptions(t *testing.T) {
	admin := startTestAdmin(t, metrics.NewSet(), NewHealth())
	client := newAdminTestClient()

	t.Run("heap gc", func(t *testing.T) {
		samples := []runtimemetrics.Sample{{Name: "/gc/cycles/forced:gc-cycles"}}
		runtimemetrics.Read(samples)
		before := samples[0].Value.Uint64()
		resp, body := getAdmin(t, client, admin, "/debug/pprof/heap?gc=1")
		if resp.StatusCode != http.StatusOK || len(body) == 0 {
			t.Fatalf("heap profile returned status %d and %d bytes", resp.StatusCode, len(body))
		}
		runtimemetrics.Read(samples)
		if got := samples[0].Value.Uint64(); got <= before {
			t.Errorf("forced GC cycles = %d, want an increase from %d", got, before)
		}
	})

	t.Run("mutex delta", func(t *testing.T) {
		resp, body := getAdmin(t, client, admin, "/debug/pprof/mutex?seconds=1")
		if resp.StatusCode != http.StatusOK || len(body) == 0 {
			t.Fatalf("mutex delta returned status %d and %d bytes", resp.StatusCode, len(body))
		}
		if got := resp.Header.Get("Content-Disposition"); got != `attachment; filename="mutex-delta"` {
			t.Errorf("Content-Disposition = %q, want a mutex-delta attachment", got)
		}
	})
}

func TestAdminLogsResponseWriteErrorsOnce(t *testing.T) {
	for _, tc := range []struct {
		path    string
		counter string
	}{
		{"/metrics", "yabot_admin_metrics_write_errors_total"},
		{"/healthz", "yabot_admin_health_write_errors_total"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			var logs bytes.Buffer
			set := metrics.NewSet()
			deps := adminTestDeps(t, set, NewHealth())
			deps.Log = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			admin := NewAdmin(deps)
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			admin.mux().ServeHTTP(failingAdminResponseWriter{httptest.NewRecorder()}, req)

			var entry struct {
				Level string `json:"level"`
				Error string `json:"error"`
			}
			if err := json.Unmarshal(logs.Bytes(), &entry); err != nil {
				t.Fatalf("want exactly one log entry, got %q: %v", logs.String(), err)
			}
			if entry.Level != "INFO" || entry.Error != io.ErrClosedPipe.Error() {
				t.Errorf("log entry = %+v, want INFO with error %q", entry, io.ErrClosedPipe)
			}
			if got := set.GetOrCreateCounter(tc.counter).Get(); got != 1 {
				t.Errorf("%s = %d, want 1", tc.counter, got)
			}
		})
	}
}

type failingAdminResponseWriter struct {
	http.ResponseWriter
}

func (failingAdminResponseWriter) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func TestAdminHealth(t *testing.T) {
	health := NewHealth()
	admin := startTestAdmin(t, metrics.NewSet(), health)
	client := newAdminTestClient()

	for _, tc := range []struct {
		name     string
		check    func() error
		status   int
		failures map[string]string
	}{
		{"healthy", nil, http.StatusOK, map[string]string{}},
		{"unhealthy", func() error { return errors.New("database unavailable") }, http.StatusServiceUnavailable, map[string]string{"store": "database unavailable"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.check != nil {
				health.Register("store", tc.check)
			}
			resp, body := getAdmin(t, client, admin, "/healthz")
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.status)
			}
			if got := resp.Header.Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", got)
			}
			var report struct {
				Healthy  *bool             `json:"healthy"`
				Failures map[string]string `json:"failures"`
			}
			if err := json.Unmarshal(body, &report); err != nil {
				t.Fatalf("decode health response: %v", err)
			}
			if want := tc.status == http.StatusOK; report.Healthy == nil || *report.Healthy != want {
				t.Errorf("healthy field in %s, want %t", body, want)
			}
			if report.Failures == nil || !maps.Equal(report.Failures, tc.failures) {
				t.Errorf("failures = %#v, want %#v", report.Failures, tc.failures)
			}
		})
	}
}

func TestAdminStartRejectsInvalidDependencies(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*AdminDeps)
		want   string
	}{
		{"missing metrics", func(d *AdminDeps) { d.Metrics = nil }, "requires"},
		{"missing health", func(d *AdminDeps) { d.Health = nil }, "requires"},
		{"missing logger", func(d *AdminDeps) { d.Log = nil }, "requires"},
		{"missing fatal callback", func(d *AdminDeps) { d.Fatal = nil }, "fatal callback"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := adminTestDeps(t, metrics.NewSet(), NewHealth())
			tc.change(&deps)
			assertAdminRejectsBeforeListen(t, deps, tc.want)
		})
	}
}

func TestAdminStartRejectsInvalidListenAddress(t *testing.T) {
	for _, address := range []string{
		"0.0.0.0:0", ":0", "127.0.0.2:0", "localhost:0", "[::1]:0", "[::ffff:127.0.0.1]:0",
		"[127.0.0.1]:0", "127.0.0.1", "127.0.0.1:", "127.0.0.1:http", "127.0.0.1:65536",
	} {
		t.Run(address, func(t *testing.T) {
			deps := adminTestDeps(t, metrics.NewSet(), NewHealth())
			deps.Listen = address
			assertAdminRejectsBeforeListen(t, deps, "127.0.0.1")
		})
	}
}

func assertAdminRejectsBeforeListen(t *testing.T, deps AdminDeps, want string) {
	t.Helper()
	admin := NewAdmin(deps)
	admin.listen = func(context.Context, string, string) (net.Listener, error) {
		t.Fatal("listener called despite invalid dependencies")
		return nil, errors.New("unexpected listen")
	}
	if err := admin.Start(t.Context()); err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Start = %v, want an error mentioning %q", err, want)
	}
}

func TestAdminStartOnOccupiedPort(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	})
	deps := adminTestDeps(t, metrics.NewSet(), NewHealth())
	deps.Listen = listener.Addr().String()
	admin := NewAdmin(deps)
	t.Cleanup(func() { stopTestAdmin(t, admin) })
	var listenErr *net.OpError
	if err := admin.Start(t.Context()); !errors.As(err, &listenErr) || listenErr.Op != "listen" {
		t.Fatalf("Start = %v, want a listener error for the occupied port", err)
	}
	if got := admin.Addr(); got != nil {
		t.Errorf("Addr after failed Start = %v, want nil", got)
	}
}

func TestAdminReportsUnexpectedServeError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wantErr := errors.New("permanent accept error")
		release := make(chan struct{})
		type fatalCall struct {
			component string
			err       error
		}
		var calls []fatalCall
		deps := adminTestDeps(t, metrics.NewSet(), NewHealth())
		deps.Fatal = func(component string, err error) {
			calls = append(calls, fatalCall{component, err})
		}
		admin := NewAdmin(deps)
		admin.listen = func(context.Context, string, string) (net.Listener, error) {
			return failingAdminListener{release, wantErr}, nil
		}
		if err := admin.Start(t.Context()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		close(release)
		synctest.Wait()

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := admin.Stop(ctx); err != nil {
			t.Errorf("Stop after Serve failure: %v", err)
		}
		synctest.Wait()
		if len(calls) != 1 {
			t.Fatalf("Fatal called %d times, want 1", len(calls))
		}
		if got := calls[0]; got.component != "admin" || !errors.Is(got.err, wantErr) {
			t.Errorf("Fatal(%q, %v), want admin and an error wrapping %v", got.component, got.err, wantErr)
		}
	})
}

func TestAdminFatalCanStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		release := make(chan struct{})
		stopped := make(chan error, 1)
		var calls int
		var admin *Admin
		deps := adminTestDeps(t, metrics.NewSet(), NewHealth())
		deps.Fatal = func(string, error) {
			calls++
			err := admin.Stop(ctx)
			if calls == 1 {
				stopped <- err
			}
		}
		admin = NewAdmin(deps)
		admin.listen = func(context.Context, string, string) (net.Listener, error) {
			return failingAdminListener{release, errors.New("permanent accept error")}, nil
		}
		if err := admin.Start(t.Context()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		close(release)

		select {
		case err := <-stopped:
			if err != nil {
				t.Errorf("Stop inside Fatal: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("Stop did not return from inside Fatal before the deadline")
		}
		synctest.Wait()
		if calls != 1 {
			t.Errorf("Fatal called %d times, want 1", calls)
		}
	})
}

type failingAdminListener struct {
	release <-chan struct{}
	err     error
}

func (l failingAdminListener) Accept() (net.Conn, error) {
	<-l.release
	return nil, l.err
}

func (failingAdminListener) Close() error { return nil }

func (failingAdminListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9090}
}

func TestAdminStop(t *testing.T) {
	t.Run("never started", func(t *testing.T) {
		stopTestAdmin(t, NewAdmin(AdminDeps{}))
	})
	t.Run("started", func(t *testing.T) {
		admin := startTestAdmin(t, metrics.NewSet(), NewHealth())
		address := admin.Addr().String()
		stopTestAdmin(t, admin)
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err == nil {
			if err := conn.Close(); err != nil {
				t.Errorf("close connection: %v", err)
			}
			t.Fatalf("admin listener %s still accepts connections", address)
		}
		stopTestAdmin(t, admin)
	})
}

func TestAdminConnectionLimit(t *testing.T) {
	set := metrics.NewSet()
	admin := NewAdmin(adminTestDeps(t, set, NewHealth()))
	startAccepting := make(chan struct{})
	listen := admin.listen
	admin.listen = func(ctx context.Context, network, address string) (net.Listener, error) {
		listener, err := listen(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return gatedAdminListener{listener, startAccepting}, nil
	}
	if err := admin.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	states := make(chan http.ConnState, 2*(adminConnectionLimit+2))
	admin.server.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew || state == http.StateClosed {
			states <- state
		}
	}
	close(startAccepting)
	t.Cleanup(func() { stopTestAdmin(t, admin) })

	deadline := time.Now().Add(5 * time.Second)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	var dialer net.Dialer
	dial := func() net.Conn {
		conn, err := dialer.DialContext(ctx, "tcp", admin.Addr().String())
		if err != nil {
			t.Fatalf("dial admin: %v", err)
		}
		t.Cleanup(func() {
			if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("close connection: %v", err)
			}
		})
		return conn
	}
	waitForState := func(want http.ConnState) {
		select {
		case got := <-states:
			if got != want {
				t.Fatalf("connection state = %v, want %v", got, want)
			}
		case <-ctx.Done():
			t.Fatalf("wait for connection state %v: %v", want, ctx.Err())
		}
	}

	held := make([]net.Conn, adminConnectionLimit)
	for i := range held {
		held[i] = dial()
		waitForState(http.StateNew)
	}
	overflow := dial()
	if err := overflow.SetReadDeadline(deadline); err != nil {
		t.Fatalf("set overflow connection deadline: %v", err)
	}
	var response [1]byte
	if n, err := overflow.Read(response[:]); n != 0 || err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("read from overflow connection = (%d, %v), want a closed connection", n, err)
	}
	if got := set.GetOrCreateCounter("yabot_admin_connections_rejected_total").Get(); got != 1 {
		t.Errorf("rejected connections = %d, want 1", got)
	}

	if err := held[0].Close(); err != nil {
		t.Fatalf("close a held connection: %v", err)
	}
	waitForState(http.StateClosed)
	if resp, _ := getAdmin(t, newAdminTestClient(), admin, "/healthz"); resp.StatusCode != http.StatusOK {
		t.Errorf("health status after freeing a connection = %d, want 200", resp.StatusCode)
	}
}

type gatedAdminListener struct {
	net.Listener
	startAccepting <-chan struct{}
}

func (l gatedAdminListener) Accept() (net.Conn, error) {
	<-l.startAccepting
	return l.Listener.Accept()
}

func TestAdminStopWithIncompleteRequest(t *testing.T) {
	accepted := make(chan struct{}, 1)
	set := metrics.NewSet()
	admin := NewAdmin(adminTestDeps(t, set, NewHealth()))
	listen := admin.listen
	admin.listen = func(ctx context.Context, network, address string) (net.Listener, error) {
		listener, err := listen(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return acceptingAdminListener{listener, accepted}, nil
	}
	if err := admin.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { stopTestAdmin(t, admin) })

	deadline := time.Now().Add(5 * time.Second)
	setupCtx, cancelSetup := context.WithDeadline(t.Context(), deadline)
	defer cancelSetup()
	var dialer net.Dialer
	conn, err := dialer.DialContext(setupCtx, "tcp", admin.Addr().String())
	if err != nil {
		t.Fatalf("dial admin: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close connection: %v", err)
		}
	}()
	if err := conn.SetDeadline(deadline); err != nil {
		t.Fatalf("set connection deadline: %v", err)
	}
	if _, err := io.WriteString(conn, "GET /debug/pprof/trace?seconds=30 HTTP/1.1\r\nHost: x\r\n"); err != nil {
		t.Fatalf("write incomplete request: %v", err)
	}
	select {
	case <-accepted:
	case <-setupCtx.Done():
		t.Fatal("admin did not accept the connection")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	start := time.Now()
	if err := admin.Stop(ctx); err != nil {
		t.Fatalf("Stop with an incomplete request: %v", err)
	}
	if got := set.GetOrCreateCounter("yabot_admin_forced_closes_total").Get(); got != 1 {
		t.Errorf("forced closes = %d, want 1", got)
	}
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("Stop with an incomplete request took %s, want under 1s", elapsed)
	}
	var response [1]byte
	if n, err := conn.Read(response[:]); n != 0 || err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("read after Stop = (%d, %v), want a closed connection", n, err)
	}
}

type acceptingAdminListener struct {
	net.Listener
	accepted chan<- struct{}
}

func (l acceptingAdminListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		select {
		case l.accepted <- struct{}{}:
		default:
		}
	}
	return conn, err
}

func TestAdminStopCancelsActiveTrace(t *testing.T) {
	if trace.IsEnabled() {
		t.Skip("runtime tracing is already enabled")
	}
	admin := startTestAdmin(t, metrics.NewSet(), NewHealth())
	client := newAdminTestClient()
	traceURL := "http://" + admin.Addr().String() + "/debug/pprof/trace?seconds=30"
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, traceURL, nil)
	if err != nil {
		t.Fatalf("create trace request: %v", err)
	}

	type traceResult struct {
		status int
		size   int64
		err    error
	}
	result := make(chan traceResult, 1)
	go func() {
		resp, err := client.Do(req)
		if err != nil {
			result <- traceResult{err: err}
			return
		}
		size, readErr := io.Copy(io.Discard, resp.Body)
		result <- traceResult{resp.StatusCode, size, errors.Join(readErr, resp.Body.Close())}
	}()

	watchdog, cancelWatchdog := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelWatchdog()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for !trace.IsEnabled() {
		select {
		case got := <-result:
			t.Fatalf("trace request finished before tracing started: %+v", got)
		case <-watchdog.Done():
			t.Fatal("trace request did not start capturing")
		case <-poll.C:
		}
	}

	conflict, body := getAdmin(t, client, admin, "/debug/pprof/trace?seconds=1")
	if conflict.StatusCode != http.StatusInternalServerError || !bytes.Contains(body, []byte("already enabled")) {
		t.Errorf("overlapping trace returned %d with body %q, want 500 with an explanation", conflict.StatusCode, body)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := admin.Stop(ctx); err != nil {
		t.Fatalf("Stop during an active trace after %s: %v", time.Since(start), err)
	}
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Errorf("Stop during an active trace took %s, want under 1s", elapsed)
	}
	if trace.IsEnabled() {
		t.Error("tracing is still enabled after Stop")
	}
	select {
	case got := <-result:
		if got.err != nil || got.status != http.StatusOK || got.size == 0 {
			t.Errorf("trace response after Stop = %+v, want 200 with trace data", got)
		}
	case <-watchdog.Done():
		t.Fatal("trace request did not finish after Stop")
	}
}

func TestAdminStopForcesCloseOnDeadline(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	set := metrics.NewSet()
	set.RegisterMetricsWriter(func(io.Writer) {
		close(entered)
		<-release
	})
	admin := startTestAdmin(t, set, NewHealth())
	defer close(release)

	address := "http://" + admin.Addr().String() + "/metrics"
	requestErr := make(chan error, 1)
	go func() {
		resp, err := newAdminTestClient().Get(address)
		if err == nil {
			_, readErr := io.Copy(io.Discard, resp.Body)
			err = errors.Join(readErr, resp.Body.Close())
		}
		requestErr <- err
	}()

	watchdog, cancelWatchdog := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelWatchdog()
	select {
	case <-entered:
	case err := <-requestErr:
		t.Fatalf("request finished before it reached the metrics writer: %v", err)
	case <-watchdog.Done():
		t.Fatal("request did not reach the metrics writer")
	}

	deadline, cancelDeadline := context.WithDeadline(t.Context(), time.Now())
	defer cancelDeadline()
	if err := admin.Stop(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Stop = %v, want context deadline exceeded", err)
	}
	if got := set.GetOrCreateCounter("yabot_admin_forced_closes_total").Get(); got != 1 {
		t.Errorf("forced closes = %d, want 1", got)
	}
	select {
	case err := <-requestErr:
		if err == nil || errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("request error = %v, want a closed connection", err)
		}
	case <-watchdog.Done():
		t.Fatal("Stop left the active connection open")
	}
}

func startTestAdmin(t *testing.T, set *metrics.Set, health *Health) *Admin {
	t.Helper()
	admin := NewAdmin(adminTestDeps(t, set, health))
	if err := admin.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { stopTestAdmin(t, admin) })
	if admin.Addr() == nil {
		t.Fatal("Addr is nil after successful Start")
	}
	return admin
}

func stopTestAdmin(t *testing.T, admin *Admin) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := admin.Stop(ctx); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

func adminTestDeps(t *testing.T, set *metrics.Set, health *Health) AdminDeps {
	return AdminDeps{
		Listen:  "127.0.0.1:0",
		Metrics: set,
		Health:  health,
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Fatal: func(component string, err error) {
			t.Errorf("unexpected Fatal(%q, %v)", component, err)
		},
	}
}

func newAdminTestClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
		Timeout:   5 * time.Second,
	}
}

func getAdmin(t *testing.T, client *http.Client, admin *Admin, path string) (*http.Response, []byte) {
	t.Helper()
	resp, err := client.Get("http://" + admin.Addr().String() + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	body, readErr := io.ReadAll(resp.Body)
	if err := errors.Join(readErr, resp.Body.Close()); err != nil {
		t.Fatalf("read GET %s: %v", path, err)
	}
	return resp, body
}
