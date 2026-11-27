package httpx

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	_ "net/http/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

const lifecycleWatchdog = 5 * time.Second

func TestServerStartBindsAndOwnsRequestContext(t *testing.T) {
	s, _ := newLifecycleServer(t, "127.0.0.1:0")
	s.Handle("GET /", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Context().Err() != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	startCtx, cancelStart := context.WithCancel(t.Context())
	defer cancelStart()
	if err := s.Start(startCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	cancelStart()
	if got := s.Name(); got != "http" {
		t.Errorf("Name = %q, want http", got)
	}
	if s.Addr() == nil {
		t.Fatal("Addr is nil after Start")
	}
	if err := s.Start(t.Context()); err == nil {
		t.Error("second Start succeeded")
	}
	if got := lifecycleGet(newLifecycleClient(t), s, "/"); got.status != http.StatusNoContent || got.err != nil {
		t.Errorf("GET after canceling Start context = %+v, want 204", got)
	}
	assertLifecyclePanic(t, func() { s.Handle("GET /late", http.NotFoundHandler()) })
	if err := s.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := s.Start(t.Context()); err == nil {
		t.Error("Start after Stop succeeded")
	}
}

func TestServerStartWithCanceledContextSkipsBind(t *testing.T) {
	s, _ := newLifecycleServer(t, "127.0.0.1:0")
	s.listen = func(context.Context, string, string) (net.Listener, error) {
		t.Error("Start tried to bind with a canceled context")
		return nil, errors.New("unexpected listen")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.Stop(ctx); err != nil {
		t.Errorf("Stop before Start = %v, want nil", err)
	}
	if err := s.Start(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Start = %v, want context canceled", err)
	}
	if s.Addr() != nil {
		t.Errorf("Addr = %v after failed Start, want nil", s.Addr())
	}
}

func TestServerStartRequiresInjectedDependenciesBeforeBind(t *testing.T) {
	for _, tc := range []struct {
		name    string
		omit    func(*Deps)
		message string
	}{
		{"metrics", func(d *Deps) { d.Metrics = nil }, "metrics set"},
		{"logger", func(d *Deps) { d.Log = nil }, "logger"},
		{"fatal", func(d *Deps) { d.Fatal = nil }, "fatal callback"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := Deps{
				Listen: "127.0.0.1:0", Metrics: metrics.NewSet(), Log: slog.New(slog.DiscardHandler),
				Fatal: func(component string, err error) { t.Errorf("unexpected Fatal(%q, %v)", component, err) },
			}
			tc.omit(&deps)
			s := New(deps)
			s.listen = func(context.Context, string, string) (net.Listener, error) {
				t.Fatal("Start tried to bind without a required dependency")
				return nil, errors.New("unexpected listen")
			}
			if err := s.Start(t.Context()); err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Errorf("Start = %v, want an error mentioning %q", err, tc.message)
			}
			if addr := s.Addr(); addr != nil {
				t.Errorf("Addr = %v after failed Start, want nil", addr)
			}
			s.Handle("GET /ready", http.HandlerFunc(middlewareReady))
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, middlewareRequest(http.MethodGet, "/ready", "192.0.2.1:1234", nil))
			assertMiddlewareStatus(t, w, http.StatusNoContent)
		})
	}
}

func TestServerStartReportsAddressInUse(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy listen address: %v", err)
	}
	t.Cleanup(func() {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close occupied listener: %v", err)
		}
	})
	s, _ := newLifecycleServer(t, listener.Addr().String())
	if err := s.Start(t.Context()); err == nil {
		t.Fatal("Start succeeded on an occupied address")
	}
	if s.Addr() != nil {
		t.Errorf("Addr = %v after failed Start, want nil", s.Addr())
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("release address: %v", err)
	}
	startLifecycleServer(t, s)
}

func TestServerStopLetsActiveRequestFinish(t *testing.T) {
	s, set := newLifecycleServer(t, "127.0.0.1:0")
	listenerClosed := make(chan struct{})
	listen := s.listen
	s.listen = func(ctx context.Context, network, address string) (net.Listener, error) {
		listener, err := listen(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &lifecycleCloseSignal{Listener: listener, closed: listenerClosed}, nil
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	t.Cleanup(finish)
	handlerErr := make(chan error, 1)
	s.Handle("GET /", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		handlerErr <- r.Context().Err()
		w.WriteHeader(http.StatusNoContent)
	}))
	startLifecycleServer(t, s)
	client := newLifecycleClient(t)
	requestDone := make(chan lifecycleResult, 1)
	go func() { requestDone <- lifecycleGet(client, s, "/") }()
	ctx, cancel := context.WithTimeout(t.Context(), lifecycleWatchdog)
	defer cancel()
	waitLifecycle(t, ctx, entered)
	stopDone := make(chan error, 1)
	go func() { stopDone <- s.Stop(ctx) }()
	waitLifecycle(t, ctx, listenerClosed)
	select {
	case err := <-stopDone:
		t.Fatalf("Stop returned while request was active: %v", err)
	default:
	}
	// A concurrent Stop with an expired context must not restart or cut short the graceful drain.
	canceled, cancelSecond := context.WithCancel(ctx)
	cancelSecond()
	if err := s.Stop(canceled); !errors.Is(err, context.Canceled) {
		t.Errorf("concurrent Stop with canceled context = %v, want context.Canceled", err)
	}
	finish()
	if err := waitLifecycle(t, ctx, handlerErr); err != nil {
		t.Errorf("request context during graceful shutdown = %v, want nil", err)
	}
	if got := waitLifecycle(t, ctx, requestDone); got.status != http.StatusNoContent || got.err != nil {
		t.Errorf("active GET = %+v, want 204", got)
	}
	if err := waitLifecycle(t, ctx, stopDone); err != nil {
		t.Errorf("Stop: %v", err)
	}
	if got := set.GetOrCreateCounter("yabot_http_forced_closes_total").Get(); got != 0 {
		t.Errorf("forced closes = %d, want 0", got)
	}
}

func TestServerStopForcesCloseWhenGraceEnds(t *testing.T) {
	for _, test := range []struct {
		name string
		want error
	}{
		{name: "server grace"},
		{name: "caller deadline", want: context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, set := newLifecycleServer(t, "127.0.0.1:0")
			if test.want == nil {
				s.shutdownGrace = time.Nanosecond
			}
			entered := make(chan struct{})
			canceled := make(chan error, 1)
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			s.Handle("GET /", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-r.Context().Done()
				canceled <- r.Context().Err()
				<-release
			}))
			startLifecycleServer(t, s)
			client := newLifecycleClient(t)
			requestDone := make(chan lifecycleResult, 1)
			go func() { requestDone <- lifecycleGet(client, s, "/") }()
			ctx, cancel := context.WithTimeout(t.Context(), lifecycleWatchdog)
			defer cancel()
			waitLifecycle(t, ctx, entered)
			stopCtx := ctx
			if test.want != nil {
				var stopCancel context.CancelFunc
				stopCtx, stopCancel = context.WithDeadline(ctx, time.Unix(0, 0))
				defer stopCancel()
			}
			if err := s.Stop(stopCtx); !errors.Is(err, test.want) {
				t.Errorf("Stop = %v, want %v", err, test.want)
			}
			if err := waitLifecycle(t, ctx, canceled); !errors.Is(err, context.Canceled) {
				t.Errorf("request cancellation = %v, want context canceled", err)
			}
			if got := waitLifecycle(t, ctx, requestDone); got.err == nil || errors.Is(got.err, context.DeadlineExceeded) {
				t.Errorf("active GET = %+v, want closed connection", got)
			}
			if err := s.Stop(stopCtx); err != nil {
				t.Errorf("second Stop = %v, want nil", err)
			}
			if got := set.GetOrCreateCounter("yabot_http_forced_closes_total").Get(); got != 1 {
				t.Errorf("forced closes = %d, want 1", got)
			}
		})
	}
}

func TestServerReportsUnexpectedServeExit(t *testing.T) {
	type failure struct {
		component string
		err       error
	}
	failures := make(chan failure, 2)
	s, _ := newLifecycleServerWithDeps(t, Deps{
		Listen: "127.0.0.1:0",
		Fatal:  func(component string, err error) { failures <- failure{component, err} },
	})
	var listener net.Listener
	listen := s.listen
	s.listen = func(ctx context.Context, network, address string) (net.Listener, error) {
		var err error
		listener, err = listen(ctx, network, address)
		return listener, err
	}
	s.Handle("GET /", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	startLifecycleServer(t, s)
	if got := lifecycleGet(newLifecycleClient(t), s, "/"); got.status != http.StatusNoContent || got.err != nil {
		t.Fatalf("GET before listener close = %+v, want 204", got)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), lifecycleWatchdog)
	defer cancel()
	if got := waitLifecycle(t, ctx, failures); got.component != "http" || !errors.Is(got.err, net.ErrClosed) {
		t.Errorf("Fatal = (%q, %v), want (http, closed listener error)", got.component, got.err)
	}
	if err := s.Stop(ctx); err != nil {
		t.Errorf("Stop after Serve exit: %v", err)
	}
	select {
	case got := <-failures:
		t.Errorf("extra Fatal = (%q, %v)", got.component, got.err)
	default:
	}
}

func TestServerAbortsFlushedResponseOnPanic(t *testing.T) {
	s, set := newLifecycleServer(t, "127.0.0.1:0")
	panicNow := make(chan struct{})
	var once sync.Once
	triggerPanic := func() { once.Do(func() { close(panicNow) }) }
	t.Cleanup(triggerPanic)
	writeErr := make(chan error, 1)
	s.Handle("GET /panic", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := io.WriteString(w, "partial")
		if err == nil {
			err = http.NewResponseController(w).Flush()
		}
		writeErr <- err
		if err != nil {
			return
		}
		<-panicNow
		panic("interrupted response")
	}))
	s.Handle("GET /ready", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	startLifecycleServer(t, s)
	client := newLifecycleClient(t)
	response, err := client.Get("http://" + s.Addr().String() + "/panic")
	if err != nil {
		t.Fatalf("GET /panic before handler panicked: %v", err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), lifecycleWatchdog)
	defer cancel()
	if err := waitLifecycle(t, ctx, writeErr); err != nil {
		t.Fatalf("flush partial body: %v", err)
	}
	prefix := make([]byte, len("partial"))
	if _, err := io.ReadFull(response.Body, prefix); err != nil || string(prefix) != "partial" || response.StatusCode != http.StatusOK {
		t.Fatalf("initial response = (%d, %q, %v), want (200, partial, nil)", response.StatusCode, prefix, err)
	}
	triggerPanic()
	if tail, err := io.ReadAll(response.Body); len(tail) != 0 || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("remaining response = (%q, %v), want abrupt EOF", tail, err)
	}
	if got := set.GetOrCreateCounter("yabot_http_panics_total").Get(); got != 1 {
		t.Errorf("panics = %d, want 1", got)
	}
	if got := lifecycleGet(client, s, "/ready"); got.status != http.StatusNoContent || got.err != nil {
		t.Errorf("GET /ready after aborted response = %+v, want 204", got)
	}
}

func TestServerOptionsAndPrivateMux(t *testing.T) {
	s, _ := newLifecycleServer(t, "127.0.0.1:0")
	if v := s.server; v.ReadHeaderTimeout != 5*time.Second || v.ReadTimeout != 10*time.Second ||
		v.WriteTimeout != 15*time.Second || v.IdleTimeout != 60*time.Second || v.MaxHeaderBytes != 32<<10 || v.ErrorLog == nil || !v.DisableGeneralOptionsHandler {
		t.Errorf("server options = (header %s, read %s, write %s, idle %s, headers %d, error logger %v)",
			v.ReadHeaderTimeout, v.ReadTimeout, v.WriteTimeout, v.IdleTimeout, v.MaxHeaderBytes, v.ErrorLog)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("GET /debug/pprof/ = %d, want 404", w.Code)
	}
}

func TestServerRejectsInvalidAndDuplicatePatterns(t *testing.T) {
	s, _ := newLifecycleServer(t, "127.0.0.1:0")
	s.Handle("GET /duplicate", http.NotFoundHandler())
	for _, pattern := range []string{"GET /duplicate", "GET /broken/{"} {
		t.Run(pattern, func(t *testing.T) {
			assertLifecyclePanic(t, func() { s.Handle(pattern, http.NotFoundHandler()) })
		})
	}
}

type lifecycleCloseSignal struct {
	net.Listener
	closed chan struct{}
	once   sync.Once
}

func (l *lifecycleCloseSignal) Close() error {
	err := l.Listener.Close()
	l.once.Do(func() { close(l.closed) })
	return err
}

func newLifecycleServer(t *testing.T, address string) (*Server, *metrics.Set) {
	t.Helper()
	return newLifecycleServerWithDeps(t, Deps{Listen: address})
}

func newLifecycleServerWithDeps(t *testing.T, d Deps) (*Server, *metrics.Set) {
	t.Helper()
	d.Metrics = metrics.NewSet()
	if d.Log == nil {
		d.Log = slog.New(slog.DiscardHandler)
	}
	if d.Fatal == nil {
		d.Fatal = func(component string, err error) { t.Errorf("unexpected Fatal(%q, %v)", component, err) }
	}
	s := New(d)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), lifecycleWatchdog)
		defer cancel()
		if err := s.Stop(ctx); err != nil {
			t.Errorf("cleanup Stop: %v", err)
		}
	})
	return s, d.Metrics
}

func startLifecycleServer(t *testing.T, s *Server) {
	t.Helper()
	if err := s.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func newLifecycleClient(t *testing.T) *http.Client {
	t.Helper()
	client := &http.Client{Transport: &http.Transport{}, Timeout: lifecycleWatchdog}
	t.Cleanup(client.CloseIdleConnections)
	return client
}

type lifecycleResult struct {
	status int
	body   string
	err    error
}

func lifecycleGet(client *http.Client, s *Server, path string) lifecycleResult {
	response, err := client.Get("http://" + s.Addr().String() + path)
	if err != nil {
		return lifecycleResult{err: err}
	}
	body, err := io.ReadAll(response.Body)
	return lifecycleResult{response.StatusCode, string(body), errors.Join(err, response.Body.Close())}
}

func waitLifecycle[T any](t *testing.T, ctx context.Context, done <-chan T) T {
	t.Helper()
	select {
	case value := <-done:
		return value
	case <-ctx.Done():
		t.Fatalf("wait for HTTP lifecycle event: %v", ctx.Err())
		var zero T
		return zero
	}
}

func assertLifecyclePanic(t *testing.T, operation func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Error("route registration did not panic")
		}
	}()
	operation()
}
