package httpx

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"testing"
)

func TestServerLimitsIdleConnectionsAndReleasesSlotsOnStop(t *testing.T) {
	const limit = 4
	const rejectedMetric = `yabot_http_rejected_total{reason="connections"}`
	logs := &middlewareLogBuffer{}
	s, set := newLifecycleServerWithDeps(t, Deps{
		Listen: "127.0.0.1:0", MaxInflight: 1, Log: slog.New(slog.NewJSONHandler(logs, nil)),
	})
	s.server.ReadHeaderTimeout = 2 * lifecycleWatchdog
	type event struct {
		conn  net.Conn
		state http.ConnState
	}
	states := make(chan event, 2*(limit+2))
	s.server.ConnState = func(conn net.Conn, state http.ConnState) {
		if state == http.StateNew || state == http.StateClosed {
			states <- event{conn, state}
		}
	}
	startLifecycleServer(t, s)
	ctx, cancel := context.WithTimeout(t.Context(), lifecycleWatchdog)
	defer cancel()
	waitState := func(want http.ConnState) net.Conn {
		t.Helper()
		got := waitLifecycle(t, ctx, states)
		if got.state != want {
			t.Fatalf("connection state = %v, want %v", got.state, want)
		}
		return got.conn
	}

	held := make([]net.Conn, limit)
	var first net.Conn
	for i := range held {
		held[i] = dialLifecycleConn(t, ctx, s)
		if conn := waitState(http.StateNew); i == 0 {
			first = conn
		}
	}
	assertLifecycleConnClosed(t, dialLifecycleConn(t, ctx, s))
	if got := set.GetOrCreateCounter(rejectedMetric).Get(); got != 1 {
		t.Errorf("rejected connections = %d, want 1", got)
	}
	if got := logs.String(); got != "" {
		t.Errorf("connection rejection logged %q, want no log", got)
	}

	if err := held[0].Close(); err != nil {
		t.Fatalf("close one held connection: %v", err)
	}
	if closed := waitState(http.StateClosed); closed != first {
		t.Fatal("a different connection closed")
	}
	held[0] = dialLifecycleConn(t, ctx, s)
	waitState(http.StateNew)

	stopCtx, cancelStop := context.WithCancel(ctx)
	cancelStop()
	if err := s.Stop(stopCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop = %v, want context canceled", err)
	}
	if got := set.GetOrCreateCounter("yabot_http_forced_closes_total").Get(); got != 1 {
		t.Errorf("forced closes = %d, want 1", got)
	}
	for _, conn := range held {
		assertLifecycleConnClosed(t, conn)
		waitState(http.StateClosed)
	}
	if got := len(first.(*limitedConn).listener.slots); got != 0 {
		t.Errorf("occupied connection slots after Stop = %d, want 0", got)
	}
}

func TestRepeatedConnectionCloseDoesNotReleaseAnotherSlot(t *testing.T) {
	for _, closeErr := range []error{nil, io.ErrClosedPipe} {
		name := "successful close"
		if closeErr != nil {
			name = "failed close"
		}
		t.Run(name, func(t *testing.T) {
			h := newMiddlewareHarness(Deps{})
			first := &stubCloseConn{err: closeErr}
			rejected := &stubCloseConn{err: closeErr}
			l := &limitedListener{
				Listener: &queuedListener{conns: []net.Conn{first, &stubCloseConn{}, &stubCloseConn{}, rejected}},
				slots:    make(chan struct{}, 2), server: h.server,
			}
			conn := acceptTestConn(t, l)
			acceptTestConn(t, l)
			for range 2 {
				if err := conn.Close(); !errors.Is(err, closeErr) {
					t.Errorf("Close = %v, want %v", err, closeErr)
				}
			}
			acceptTestConn(t, l)
			if _, err := l.Accept(); !errors.Is(err, net.ErrClosed) {
				t.Fatalf("Accept at capacity = %v, want listener closed after rejecting overflow", err)
			}
			if first.calls != 1 || rejected.calls != 1 {
				t.Errorf("underlying Close calls on first = %d, rejected = %d, want one each", first.calls, rejected.calls)
			}
			h.assertCounter(t, `yabot_http_rejected_total{reason="connections"}`, 1)
			if closeErr != nil {
				h.assertCounter(t, "yabot_http_server_errors_total", 2)
				for _, log := range h.records(t) {
					if log.Level != "ERROR" {
						t.Errorf("close error logged at %q, want ERROR", log.Level)
					}
				}
			}
		})
	}
}

func dialLifecycleConn(t *testing.T, ctx context.Context, s *Server) net.Conn {
	t.Helper()
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp4", s.Addr().String())
	if err != nil {
		t.Fatalf("dial HTTP server: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close client connection: %v", err)
		}
	})
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			t.Fatalf("set connection deadline: %v", err)
		}
	}
	return conn
}

func assertLifecycleConnClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	var data [1]byte
	if n, err := conn.Read(data[:]); n != 0 || err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read from closed connection = (%d, %v), want EOF or connection reset", n, err)
	}
}

type stubCloseConn struct {
	net.Conn
	err   error
	calls int
}

func (c *stubCloseConn) Close() error {
	c.calls++
	return c.err
}

type queuedListener struct {
	net.Listener
	conns []net.Conn
}

func (l *queuedListener) Accept() (net.Conn, error) {
	if len(l.conns) == 0 {
		return nil, net.ErrClosed
	}
	conn := l.conns[0]
	l.conns = l.conns[1:]
	return conn, nil
}

func acceptTestConn(t *testing.T, l net.Listener) net.Conn {
	t.Helper()
	conn, err := l.Accept()
	if err != nil {
		t.Fatalf("accept connection: %v", err)
	}
	return conn
}
