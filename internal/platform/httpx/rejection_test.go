package httpx

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestRejectedUploadsReleaseConnectionSlots(t *testing.T) {
	const ready = "GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n"
	const upload = "POST /upload HTTP/1.1\r\nHost: localhost\r\n"
	for _, tc := range []struct {
		name            string
		status          int
		connectionLimit int
		partialBody     string
	}{
		{"IP rate limit with content length", http.StatusTooManyRequests, 1, "Content-Length: 4\r\n\r\nx"},
		{"inflight limit with chunked body", http.StatusServiceUnavailable, 2, "Transfer-Encoding: chunked\r\n\r\n4\r\nx"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, set := newLifecycleServerWithDeps(t, Deps{
				Listen: "127.0.0.1:0", MaxInflight: 1, IPBurst: 1,
				Now: func() time.Time { return time.Unix(1, 0) },
			})
			closed := watchLimitedLifecycleConnections(s, tc.connectionLimit)
			ctx, cancel := context.WithTimeout(t.Context(), lifecycleWatchdog)
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			s.Handle("GET /hold", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				close(entered)
				select {
				case <-release:
					w.WriteHeader(http.StatusNoContent)
				case <-ctx.Done():
				}
			}))
			s.Handle("GET /ready", http.HandlerFunc(middlewareReady))
			startLifecycleServer(t, s)

			var held net.Conn
			if tc.status == http.StatusServiceUnavailable {
				held = dialLifecycleConn(t, ctx, s)
				writeLifecycleRequest(t, held, "GET /hold HTTP/1.1\r\nHost: localhost\r\n\r\n")
				waitLifecycle(t, ctx, entered)
			}
			conn := dialLifecycleConn(t, ctx, s)
			reader := bufio.NewReader(conn)
			if held == nil {
				writeLifecycleRequest(t, conn, ready)
				readLifecycleResponse(t, reader, http.StatusNoContent)
			}

			writeLifecycleRequest(t, conn, upload+"Content-Length: 0\r\n\r\n")
			if readLifecycleResponse(t, reader, tc.status).Close {
				t.Fatal("rejection without a body closed the connection")
			}
			writeLifecycleRequest(t, conn, ready)
			readLifecycleResponse(t, reader, tc.status)
			assertPartialUploadCloses(t, conn, reader, upload+tc.partialBody, tc.status, closed)

			if held != nil {
				close(release)
				readLifecycleResponse(t, bufio.NewReader(held), http.StatusNoContent)
			}
			next := dialLifecycleConn(t, ctx, s)
			writeLifecycleRequest(t, next, "GET /ready HTTP/1.1\r\nHost: localhost\r\nX-Forwarded-For: 192.0.2.1\r\n\r\n")
			readLifecycleResponse(t, bufio.NewReader(next), http.StatusNoContent)
			if got := set.GetOrCreateCounter(`yabot_http_rejected_total{reason="connections"}`).Get(); got != 0 {
				t.Errorf("connection rejections = %d, want none", got)
			}
		})
	}
}

func TestEarlyResponsesReleaseConnectionSlots(t *testing.T) {
	s, set := newLifecycleServer(t, "127.0.0.1:0")
	closed := watchLimitedLifecycleConnections(s, 1)
	s.Handle("GET /ready", http.HandlerFunc(middlewareReady))
	s.Handle("POST /bad-request", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	s.Handle("POST /panic", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("handler failed")
	}))
	s.Handle("POST /write", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := io.WriteString(w, "ok"); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	s.Handle("POST /flush", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("flush response: %v", err)
		}
	}))
	s.Handle("POST /empty", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	startLifecycleServer(t, s)

	for _, tc := range []struct {
		name, path string
		status     int
	}{
		{"bad_request", "/bad-request", http.StatusBadRequest},
		{"not_found", "/missing", http.StatusNotFound},
		{"panic_recovery", "/panic", http.StatusInternalServerError},
		{"implicit_write", "/write", http.StatusOK},
		{"flush", "/flush", http.StatusOK},
		{"handler_return", "/empty", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), lifecycleWatchdog)
			defer cancel()
			conn := dialLifecycleConn(t, ctx, s)
			request := "POST " + tc.path + " HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r\nx"
			assertPartialUploadCloses(t, conn, bufio.NewReader(conn), request, tc.status, closed)
		})
	}
	ctx, cancel := context.WithTimeout(t.Context(), lifecycleWatchdog)
	defer cancel()
	next := dialLifecycleConn(t, ctx, s)
	writeLifecycleRequest(t, next, "GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n")
	readLifecycleResponse(t, bufio.NewReader(next), http.StatusNoContent)
	if got := set.GetOrCreateCounter(`yabot_http_rejected_total{reason="connections"}`).Get(); got != 0 {
		t.Errorf("connection rejections = %d, want none", got)
	}
}

func TestCompleteOrAbsentRequestBodiesKeepConnectionAlive(t *testing.T) {
	s, _ := newLifecycleServer(t, "127.0.0.1:0")
	s.Handle("POST /upload", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil || n != 4 {
			t.Errorf("read request body: got %d bytes and %v, want 4 bytes and no error", n, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	s.Handle("GET /ready", http.HandlerFunc(middlewareReady))
	startLifecycleServer(t, s)
	ctx, cancel := context.WithTimeout(t.Context(), lifecycleWatchdog)
	defer cancel()
	conn := dialLifecycleConn(t, ctx, s)
	reader := bufio.NewReader(conn)
	const upload = "POST /upload HTTP/1.1\r\nHost: localhost\r\n"
	const ready = "GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n"
	for _, tc := range []struct {
		name, request string
	}{
		{"no body", ready},
		{"complete body with content length", upload + "Content-Length: 4\r\n\r\ndata"},
		{"complete chunked body", upload + "Transfer-Encoding: chunked\r\n\r\n4\r\ndata\r\n0\r\n\r\n"},
		{"follow-up request", ready},
	} {
		writeLifecycleRequest(t, conn, tc.request)
		if readLifecycleResponse(t, reader, http.StatusNoContent).Close {
			t.Fatalf("response to %s closed the connection", tc.name)
		}
	}
}

func watchLimitedLifecycleConnections(s *Server, limit int) <-chan net.Conn {
	listen := s.listen
	s.listen = func(ctx context.Context, network, address string) (net.Listener, error) {
		listener, err := listen(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &limitedListener{Listener: listener, slots: make(chan struct{}, limit), server: s}, nil
	}
	closed := make(chan net.Conn, limit+1)
	s.server.ConnState = func(conn net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			closed <- conn
		}
	}
	return closed
}

func assertPartialUploadCloses(t *testing.T, conn net.Conn, reader *bufio.Reader, request string, status int, closed <-chan net.Conn) {
	t.Helper()
	promptCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	deadline, _ := promptCtx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		t.Fatalf("set upload deadline: %v", err)
	}
	writeLifecycleRequest(t, conn, request)
	if !readLifecycleResponse(t, reader, status).Close {
		t.Error("response to partial upload did not signal connection closure")
	}
	assertLifecycleConnClosed(t, conn)
	// A TCP FIN can arrive before net/http closes the socket and releases the slot.
	if got := waitLifecycle(t, promptCtx, closed); got.RemoteAddr().String() != conn.LocalAddr().String() {
		t.Fatal("a different connection closed")
	}
}

func writeLifecycleRequest(t *testing.T, conn net.Conn, request string) {
	t.Helper()
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatalf("write HTTP request: %v", err)
	}
}

func readLifecycleResponse(t *testing.T, reader *bufio.Reader, status int) *http.Response {
	t.Helper()
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatalf("read HTTP response, want status %d: %v", status, err)
	}
	_, err = io.Copy(io.Discard, response.Body)
	if err := errors.Join(err, response.Body.Close()); err != nil {
		t.Fatalf("read HTTP response body: %v", err)
	}
	if response.StatusCode != status {
		t.Fatalf("HTTP status = %d, want %d", response.StatusCode, status)
	}
	return response
}
