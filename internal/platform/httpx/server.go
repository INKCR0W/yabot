package httpx

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

const connectionLimitMultiplier = 4

type Deps struct {
	// Listen must use the IPv4 literal 127.0.0.1 and a numeric port.
	Listen string
	// TrustedProxyHeader is read only from loopback peers, using its rightmost value. The reverse
	// proxy must set or append it from the connection peer; a header it forwards unchanged is spoofable.
	TrustedProxyHeader string
	MaxInflight        int
	IPRPS              float64
	IPBurst            int
	// Metrics and Log are required by Start.
	Metrics *metrics.Set
	Log     *slog.Logger
	// Fatal is required by Start and receives unexpected serving errors.
	Fatal func(component string, err error)
	Now   func() time.Time
}

type Server struct {
	deps   Deps
	mux    *http.ServeMux
	slots  chan struct{}
	byIP   *KeyedLimiter[netip.Addr]
	stats  httpMetrics
	server *http.Server

	hasMetrics    bool
	hasLogger     bool
	listen        func(context.Context, string, string) (net.Listener, error)
	shutdownGrace time.Duration

	mu         sync.Mutex
	addr       net.Addr
	serveDone  chan struct{}
	cancelBase context.CancelFunc
	stopDone   chan struct{}
}

// New uses the documented HTTP defaults for zero-valued options.
func New(d Deps) *Server {
	hasMetrics, hasLogger := d.Metrics != nil, d.Log != nil
	if d.Listen == "" {
		d.Listen = "127.0.0.1:8080"
	}
	if d.TrustedProxyHeader == "" {
		d.TrustedProxyHeader = "X-Forwarded-For"
	}
	if d.MaxInflight == 0 {
		d.MaxInflight = 256
	}
	if d.IPRPS == 0 {
		d.IPRPS = 20
	}
	if d.IPBurst == 0 {
		d.IPBurst = 40
	}
	if d.Metrics == nil {
		d.Metrics = metrics.NewSet()
	}
	if d.Log == nil {
		d.Log = slog.New(slog.DiscardHandler)
	}
	if d.Now == nil {
		d.Now = time.Now
	}

	var listenConfig net.ListenConfig
	stats := newHTTPMetrics(d.Metrics)
	const ipLimiterCapacity = 10_000
	s := &Server{
		deps:          d,
		mux:           http.NewServeMux(),
		slots:         make(chan struct{}, d.MaxInflight),
		byIP:          NewKeyedLimiter[netip.Addr](d.IPRPS, d.IPBurst, ipLimiterCapacity, stats.evictions.Inc),
		stats:         stats,
		hasMetrics:    hasMetrics,
		hasLogger:     hasLogger,
		listen:        listenConfig.Listen,
		shutdownGrace: 5 * time.Second,
	}
	s.server = &http.Server{
		Handler:           s.recoverPanics(withRequestID(s.logAccess(s.limitBody(s.limitInflight(s.limitIP(s.mux)))))),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    32 << 10,
		// Send "OPTIONS *" through the middleware chain instead of the built-in reply.
		DisableGeneralOptionsHandler: true,
		ErrorLog:                     log.New(serverErrorLog{s}, "", 0),
	}
	return s
}

func (s *Server) Name() string {
	return "http"
}

// Handle registers a Go ServeMux pattern. Registration panics after a successful Start.
// Handlers must read the request body to EOF before starting the final response to keep the connection open.
func (s *Server) Handle(pattern string, h http.Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.addr != nil {
		panic("cannot register an HTTP route after the server has started")
	}
	if h == nil {
		panic("cannot register a nil HTTP handler")
	}
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		if response, ok := r.Context().Value(responseKey).(*responseWriter); ok {
			response.route = r.Pattern
		}
		h.ServeHTTP(w, r)
	})
}

func (s *Server) Handler() http.Handler {
	return s.server.Handler
}

// Addr returns nil until Start binds successfully. Callers must treat the address as read-only.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

// Start binds synchronously and can succeed once. The supplied context only governs startup.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.addr != nil {
		return errors.New("HTTP server has already started")
	}
	switch {
	case !s.hasMetrics:
		return errors.New("HTTP server requires a metrics set")
	case !s.hasLogger:
		return errors.New("HTTP server requires a logger")
	case s.deps.Fatal == nil:
		return errors.New("HTTP server requires a fatal callback")
	}
	if s.deps.MaxInflight > math.MaxInt/connectionLimitMultiplier {
		return errors.New("HTTP server connection limit exceeds the supported range")
	}
	address, err := netip.ParseAddrPort(s.deps.Listen)
	if err != nil || address.Addr() != netip.AddrFrom4([4]byte{127, 0, 0, 1}) {
		return errors.New("HTTP server listen address must be in 127.0.0.1:port form")
	}
	listener, err := s.listen(ctx, "tcp4", address.String())
	if err != nil {
		return fmt.Errorf("listen for HTTP server: %w", err)
	}
	base, cancel := context.WithCancel(context.Background())
	s.server.BaseContext = func(net.Listener) context.Context { return base }
	done := make(chan struct{})
	s.addr, s.cancelBase, s.serveDone = listener.Addr(), cancel, done
	go s.serve(&limitedListener{
		Listener: listener,
		slots:    make(chan struct{}, connectionLimitMultiplier*s.deps.MaxInflight),
		server:   s,
	}, done)
	return nil
}

func (s *Server) serve(listener net.Listener, done chan struct{}) {
	err := s.server.Serve(listener)
	// A Fatal callback can call Stop.
	close(done)
	if !errors.Is(err, http.ErrServerClosed) {
		s.stats.serverErrors.Inc()
		s.deps.Fatal(s.Name(), fmt.Errorf("serve HTTP server: %w", err))
	}
}

// Stop drains requests for up to five seconds, bounded by ctx, then cancels and closes remaining connections.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.addr == nil {
		s.mu.Unlock()
		return nil
	}
	if done := s.stopDone; done != nil {
		s.mu.Unlock()
		select {
		case <-done:
			return nil
		default:
		}
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan struct{})
	s.stopDone = done
	s.mu.Unlock()
	defer close(done)

	shutdownCtx, cancelShutdown := context.WithTimeout(ctx, s.shutdownGrace)
	defer cancelShutdown()
	err := s.server.Shutdown(shutdownCtx)
	s.cancelBase()
	if err == nil {
		<-s.serveDone
		return nil
	}
	if err := s.server.Close(); err != nil {
		s.recordServerError("HTTP server close failed")
	}
	if err == shutdownCtx.Err() {
		s.stats.forcedCloses.Inc()
		s.deps.Log.Info("HTTP server force-closed remaining connections", "reason", err)
	} else {
		s.recordServerError("HTTP server shutdown failed")
	}
	<-s.serveDone
	return ctx.Err()
}

func (s *Server) recordServerError(message string) {
	s.stats.serverErrors.Inc()
	s.deps.Log.Error(message)
}

type serverErrorLog struct {
	server *Server
}

func (l serverErrorLog) Write(p []byte) (int, error) {
	// net/http diagnostics can include unsanitized request or connection details.
	l.server.recordServerError("net/http server reported an error")
	return len(p), nil
}
