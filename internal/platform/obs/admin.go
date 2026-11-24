package obs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"net/netip"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

// Leave most of the process's one-second exit budget to other components.
const adminShutdownGracePeriod = 500 * time.Millisecond

const adminConnectionLimit = 16

type AdminDeps struct {
	// Listen must use the literal 127.0.0.1 and a numeric port. Port 0 selects an available port.
	Listen  string
	Metrics *metrics.Set
	Health  *Health
	Log     *slog.Logger
	// Fatal is required and receives unexpected serving errors after Serve exits.
	Fatal func(component string, err error)
}

type Admin struct {
	deps   AdminDeps
	listen func(context.Context, string, string) (net.Listener, error)

	metricsWriteErrors  *metrics.Counter
	healthWriteErrors   *metrics.Counter
	connectionsRejected *metrics.Counter
	forcedCloses        *metrics.Counter

	mu     sync.Mutex
	server *http.Server
	addr   net.Addr
	done   chan struct{}
	cancel context.CancelFunc
}

func NewAdmin(d AdminDeps) *Admin {
	var config net.ListenConfig
	admin := &Admin{deps: d, listen: config.Listen}
	if d.Metrics != nil {
		admin.metricsWriteErrors = d.Metrics.GetOrCreateCounter("yabot_admin_metrics_write_errors_total")
		admin.healthWriteErrors = d.Metrics.GetOrCreateCounter("yabot_admin_health_write_errors_total")
		admin.connectionsRejected = d.Metrics.GetOrCreateCounter("yabot_admin_connections_rejected_total")
		admin.forcedCloses = d.Metrics.GetOrCreateCounter("yabot_admin_forced_closes_total")
	}
	return admin
}

func (a *Admin) Name() string {
	return "admin"
}

// Start binds the configured address before returning. Each Admin can start successfully once.
func (a *Admin) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.deps.Metrics == nil || a.deps.Health == nil || a.deps.Log == nil || a.deps.Fatal == nil {
		return errors.New("admin server requires metrics, health, logger, and fatal callback")
	}
	address, err := netip.ParseAddrPort(a.deps.Listen)
	if err != nil {
		return fmt.Errorf("admin server listen address must have the form 127.0.0.1:port: %w", err)
	}
	if address.Addr() != netip.AddrFrom4([4]byte{127, 0, 0, 1}) {
		return errors.New("admin server must listen on the IPv4 literal 127.0.0.1")
	}

	a.mu.Lock()
	if a.server != nil {
		a.mu.Unlock()
		return errors.New("admin server has already started")
	}
	listener, err := a.listen(ctx, "tcp4", address.String())
	if err != nil {
		a.mu.Unlock()
		return fmt.Errorf("listen for admin server: %w", err)
	}
	limited := &adminListener{
		Listener: listener,
		slots:    make(chan struct{}, adminConnectionLimit),
		rejected: a.connectionsRejected,
		log:      a.deps.Log,
	}
	requestCtx, cancel := context.WithCancel(context.Background())
	// pprof extends the write deadline by the capture duration for CPU profiles and traces.
	server := &http.Server{
		Handler:           a.mux(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		ErrorLog:          slog.NewLogLogger(a.deps.Log.Handler(), slog.LevelError),
		BaseContext:       func(net.Listener) context.Context { return requestCtx },
	}
	addr := listener.Addr()
	done := make(chan struct{})
	a.server, a.addr, a.done, a.cancel = server, addr, done, cancel
	go func() {
		err := server.Serve(limited)
		// Release Stop before calling Fatal so the callback can stop the server.
		close(done)
		if !errors.Is(err, http.ErrServerClosed) {
			a.reportFatal(fmt.Errorf("serve admin server: %w", err))
		}
	}()
	a.mu.Unlock()

	a.deps.Log.Info("admin server listening", "addr", addr.String())
	return nil
}

// Stop reports a context error from forced shutdown only if the caller's context is done.
func (a *Admin) Stop(ctx context.Context) error {
	a.mu.Lock()
	server, done, cancel := a.server, a.done, a.cancel
	a.mu.Unlock()
	if server == nil {
		return nil
	}

	cancel()
	shutdownCtx, cancelShutdown := context.WithTimeout(ctx, adminShutdownGracePeriod)
	defer cancelShutdown()
	err := server.Shutdown(shutdownCtx)
	if err == nil {
		<-done
		return nil
	}

	closeErr := server.Close()
	<-done
	if err == shutdownCtx.Err() {
		a.forcedCloses.Inc()
		a.deps.Log.Info("admin server force-closed remaining connections", "reason", err)
		err = ctx.Err()
	}
	return errors.Join(err, closeErr)
}

// Addr returns nil until Start binds successfully. The returned address must not be modified.
func (a *Admin) Addr() net.Addr {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.addr
}

func (a *Admin) reportFatal(err error) {
	a.deps.Fatal(a.Name(), err)
}

type adminListener struct {
	net.Listener
	slots    chan struct{}
	rejected *metrics.Counter
	log      *slog.Logger
}

func (l *adminListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.slots <- struct{}{}:
			return &adminConn{Conn: conn, slots: l.slots}, nil
		default:
			l.rejected.Inc()
			if err := conn.Close(); err != nil {
				l.log.Error("close rejected admin connection", "error", err)
			}
		}
	}
}

type adminConn struct {
	net.Conn
	slots     <-chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func (c *adminConn) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.Conn.Close()
		<-c.slots
	})
	return c.closeErr
}

func (a *Admin) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", a.writeMetrics)
	mux.HandleFunc("GET /healthz", a.writeHealth)
	// net/http/pprof registers routes on import, but YaBot never serves DefaultServeMux.
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	return mux
}

func (a *Admin) writeMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	output := &writeErrorRecorder{Writer: w}
	a.deps.Metrics.WritePrometheus(output)
	metrics.WriteGoMetrics(output)
	metrics.WriteProcMetrics(output)
	if output.err != nil {
		a.metricsWriteErrors.Inc()
		a.deps.Log.InfoContext(r.Context(), "write admin metrics response", "error", output.err)
	}
}

type writeErrorRecorder struct {
	io.Writer
	err error
}

func (w *writeErrorRecorder) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if err != nil && w.err == nil {
		w.err = err
	}
	return n, err
}

func (a *Admin) writeHealth(w http.ResponseWriter, r *http.Request) {
	report := a.deps.Health.Check()
	response := struct {
		Healthy  bool              `json:"healthy"`
		Failures map[string]string `json:"failures"`
	}{Healthy: report.Healthy, Failures: make(map[string]string, len(report.Failures))}
	for _, failure := range report.Failures {
		response.Failures[failure.Name] = failure.Error
	}

	w.Header().Set("Content-Type", "application/json")
	if !response.Healthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		a.healthWriteErrors.Inc()
		a.deps.Log.InfoContext(r.Context(), "write admin health response", "error", err)
	}
}
