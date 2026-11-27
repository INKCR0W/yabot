package httpx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"strconv"
	"sync/atomic"

	"github.com/VictoriaMetrics/metrics"
)

type contextKey uint8

const (
	idKey contextKey = iota
	responseKey
	requestIDHeader  = "X-Request-Id"
	requestBodyLimit = 64 << 10
)

// RequestID returns the generated request ID, or an empty string outside the middleware.
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(idKey).(string)
	return id
}

func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var random [8]byte
		rand.Read(random[:])
		id := hex.EncodeToString(random[:])
		w.Header().Set(requestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), idKey, id)))
	})
}

func (s *Server) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := &responseWriter{ResponseWriter: w, server: s, route: "unmatched"}
		defer func() {
			if response.finish != nil {
				defer response.finish()
			}
			value := recover()
			if value == nil {
				return
			}
			if value == http.ErrAbortHandler {
				panic(value)
			}
			s.stats.panics.Inc()
			id := w.Header().Get(requestIDHeader)
			// Panic payloads can contain request data or secrets.
			s.deps.Log.ErrorContext(r.Context(), "HTTP handler panicked",
				"request_id", id, "route", response.route,
				"panic", fmt.Sprintf("%T", value), "stack", string(debug.Stack()))
			if response.status != 0 {
				panic(http.ErrAbortHandler)
			}
			clear(w.Header())
			w.Header().Set(requestIDHeader, id)
			WriteError(response, http.StatusInternalServerError, "internal", s.internalErrorMessage(r, id, response.route))
		}()
		next.ServeHTTP(response, r)
	})
}

func (s *Server) internalErrorMessage(r *http.Request, requestID, route string) (message string) {
	const fallback = "internal error"
	defer func() {
		if value := recover(); value != nil {
			s.stats.panics.Inc()
			s.deps.Log.ErrorContext(r.Context(), "HTTP error message callback panicked",
				"request_id", requestID, "route", route,
				"panic", fmt.Sprintf("%T", value), "stack", string(debug.Stack()))
			message = fallback
		}
	}()
	return s.errorMessage(r, "internal", fallback)
}

func (s *Server) logAccess(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		begin := s.deps.Now()
		response := w.(*responseWriter)
		ctx := context.WithValue(r.Context(), responseKey, response)
		method := logMethod(r.Method)
		completed := false
		logRequest := func() {
			status := response.status
			switch {
			case !completed:
				status = http.StatusInternalServerError
			case status == 0:
				status = http.StatusOK
			}
			elapsed := max(s.deps.Now().Sub(begin), 0)
			routeLabel := "route=" + strconv.Quote(response.route)
			s.deps.Metrics.GetOrCreateCounter("yabot_http_requests_total{" +
				routeLabel + `,status="` + strconv.Itoa(status) + `"}`).Inc()
			s.deps.Metrics.GetOrCreateHistogram("yabot_http_request_duration_seconds{" +
				routeLabel + "}").Update(elapsed.Seconds())
			s.deps.Log.InfoContext(ctx, "HTTP request",
				"request_id", RequestID(ctx), "method", method, "route", response.route,
				"status", status, "bytes", response.bytes, "duration_ms", elapsed.Milliseconds())
		}
		defer func() {
			if completed {
				logRequest()
			} else {
				// Include a recovery response in the byte count when unwinding.
				response.finish = logRequest
			}
		}()
		next.ServeHTTP(w, r.WithContext(ctx))
		completed = true
	})
}

func (s *Server) limitBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := w.(*responseWriter)
		// net/http uses the original writer to close an HTTP/1 connection after overflow.
		body := &limitedBody{
			ReadCloser: http.MaxBytesReader(response.ResponseWriter, r.Body, requestBodyLimit),
			tooLarge:   s.stats.bodyTooLarge,
		}
		r.Body = body
		if r.ContentLength != 0 {
			response.body = body
		}
		next.ServeHTTP(w, r)
		// net/http can commit a default 200 after a handler returns without writing.
		response.closeUnreadBody()
	})
}

type limitedBody struct {
	io.ReadCloser
	tooLarge *metrics.Counter
	// done is set only at EOF; after an overflow the rest of the body must not be drained either.
	done       atomic.Bool
	overflowed atomic.Bool
}

func (b *limitedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.done.Store(true)
	} else if _, overflow := err.(*http.MaxBytesError); overflow && b.overflowed.CompareAndSwap(false, true) {
		b.tooLarge.Inc()
	}
	return n, err
}

func (s *Server) limitInflight(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case s.slots <- struct{}{}:
		default:
			s.stats.inflightRejected.Inc()
			WriteError(w, http.StatusServiceUnavailable, "busy", s.errorMessage(r, "busy", "server busy"))
			return
		}
		s.stats.inflight.Inc()
		defer func() {
			s.stats.inflight.Dec()
			<-s.slots
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) limitIP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.byIP.Allow(ipBucket(s.clientIP(r)), s.deps.Now()) {
			s.stats.ipRejected.Inc()
			w.Header().Set("Retry-After", "1")
			WriteError(w, http.StatusTooManyRequests, "rate_limited", s.errorMessage(r, "rate_limited", "too many requests"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Extension methods are client-controlled tokens; keep log fields small and fixed.
func logMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodOptions, http.MethodConnect, http.MethodTrace:
		return method
	}
	return "OTHER"
}
