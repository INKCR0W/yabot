package httpx

import (
	"bufio"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"time"
)

// WriteError writes the shared JSON envelope. Response write failures are counted by Server's middleware.
func WriteError(w http.ResponseWriter, status int, code, message string) {
	type errorBody struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	body := struct {
		Error errorBody `json:"error"`
	}{Error: errorBody{Code: code, Message: message}}

	header := w.Header()
	header.Del("Content-Length")
	header.Set("Content-Type", "application/json; charset=utf-8")
	header.Set("Cache-Control", "no-store")
	header.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	// A string-only envelope cannot fail to encode. The writer observes I/O errors.
	json.NewEncoder(w).Encode(body)
}

type responseWriter struct {
	http.ResponseWriter
	server *Server
	body   *limitedBody
	route  string
	status int
	bytes  int
	finish func()
}

func (w *responseWriter) WriteHeader(status int) {
	final := w.status == 0 && (status == http.StatusSwitchingProtocols || status >= 200 && status <= 999)
	if final {
		w.closeUnreadBody()
	}
	w.ResponseWriter.WriteHeader(status)
	if final {
		w.status = status
	}
}

func (w *responseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.closeUnreadBody()
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += n
	if err != nil {
		w.server.stats.writeErrors.Inc()
		if isHandlerMisuse(err) {
			w.server.deps.Log.Error("HTTP handler misused the response writer", "route", w.route, "error", err)
		}
	}
	return n, err
}

func (w *responseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err == nil {
		w.body = nil
	}
	return conn, rw, err
}

func (w *responseWriter) Flush() {
	w.FlushError()
}

func (w *responseWriter) FlushError() error {
	writer := w.ResponseWriter
	for {
		switch f := writer.(type) {
		case interface{ FlushError() error }, http.Flusher:
			w.closeUnreadBody()
			err := http.NewResponseController(writer).Flush()
			if errors.Is(err, http.ErrNotSupported) {
				return err
			}
			if w.status == 0 {
				w.status = http.StatusOK
			}
			if err != nil {
				w.server.stats.writeErrors.Inc()
			}
			return err
		case interface{ Unwrap() http.ResponseWriter }:
			writer = f.Unwrap()
		default:
			return http.ErrNotSupported
		}
	}
}

func (w *responseWriter) closeUnreadBody() {
	if w.status != 0 || w.body == nil || w.body.done.Load() {
		return
	}
	w.Header().Set("Connection", "close")
	// net/http can still drain the body after flushing the response.
	err := http.NewResponseController(w.ResponseWriter).SetReadDeadline(time.Unix(0, 0))
	if err != nil && !errors.Is(err, http.ErrNotSupported) && !errors.Is(err, net.ErrClosed) {
		w.server.recordServerError("set read deadline on HTTP request with unread body failed")
	}
}

// Client disconnects are expected; these errors mean the handler broke the HTTP contract.
func isHandlerMisuse(err error) bool {
	return errors.Is(err, http.ErrContentLength) || errors.Is(err, http.ErrBodyNotAllowed) || errors.Is(err, http.ErrHijacked)
}
