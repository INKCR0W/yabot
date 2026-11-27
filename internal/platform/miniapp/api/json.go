package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/INKCR0W/yabot/internal/platform/i18n"
)

// WriteJSON encodes before committing the response. Encoding failures produce a 500
// with the default English message.
func (a *API) WriteJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		a.encodingErrors.Inc()
		// Custom marshalers can include response data in their error text.
		a.log.Error("failed to encode Mini App JSON response")
		a.writeError(w, i18n.En, http.StatusInternalServerError, CodeInternal)
		return
	}

	header := w.Header()
	header.Del("Content-Length")
	header.Set("Content-Type", "application/json; charset=utf-8")
	header.Set("Cache-Control", "no-store")
	header.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	// The httpx response writer observes I/O errors.
	w.Write(body)
}

// DecodeJSON accepts one JSON value and rejects unknown struct fields. If the body
// limit was reached while reading, it returns the *http.MaxBytesError.
func DecodeJSON(r *http.Request, v any) error {
	if r.Body == nil {
		return io.EOF
	}
	body := &overflowWatch{reader: r.Body}
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	err := decoder.Decode(v)
	if err == nil {
		var extra json.RawMessage
		switch err = decoder.Decode(&extra); err {
		case io.EOF:
			err = nil
		case nil:
			err = errors.New("request body must contain exactly one JSON value")
		}
	}
	// The decoder can finish a value from buffered bytes after the reader reported the overflow.
	if body.overflow != nil {
		return body.overflow
	}
	return err
}

type overflowWatch struct {
	reader   io.Reader
	overflow *http.MaxBytesError
}

func (w *overflowWatch) Read(p []byte) (int, error) {
	n, err := w.reader.Read(p)
	if overflow, ok := errors.AsType[*http.MaxBytesError](err); ok {
		w.overflow = overflow
	}
	return n, err
}
