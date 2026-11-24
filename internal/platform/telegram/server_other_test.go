//go:build !windows

package telegram

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func newBotTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	return httptest.NewServer(handler)
}
