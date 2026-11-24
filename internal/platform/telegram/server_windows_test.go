package telegram

import (
	"net"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
)

func newBotTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	config := net.ListenConfig{
		Control: func(network, address string, conn syscall.RawConn) error {
			// Windows can reuse an ephemeral port already bound by a loopback client.
			const exclusiveAddrUse = ^syscall.SO_REUSEADDR
			var socketErr error
			if err := conn.Control(func(fd uintptr) {
				socketErr = syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, exclusiveAddrUse, 1)
			}); err != nil {
				return err
			}
			return socketErr
		},
	}
	listener, err := config.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for fake Bot API: %v", err)
	}
	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: handler}}
	server.Start()
	return server
}
