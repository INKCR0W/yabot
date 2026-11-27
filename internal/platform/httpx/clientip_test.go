package httpx

import (
	"net"
	"net/http"
	"strings"
	"testing"
)

func TestClientIPTrustsOnlyLastAddressFromLoopback(t *testing.T) {
	const loopback = "127.0.0.1:4321"
	for _, tc := range []struct {
		name           string
		peer           string
		configured     string
		forwarded      []string
		referencePeer  string
		fallbackReason string
	}{
		{name: "single item", peer: loopback, forwarded: []string{"192.0.2.20"}, referencePeer: "192.0.2.20:1234"},
		{name: "comma separated", peer: loopback, forwarded: []string{"192.0.2.21, 192.0.2.22"}, referencePeer: "192.0.2.22:1234"},
		{name: "multiple header lines", peer: loopback, forwarded: []string{"192.0.2.23", "192.0.2.24, 192.0.2.25"}, referencePeer: "192.0.2.25:1234"},
		{name: "whitespace", peer: loopback, forwarded: []string{"\t 192.0.2.26  "}, referencePeer: "192.0.2.26:1234"},
		{name: "IPv6", peer: "[::1]:4321", forwarded: []string{"2001:db8:3:4::1"}, referencePeer: "[2001:db8:3:4::2]:1234"},
		{name: "IPv6 with zone", peer: loopback, forwarded: []string{"fe80::42%zone-secret"}, referencePeer: "[fe80::43]:1234"},
		{name: "mapped IPv4 header", peer: loopback, forwarded: []string{"::ffff:192.0.2.27"}, referencePeer: "192.0.2.27:1234"},
		{name: "mapped loopback peer", peer: "[::ffff:127.0.0.1]:4321", forwarded: []string{"192.0.2.28"}, referencePeer: "192.0.2.28:1234"},
		{name: "custom header", peer: loopback, configured: "X-Proxy-Client", forwarded: []string{"192.0.2.29"}, referencePeer: "192.0.2.29:1234"},
		{name: "invalid rightmost item", peer: loopback, forwarded: []string{"192.0.2.30, malformed-header-secret"}, referencePeer: loopback, fallbackReason: "invalid"},
		{name: "empty final header line", peer: loopback, forwarded: []string{"192.0.2.31", ""}, referencePeer: loopback, fallbackReason: "invalid"},
		{name: "trailing comma", peer: loopback, forwarded: []string{"192.0.2.32,"}, referencePeer: loopback, fallbackReason: "invalid"},
		{name: "missing header", peer: loopback, referencePeer: loopback, fallbackReason: "missing"},
		{name: "untrusted peer spoof", peer: "192.0.2.33:4321", forwarded: []string{"192.0.2.34"}, referencePeer: "192.0.2.33:1234"},
		{name: "untrusted peer malformed header", peer: "192.0.2.35:4321", forwarded: []string{"untrusted-header-secret"}, referencePeer: "192.0.2.35:1234"},
		{name: "invalid peer", peer: "malformed-peer-secret", forwarded: []string{"192.0.2.36"}, referencePeer: "another-malformed-peer", fallbackReason: "invalid"},
		{name: "invalid peer port", peer: "192.0.2.37:bad-port-secret", forwarded: []string{"192.0.2.38"}, referencePeer: "another-malformed-peer", fallbackReason: "invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newMiddlewareHarness(Deps{IPBurst: 1, TrustedProxyHeader: tc.configured})
			h.server.Handle("GET /ready", http.HandlerFunc(middlewareReady))
			req := middlewareRequest(http.MethodGet, "/ready", tc.peer, nil)
			header := "X-Forwarded-For"
			if tc.configured != "" {
				header = tc.configured
				req.Header.Add("X-Forwarded-For", "192.0.2.200")
			}
			for _, line := range tc.forwarded {
				req.Header.Add(header, line)
			}
			first := h.serve(req)
			assertMiddlewareStatus(t, first, http.StatusNoContent)

			var missing, invalid uint64
			switch tc.fallbackReason {
			case "missing":
				missing = 1
			case "invalid":
				invalid = 1
			}
			h.assertCounter(t, `yabot_http_client_ip_fallback_total{reason="missing"}`, missing)
			h.assertCounter(t, `yabot_http_client_ip_fallback_total{reason="invalid"}`, invalid)
			var accessLogs, errorLogs uint64
			for _, record := range h.records(t) {
				switch record.Level {
				case "INFO":
					accessLogs++
					assertMiddlewareAccess(t, record, first, "GET /ready", http.StatusNoContent)
				case "ERROR":
					errorLogs++
				default:
					t.Errorf("unexpected log: %+v", record)
				}
			}
			if accessLogs != 1 || errorLogs != invalid {
				t.Errorf("logs contained %d access and %d error records, want 1 and %d", accessLogs, errorLogs, invalid)
			}
			assertClientIPLogPrivacy(t, h.output.String(), tc.peer, tc.forwarded)

			probe := h.serve(middlewareRequest(http.MethodGet, "/ready", tc.referencePeer, nil))
			assertMiddlewareStatus(t, probe, http.StatusTooManyRequests)
			other := h.serve(middlewareRequest(http.MethodGet, "/ready", "198.51.100.1:1234", nil))
			assertMiddlewareStatus(t, other, http.StatusNoContent)
		})
	}
}

func assertClientIPLogPrivacy(t *testing.T, output, peer string, forwarded []string) {
	t.Helper()
	if host, _, err := net.SplitHostPort(peer); err == nil {
		peer = host
	}
	secrets := []string{peer}
	for _, line := range forwarded {
		for item := range strings.SplitSeq(line, ",") {
			secrets = append(secrets, strings.TrimSpace(item))
		}
	}
	for _, secret := range secrets {
		if secret != "" && strings.Contains(output, secret) {
			t.Errorf("logs contain private peer or forwarded value %q", secret)
		}
	}
}
