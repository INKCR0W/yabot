package httpx

import (
	"net/http"
	"net/netip"
	"strings"
)

func (s *Server) clientIP(r *http.Request) netip.Addr {
	remote, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		s.invalidClientIP(r, "invalid HTTP peer address")
		return netip.Addr{}
	}
	peer := remote.Addr().WithZone("").Unmap()
	if !peer.IsLoopback() {
		return peer
	}

	values := r.Header.Values(s.deps.TrustedProxyHeader)
	if len(values) == 0 {
		s.stats.missingIP.Inc()
		return peer
	}
	value := values[len(values)-1]
	value = value[strings.LastIndexByte(value, ',')+1:]
	ip, err := netip.ParseAddr(strings.TrimSpace(value))
	if err != nil {
		s.invalidClientIP(r, "invalid HTTP proxy address")
		return peer
	}
	return ip.WithZone("").Unmap()
}

func (s *Server) invalidClientIP(r *http.Request, message string) {
	s.stats.invalidIP.Inc()
	s.deps.Log.ErrorContext(r.Context(), message, "request_id", RequestID(r.Context()))
}

func ipBucket(ip netip.Addr) netip.Addr {
	if ip.Is6() {
		return netip.PrefixFrom(ip, 64).Masked().Addr()
	}
	return ip
}
