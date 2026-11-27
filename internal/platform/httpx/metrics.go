package httpx

import "github.com/VictoriaMetrics/metrics"

type httpMetrics struct {
	inflight            *metrics.Gauge
	inflightRejected    *metrics.Counter
	ipRejected          *metrics.Counter
	connectionsRejected *metrics.Counter
	bodyTooLarge        *metrics.Counter
	panics              *metrics.Counter
	evictions           *metrics.Counter
	missingIP           *metrics.Counter
	invalidIP           *metrics.Counter
	writeErrors         *metrics.Counter
	forcedCloses        *metrics.Counter
	abandonedHandlers   *metrics.Counter
	serverErrors        *metrics.Counter
}

func newHTTPMetrics(set *metrics.Set) httpMetrics {
	return httpMetrics{
		inflight:            set.GetOrCreateGauge("yabot_http_inflight", nil),
		inflightRejected:    set.GetOrCreateCounter(`yabot_http_rejected_total{reason="inflight"}`),
		ipRejected:          set.GetOrCreateCounter(`yabot_http_rejected_total{reason="ip_rate"}`),
		connectionsRejected: set.GetOrCreateCounter(`yabot_http_rejected_total{reason="connections"}`),
		bodyTooLarge:        set.GetOrCreateCounter("yabot_http_body_too_large_total"),
		panics:              set.GetOrCreateCounter("yabot_http_panics_total"),
		evictions:           set.GetOrCreateCounter("yabot_http_ip_limiter_evictions_total"),
		missingIP:           set.GetOrCreateCounter(`yabot_http_client_ip_fallback_total{reason="missing"}`),
		invalidIP:           set.GetOrCreateCounter(`yabot_http_client_ip_fallback_total{reason="invalid"}`),
		writeErrors:         set.GetOrCreateCounter("yabot_http_write_errors_total"),
		forcedCloses:        set.GetOrCreateCounter("yabot_http_forced_closes_total"),
		abandonedHandlers:   set.GetOrCreateCounter("yabot_http_stop_abandoned_handlers_total"),
		serverErrors:        set.GetOrCreateCounter("yabot_http_server_errors_total"),
	}
}
