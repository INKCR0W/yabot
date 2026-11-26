package sender

import (
	"time"

	"github.com/VictoriaMetrics/metrics"

	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

type senderMetrics struct {
	set            *metrics.Set
	depth          [Background + 1]*metrics.Gauge
	busy           [Background + 1]*metrics.Gauge
	latency        [Background + 1]*metrics.Histogram
	chatPaused     *metrics.Gauge
	classPaused    *metrics.Gauge
	globalPaused   *metrics.Gauge
	callbackPanics *metrics.Counter
}

func newSenderMetrics(set *metrics.Set) senderMetrics {
	m := senderMetrics{set: set}
	if set == nil {
		return m
	}
	for class := Deadline; class <= Background; class++ {
		label := `{class="` + class.String() + `"}`
		m.depth[class] = set.GetOrCreateGauge("yabot_sender_queue_depth"+label, nil)
		m.busy[class] = set.GetOrCreateGauge("yabot_sender_workers_busy"+label, nil)
		m.latency[class] = set.GetOrCreateHistogram("yabot_sender_latency_seconds" + label)
	}
	m.chatPaused = set.GetOrCreateGauge(`yabot_sender_gate_paused{scope="chat"}`, nil)
	m.classPaused = set.GetOrCreateGauge(`yabot_sender_gate_paused{scope="class"}`, nil)
	m.globalPaused = set.GetOrCreateGauge(`yabot_sender_gate_paused{scope="global"}`, nil)
	m.callbackPanics = set.GetOrCreateCounter("yabot_sender_callback_panics_total")
	return m
}

func (m *senderMetrics) setDepth(class Class, depth int) {
	if m.set != nil {
		m.depth[class].Set(float64(depth))
	}
}

func (m *senderMetrics) setBusy(class Class, busy int) {
	if m.set != nil {
		m.busy[class].Set(float64(busy))
	}
}

func (m *senderMetrics) setGate(counts gateCounts) {
	m.chatPaused.Set(float64(counts.chats))
	m.classPaused.Set(float64(counts.methods))
	m.globalPaused.Set(float64(counts.global))
}

func (m *senderMetrics) countAttempt(r Request, outcome telegram.Class) {
	if m.set != nil {
		_, label := methodInfo(r.Method)
		m.set.GetOrCreateCounter(`yabot_sender_requests_total{method="` + label + `",class="` +
			r.Class.String() + `",outcome="` + outcome.String() + `"}`).Inc()
	}
}

func (m *senderMetrics) countResult(class Class, reason DropReason, latency time.Duration) {
	if reason != 0 {
		m.countDrop(class, reason.String())
		return
	}
	if m.set != nil {
		m.latency[class].Update(latency.Seconds())
	}
}

func (m *senderMetrics) countDrop(class Class, reason string) {
	if m.set != nil {
		m.set.GetOrCreateCounter(`yabot_sender_dropped_total{class="` + class.String() +
			`",reason="` + reason + `"}`).Inc()
	}
}

func (m *senderMetrics) countCallbackPanic() {
	if m.set != nil {
		m.callbackPanics.Inc()
	}
}
