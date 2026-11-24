package telegram

import (
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

const (
	skewWindowSize    = 64
	maxSkew           = 60 * time.Second
	skewWarnThreshold = 2 * time.Second
)

// TimeSkew estimates local wall time minus Telegram wall time. It is safe for concurrent use and must not be copied.
type TimeSkew struct {
	log          *slog.Logger
	invalidDates *metrics.Counter

	mu      sync.Mutex
	samples [skewWindowSize]time.Duration
	next    int
	count   int
	skew    time.Duration
}

// NewTimeSkew registers metrics in set, if provided. A nil logger discards warnings.
func NewTimeSkew(log *slog.Logger, set *metrics.Set) *TimeSkew {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	t := &TimeSkew{log: log}
	if set != nil {
		set.NewGauge("yabot_clock_skew_seconds", func() float64 { return t.Skew().Seconds() })
		t.invalidDates = set.NewCounter("yabot_clock_skew_invalid_dates_total")
	}
	return t
}

func (t *TimeSkew) ObserveResponse(sentWall, recvWall time.Time, httpDate string) {
	serverWall, err := http.ParseTime(httpDate)
	if err != nil {
		if t.invalidDates != nil {
			t.invalidDates.Inc()
		}
		return
	}

	// HTTP Date uses wall time, rounded down to the start of the second.
	sentWall = sentWall.Round(0)
	recvWall = recvWall.Round(0)
	midpoint := sentWall.Add(recvWall.Sub(sentWall) / 2)
	sample := midpoint.Sub(serverWall.Add(500 * time.Millisecond))

	t.mu.Lock()
	t.samples[t.next] = sample
	t.next = (t.next + 1) % len(t.samples)
	t.count = min(t.count+1, len(t.samples))
	sorted := t.samples
	slices.Sort(sorted[:t.count])
	estimate := sorted[t.count/2]
	if t.count%2 == 0 {
		// Average the two central samples without overflowing duration arithmetic.
		lower := sorted[t.count/2-1]
		if lower < 0 && estimate >= 0 {
			estimate = (lower + estimate) / 2
		} else {
			estimate = lower + (estimate-lower)/2
		}
	}
	skew := min(max(estimate, -maxSkew), maxSkew)
	crossedThreshold := t.skew >= -skewWarnThreshold && t.skew <= skewWarnThreshold &&
		(skew < -skewWarnThreshold || skew > skewWarnThreshold)
	t.skew = skew
	t.mu.Unlock()

	if crossedThreshold {
		t.log.Warn("Telegram clock skew exceeds threshold", "skew_seconds", skew.Seconds())
	}
}

// Age returns the elapsed time since tgDate, a Unix timestamp in seconds, using the current skew estimate.
func (t *TimeSkew) Age(recvWall time.Time, tgDate int64) time.Duration {
	return max(0, recvWall.Round(0).Add(-t.Skew()).Sub(time.Unix(tgDate, 0)))
}

// Skew returns zero until a valid sample has been observed.
func (t *TimeSkew) Skew() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.skew
}
