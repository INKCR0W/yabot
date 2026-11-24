package telegram

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

func TestTimeSkewEstimate(t *testing.T) {
	s := time.Second
	for _, tc := range []struct {
		name    string
		samples []time.Duration
		want    time.Duration
	}{
		{name: "no samples"},
		{name: "odd median", samples: []time.Duration{12 * s, -30 * s, 4 * s}, want: 4 * s},
		{name: "even median", samples: []time.Duration{100 * s, -200 * s, -9 * s, 16 * s}, want: 3500 * time.Millisecond},
		{name: "positive clamp", samples: []time.Duration{120 * s}, want: 60 * s},
		{name: "negative clamp", samples: []time.Duration{-120 * s}, want: -60 * s},
		{name: "clamp after averaging", samples: []time.Duration{-120 * s, 180 * s}, want: 30 * s},
	} {
		t.Run(tc.name, func(t *testing.T) {
			skew := NewTimeSkew(nil, nil)
			for _, sample := range tc.samples {
				observeSkewSample(skew, sample)
			}
			if got := skew.Skew(); got != tc.want {
				t.Errorf("Skew() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTimeSkewResponseMidpoint(t *testing.T) {
	serverTime := time.Unix(1_800_000_000, 0).UTC()
	for _, tc := range []struct {
		name string
		sent time.Duration
		recv time.Duration
		want time.Duration
	}{
		{name: "Date second midpoint", sent: 100 * time.Millisecond, recv: 900 * time.Millisecond},
		{name: "two second RTT", sent: 500 * time.Millisecond, recv: 2500 * time.Millisecond, want: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			skew := NewTimeSkew(nil, nil)
			skew.ObserveResponse(serverTime.Add(tc.sent), serverTime.Add(tc.recv), serverTime.Format(http.TimeFormat))
			if got := skew.Skew(); got != tc.want {
				t.Errorf("Skew() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTimeSkewKeepsLast64Samples(t *testing.T) {
	skew := NewTimeSkew(nil, nil)
	for _, batch := range []struct {
		count  int
		sample time.Duration
		want   time.Duration
	}{
		{64, time.Second, time.Second},
		{31, -time.Second, time.Second},
		{1, -time.Second, 0},
		{32, -time.Second, -time.Second},
	} {
		for range batch.count {
			observeSkewSample(skew, batch.sample)
		}
		if got := skew.Skew(); got != batch.want {
			t.Errorf("Skew() after %d observations of %v = %v, want %v", batch.count, batch.sample, got, batch.want)
		}
	}
}

func TestTimeSkewInvalidDatesDoNotFillWindow(t *testing.T) {
	for _, tc := range []struct {
		name  string
		dates [2]string
	}{
		{"empty first", [2]string{"", "not a date"}},
		{"malformed first", [2]string{"not a date", ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			skew := NewTimeSkew(nil, nil)
			observeSkewSample(skew, time.Second)
			now := time.Unix(1_800_000_000, 0)
			for i := range 63 {
				skew.ObserveResponse(now, now, tc.dates[i%len(tc.dates)])
			}
			observeSkewSample(skew, 3*time.Second)
			if got, want := skew.Skew(), 2*time.Second; got != want {
				t.Errorf("Skew() = %v, want %v", got, want)
			}
		})
	}
}

func TestTimeSkewInvalidDatesAndMetrics(t *testing.T) {
	var logs bytes.Buffer
	set := metrics.NewSet()
	skew := NewTimeSkew(slog.New(slog.NewJSONHandler(&logs, nil)), set)
	var exposition bytes.Buffer
	set.WritePrometheus(&exposition)
	if !strings.Contains(exposition.String(), "yabot_clock_skew_seconds 0\n") {
		t.Fatalf("initial metrics = %q, want zero skew", exposition.String())
	}

	const sample = 1250 * time.Millisecond
	observeSkewSample(skew, sample)
	now := time.Unix(1_800_000_000, 0)
	for _, date := range []string{"", "not a date"} {
		skew.ObserveResponse(now, now, date)
		if got := skew.Skew(); got != sample {
			t.Errorf("Skew() after Date %q = %v, want %v", date, got, sample)
		}
	}
	if logs.Len() != 0 {
		t.Errorf("invalid dates emitted logs: %s", &logs)
	}
	exposition.Reset()
	set.WritePrometheus(&exposition)
	for _, want := range []string{"yabot_clock_skew_seconds 1.25\n", "yabot_clock_skew_invalid_dates_total 2\n"} {
		if !strings.Contains(exposition.String(), want) {
			t.Errorf("metrics = %q, want %q", exposition.String(), want)
		}
	}
}

func TestTimeSkewAge(t *testing.T) {
	serverTime := time.Unix(1_800_000_000, 0)
	for _, tc := range []struct {
		name   string
		sample time.Duration
		recv   time.Duration
		want   time.Duration
	}{
		{name: "subtract positive skew", sample: 2 * time.Second, recv: 3500 * time.Millisecond, want: 1500 * time.Millisecond},
		{name: "subtract negative skew", sample: -2 * time.Second, recv: -500 * time.Millisecond, want: 1500 * time.Millisecond},
		{name: "clamp future update", sample: 2 * time.Second, recv: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			skew := NewTimeSkew(nil, nil)
			observeSkewSample(skew, tc.sample)
			if got := skew.Age(serverTime.Add(tc.recv), serverTime.Unix()); got != tc.want {
				t.Errorf("Age() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTimeSkewWarnsOnThresholdCrossings(t *testing.T) {
	var logs bytes.Buffer
	skew := NewTimeSkew(slog.New(slog.NewJSONHandler(&logs, nil)), nil)
	for _, tc := range []struct {
		sample time.Duration
		warns  int
	}{
		{2 * time.Second, 0},
		{3 * time.Second, 1},
		{4 * time.Second, 1},
		{0, 1},
		{-2 * time.Second, 1},
		{-3 * time.Second, 2},
		{-4 * time.Second, 2},
	} {
		for range 64 {
			observeSkewSample(skew, tc.sample)
		}
		if got := bytes.Count(logs.Bytes(), []byte("\n")); got != tc.warns {
			t.Fatalf("warning count after observing %v = %d, want %d; logs: %s", tc.sample, got, tc.warns, &logs)
		}
	}
	dec := json.NewDecoder(&logs)
	for _, want := range []float64{2.5, -2.5} {
		var entry struct {
			Level string  `json:"level"`
			Skew  float64 `json:"skew_seconds"`
		}
		if err := dec.Decode(&entry); err != nil {
			t.Fatal(err)
		}
		if entry.Level != "WARN" || entry.Skew != want {
			t.Errorf("warning = %+v, want WARN for %v seconds", entry, want)
		}
	}
}

func TestTimeSkewConcurrentObserveAndAge(t *testing.T) {
	skew := NewTimeSkew(nil, nil)
	serverTime := time.Unix(1_800_000_000, 0)
	recv := serverTime.Add(3 * time.Second)
	observeSkewSample(skew, time.Second)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			<-start
			for range 128 {
				observeSkewSample(skew, time.Second)
				if age := skew.Age(recv, serverTime.Unix()); age != 2*time.Second {
					t.Errorf("Age() = %v, want 2s", age)
				}
			}
		})
	}
	close(start)
	wg.Wait()
}

func observeSkewSample(skew *TimeSkew, sample time.Duration) {
	serverTime := time.Unix(1_800_000_000, 0).UTC()
	localTime := serverTime.Add(500*time.Millisecond + sample)
	skew.ObserveResponse(localTime, localTime, serverTime.Format(http.TimeFormat))
}
