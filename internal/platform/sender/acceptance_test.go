package sender

import (
	"cmp"
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-telegram/bot"

	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

const (
	acceptanceArrivalWindow = 10 * time.Second
	acceptanceDeadline      = 8 * time.Second
	acceptanceLatency       = 50 * time.Millisecond
	acceptanceBigChat       = -1
	acceptanceSmallTotal    = 990
	acceptanceSmallInterval = time.Second
)

func acceptanceDeps(t *testing.T, rate float64) Deps {
	t.Helper()
	d := gateRuntimeDeps(t)
	d.Workers, d.ReservedDeadline, d.ReservedDecision = 16, 6, 4
	d.CapInteractive, d.CapBackground = 4, 4
	d.GlobalRPS, d.GlobalBurst = rate, 5
	d.ClassWeights = [4]int{8, 4, 2, 1}
	d.ChatQueue, d.MaxRetries = 256, 4
	return d
}

type acceptanceArrival struct {
	chat int64
	at   time.Duration
}

type acceptanceTrafficStats struct {
	big, other, wouldMiss, busy, expired int
	startsWhileArriving                  int
	firstStart, lastStart                time.Duration
	otherDelayP99                        time.Duration
}

func acceptanceEvenTraffic() []acceptanceArrival {
	random := rand.New(rand.NewPCG(2026, 4))
	arrivals := make([]acceptanceArrival, 0, 10_000)
	for chat := int64(-2); chat >= -201; chat-- {
		for range 50 {
			arrivals = append(arrivals, acceptanceArrival{chat, time.Duration(random.Int64N(int64(acceptanceArrivalWindow)))})
		}
	}
	return arrivals
}

func acceptanceRaidTraffic() []acceptanceArrival {
	arrivals := make([]acceptanceArrival, 0, 3000+acceptanceSmallTotal)
	for i := range 3000 {
		arrivals = append(arrivals, acceptanceArrival{acceptanceBigChat, time.Duration(i) * time.Second / 300})
	}
	for chat := range 99 {
		phase := time.Duration(2*chat+1) * 5 * time.Millisecond
		for second := range 10 {
			arrivals = append(arrivals, acceptanceArrival{int64(-2 - chat), time.Duration(second)*acceptanceSmallInterval + phase})
		}
	}
	return arrivals
}

func runAcceptanceTraffic(t *testing.T, s *Sender, arrivals []acceptanceArrival) acceptanceTrafficStats {
	t.Helper()
	slices.SortStableFunc(arrivals, func(a, b acceptanceArrival) int { return cmp.Compare(a.at, b.at) })
	start := time.Now()
	type attempt struct {
		chat      int64
		at, delay time.Duration
	}
	starts := make(chan attempt, len(arrivals))
	var completed, expired atomic.Int32
	var stats acceptanceTrafficStats
	for _, arrival := range arrivals {
		at := start.Add(arrival.at)
		time.Sleep(time.Until(at))
		deadline := at.Add(acceptanceDeadline)
		err := s.Enqueue(Request{
			Class: Deadline, ChatID: arrival.chat, Method: "sendChatJoinRequestWebApp", Deadline: deadline,
			Do: func(context.Context, *bot.Bot) error {
				now := time.Now()
				if now.Before(deadline) {
					starts <- attempt{arrival.chat, now.Sub(start), now.Sub(at)}
				} else {
					t.Errorf("chat %d started at %v, deadline %v", arrival.chat, now.Sub(start), deadline.Sub(start))
				}
				time.Sleep(acceptanceLatency)
				return nil
			},
			Done: func(r Result) {
				switch r.Dropped {
				case Expired:
					if r.Outcome.Class != telegram.Retry || r.Attempts != 0 || r.Ambiguous {
						t.Errorf("chat %d result = %+v, want expiry before the first attempt", arrival.chat, r)
					}
					expired.Add(1)
				case 0:
					if r.Outcome.Class != telegram.OK || r.Attempts != 1 || r.Ambiguous || r.Err != nil {
						t.Errorf("chat %d result = %+v, want one successful attempt", arrival.chat, r)
					}
					completed.Add(1)
				default:
					t.Errorf("chat %d unexpectedly dropped: %+v", arrival.chat, r)
				}
			},
		})
		switch {
		case err == nil:
		case errors.Is(err, ErrWouldMiss):
			stats.wouldMiss++
		case errors.Is(err, ErrBusy):
			stats.busy++
		default:
			t.Fatalf("enqueue at %v: %v", arrival.at, err)
		}
	}
	time.Sleep(time.Until(start.Add(acceptanceArrivalWindow + acceptanceDeadline + acceptanceLatency)))
	synctest.Wait()
	otherDelays := make([]time.Duration, 0, len(starts))
	for range len(starts) {
		started := <-starts
		if started.chat == acceptanceBigChat {
			stats.big++
		} else {
			stats.other++
			otherDelays = append(otherDelays, started.delay)
		}
		if started.at < acceptanceArrivalWindow {
			stats.startsWhileArriving++
		}
		if stats.big+stats.other == 1 || started.at < stats.firstStart {
			stats.firstStart = started.at
		}
		stats.lastStart = max(stats.lastStart, started.at)
	}
	if len(otherDelays) > 0 {
		slices.Sort(otherDelays)
		stats.otherDelayP99 = otherDelays[(99*len(otherDelays)+99)/100-1]
	}
	stats.expired = int(expired.Load())
	if got, want := int(completed.Load()), stats.big+stats.other; got != want {
		t.Errorf("completed %d successful requests after %d starts", got, want)
	}
	if accounted := stats.big + stats.other + stats.wouldMiss + stats.busy + stats.expired; accounted != len(arrivals) {
		t.Errorf("accounted for %d of %d arrivals", accounted, len(arrivals))
	}
	return stats
}

func TestAcceptanceJoinRequestBurstUsesFullRate(t *testing.T) {
	for _, tc := range []struct {
		rate, minStarts, maxStarts int
	}{
		// With around 200 active chats, the admission estimate allows about
		// three queued entries per chat at 100/s, leaving about 600 to drain.
		{100, 1550, 1650},
		{25, 420, 455},
	} {
		t.Run(strconv.Itoa(tc.rate), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := acceptanceDeps(t, float64(tc.rate))
				s := gateRuntimeStart(t, d)
				defer gateRuntimeStop(t, s)
				stats := runAcceptanceTraffic(t, s, acceptanceEvenTraffic())
				interval := time.Second / time.Duration(tc.rate)
				duringArrivals := d.GlobalBurst + int((acceptanceArrivalWindow-stats.firstStart-time.Nanosecond)/interval)
				overObservedSpan := d.GlobalBurst + int((stats.lastStart-stats.firstStart)/interval)
				t.Logf("starts=%d/%d available tokens from %v to %v, during_arrivals=%d/%d would_miss=%d busy=%d expired=%d",
					stats.other, overObservedSpan, stats.firstStart, stats.lastStart, stats.startsWhileArriving, duringArrivals,
					stats.wouldMiss, stats.busy, stats.expired)
				if stats.other < tc.minStarts || stats.other > tc.maxStarts {
					t.Errorf("starts=%d, want %d–%d", stats.other, tc.minStarts, tc.maxStarts)
				}
				if stats.startsWhileArriving != duringArrivals || stats.other != overObservedSpan {
					t.Error("expected the arrival window and the observed send span to use every refill")
				}
			})
		})
	}
	t.Run("reservation_reuse", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			d := acceptanceDeps(t, 100)
			s := gateRuntimeStart(t, d)
			defer gateRuntimeStop(t, s)
			trigger, done := gateRuntimeRequest(Decision, -1, "getChat", time.Time{})
			trigger.Do = func(context.Context, *bot.Bot) error { return &bot.TooManyRequestsError{RetryAfter: 1} }
			gateRuntimeEnqueue(t, s, trigger)
			synctest.Wait()
			gateRuntimePending(t, done)
			// Separate wakeups force blocked scans before any new token can accrue.
			for range d.GlobalBurst - 1 {
				r, paused := gateRuntimeRequest(Decision, trigger.ChatID, trigger.Method, time.Time{})
				gateRuntimeEnqueue(t, s, r)
				synctest.Wait()
				gateRuntimePending(t, paused)
			}
			var starts atomic.Int32
			for range d.GlobalBurst - 1 {
				gateRuntimeEnqueue(t, s, Request{
					Class: Deadline, ChatID: -2, Method: "sendChatJoinRequestWebApp", Deadline: time.Now().Add(acceptanceDeadline),
					Do:   func(context.Context, *bot.Bot) error { starts.Add(1); return nil },
					Done: func(Result) {},
				})
			}
			synctest.Wait()
			t.Logf("starts before first refill=%d/%d", starts.Load(), d.GlobalBurst-1)
			if int(starts.Load()) != d.GlobalBurst-1 {
				t.Errorf("starts before first refill=%d, want %d", starts.Load(), d.GlobalBurst-1)
			}
		})
	})
}

func TestAcceptanceBigChatFloodKeepsSmallChatsOnTime(t *testing.T) {
	for _, tc := range []struct {
		rate, minSmall int
	}{
		{100, (99*acceptanceSmallTotal + 99) / 100},
		{25, 300},
	} {
		t.Run(strconv.Itoa(tc.rate), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := gateRuntimeStart(t, acceptanceDeps(t, float64(tc.rate)))
				defer gateRuntimeStop(t, s)
				stats := runAcceptanceTraffic(t, s, acceptanceRaidTraffic())
				logAcceptanceRaid(t, stats)
				if stats.other < tc.minSmall || stats.big == 0 {
					t.Errorf("small starts=%d, big starts=%d, want small >= %d and big > 0", stats.other, stats.big, tc.minSmall)
				}
				if tc.rate == 100 && stats.otherDelayP99 > acceptanceSmallInterval {
					t.Errorf("small-chat p99 start delay=%v, want at most one arrival interval (%v)", stats.otherDelayP99, acceptanceSmallInterval)
				}
			})
		})
	}
}

func logAcceptanceRaid(t *testing.T, stats acceptanceTrafficStats) {
	t.Helper()
	t.Logf("small starts=%d/990 (%.2f%%) delay p99=%v big starts=%d/3000 would_miss=%d busy=%d expired=%d",
		stats.other, 100*float64(stats.other)/acceptanceSmallTotal, stats.otherDelayP99, stats.big, stats.wouldMiss, stats.busy, stats.expired)
}

func TestAcceptanceClassWeights(t *testing.T) {
	for _, tc := range []struct {
		name    string
		weights []int
	}{
		{"two_classes", []int{8, 4}},
		{"four_classes", []int{8, 4, 2, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := gateRuntimeStart(t, acceptanceDeps(t, 100))
				defer gateRuntimeStop(t, s)
				const window = 6 * time.Second
				var starts [Background + 1]atomic.Int32
				start := time.Now()
				for i := range tc.weights {
					class := Deadline + Class(i)
					for i := range 600 {
						gateRuntimeEnqueue(t, s, Request{
							Class: class, ChatID: int64(i%4 + 1), Method: "getChat", Deadline: start.Add(time.Minute),
							Do: func(context.Context, *bot.Bot) error {
								if time.Since(start) < window {
									starts[class].Add(1)
								}
								return nil
							},
							Done: func(Result) {},
						})
					}
				}
				time.Sleep(window)
				synctest.Wait()
				counts := make([]int, len(tc.weights))
				var total, totalWeight int
				for i, weight := range tc.weights {
					class := Deadline + Class(i)
					counts[i] = int(starts[class].Load())
					total += counts[i]
					totalWeight += weight
					if s.ClassWait(class) == 0 {
						t.Errorf("%s backlog drained during the measurement window", class)
					}
				}
				t.Logf("starts by class=%v total=%d", counts, total)
				if total < 600 {
					t.Fatalf("total starts=%d, want at least 600", total)
				}
				const shareTolerance = 0.03
				for i, weight := range tc.weights {
					class := Deadline + Class(i)
					share := float64(counts[i]) / float64(total)
					want := float64(weight) / float64(totalWeight)
					if share < want-shareTolerance || share > want+shareTolerance {
						t.Errorf("%s share = %.2f%% (%d/%d), want %.2f%% ±3 points", class, 100*share, counts[i], total, 100*want)
					}
				}
			})
		})
	}
}

func TestAcceptanceWorkerReservations(t *testing.T) {
	for _, first := range []Class{Interactive, Background} {
		t.Run(first.String()+"_first", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := acceptanceDeps(t, 100)
				s := gateRuntimeStart(t, d)
				defer gateRuntimeStop(t, s)
				second := Interactive
				if first == Interactive {
					second = Background
				}
				var started, active [Background + 1]atomic.Int32
				for _, class := range []Class{first, second} {
					for i := range 8 {
						gateRuntimeEnqueue(t, s, Request{
							Class: class, ChatID: int64(i + 1), Method: "getChat", Deadline: time.Now().Add(30 * time.Second),
							Do: func(ctx context.Context, _ *bot.Bot) error {
								started[class].Add(1)
								active[class].Add(1)
								defer active[class].Add(-1)
								select {
								case <-time.After(30 * time.Second):
									return nil
								case <-ctx.Done():
									return ctx.Err()
								}
							},
							Done: func(Result) {},
						})
					}
					time.Sleep(100 * time.Millisecond)
					synctest.Wait()
				}
				if gotFirst, gotSecond := active[first].Load(), active[second].Load(); gotFirst != 4 || gotSecond != 2 {
					t.Errorf("slow calls in flight: %s=%d %s=%d, want 4 and 2", first, gotFirst, second, gotSecond)
				}

				probeAt := time.Now()
				release := make(chan struct{})
				defer close(release)
				deadlineStarts := make(chan time.Duration, d.ReservedDeadline)
				for range d.ReservedDeadline {
					gateRuntimeEnqueue(t, s, Request{
						Class: Deadline, ChatID: -1, Method: "sendChatJoinRequestWebApp", Deadline: probeAt.Add(acceptanceDeadline),
						Do: func(ctx context.Context, _ *bot.Bot) error {
							deadlineStarts <- time.Since(probeAt)
							select {
							case <-release:
								return nil
							case <-ctx.Done():
								return ctx.Err()
							}
						},
						Done: func(Result) {},
					})
				}
				const tokenInterval = 10 * time.Millisecond
				time.Sleep(time.Duration(d.ReservedDeadline) * tokenInterval)
				synctest.Wait()
				var delays []time.Duration
				for range len(deadlineStarts) {
					delays = append(delays, <-deadlineStarts)
				}
				slices.Sort(delays)
				t.Logf("slow calls in flight: Interactive=%d Background=%d, Deadline start delays=%v",
					active[Interactive].Load(), active[Background].Load(), delays)
				if len(delays) != d.ReservedDeadline || delays[0] > tokenInterval {
					t.Errorf("Deadline start delays=%v, want %d starts with first within %v", delays, d.ReservedDeadline, tokenInterval)
				}
				if active[first].Load() != 4 || active[second].Load() != 2 || started[first].Load() != 4 || started[second].Load() != 2 {
					t.Error("slow calls exceeded their cap or released capacity during the probe")
				}
			})
		})
	}
}

func TestAcceptanceRaidWeight(t *testing.T) {
	for _, rate := range []int{100, 25} {
		t.Run(strconv.Itoa(rate), func(t *testing.T) {
			var baseline acceptanceTrafficStats
			for _, variant := range []struct {
				name   string
				weight float64
			}{{"baseline", 1}, {"downweighted", 0.25}} {
				t.Run(variant.name, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						s := gateRuntimeStart(t, acceptanceDeps(t, float64(rate)))
						defer gateRuntimeStop(t, s)
						if variant.weight < 1 {
							s.SetChatWeight(acceptanceBigChat, variant.weight, time.Now().Add(time.Minute))
						}
						stats := runAcceptanceTraffic(t, s, acceptanceRaidTraffic())
						logAcceptanceRaid(t, stats)
						if variant.weight == 1 {
							baseline = stats
						} else if stats.other < baseline.other || stats.big == 0 || stats.big >= baseline.big {
							t.Errorf("weighted starts: small=%d big=%d, want small >= %d and 0 < big < %d",
								stats.other, stats.big, baseline.other, baseline.big)
						}
					})
				})
			}
		})
	}
	t.Run("interactive", func(t *testing.T) {
		baseline := acceptanceInteractiveDelays(t, 1)
		weighted := acceptanceInteractiveDelays(t, 0.25)
		t.Logf("Interactive big-chat start delays: baseline=%v weighted=%v", baseline, weighted)
		if len(baseline) != 4 || !slices.Equal(baseline, weighted) {
			t.Error("expected four identical big-chat Interactive start delays with and without downweighting")
		}
	})
}

func acceptanceInteractiveDelays(t *testing.T, weight float64) []time.Duration {
	t.Helper()
	var delays []time.Duration
	synctest.Test(t, func(t *testing.T) {
		d := acceptanceDeps(t, 100)
		s := gateRuntimeStart(t, d)
		defer gateRuntimeStop(t, s)
		if weight < 1 {
			s.SetChatWeight(acceptanceBigChat, weight, time.Now().Add(time.Minute))
		}
		// Exhaust the burst so both chats compete for replenished tokens.
		for range d.GlobalBurst {
			r, done := gateRuntimeRequest(Decision, -3, "getChat", time.Time{})
			gateRuntimeEnqueue(t, s, r)
			synctest.Wait()
			gateRuntimeResult(t, done)
		}
		queuedAt := time.Now()
		starts := make(chan time.Duration, 4)
		for range 4 {
			for _, chat := range []int64{acceptanceBigChat, -2} {
				gateRuntimeEnqueue(t, s, Request{
					Class: Interactive, ChatID: chat, Method: "getChat", Deadline: queuedAt.Add(time.Second),
					Do: func(context.Context, *bot.Bot) error {
						if chat == acceptanceBigChat {
							starts <- time.Since(queuedAt)
						}
						return nil
					},
					Done: func(Result) {},
				})
			}
		}
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		for range len(starts) {
			delays = append(delays, <-starts)
		}
	})
	return delays
}
