package config

import (
	"bytes"
	"encoding/base64"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func validConfigForValidation() Config {
	c := Default()
	c.Telegram.Token = newSecret("test-token")
	c.Turnstile.SiteKey = "test-site-key"
	c.Turnstile.Secret = newSecret("test-turnstile-secret")
	c.Security.HMACKey = newSecret(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	return c
}

func TestValidateRejectsInvalidFields(t *testing.T) {
	cases := []struct {
		field  string
		change func(*Config)
	}{
		{"telegram.token", func(c *Config) { c.Telegram.Token = Secret{} }},
		{"telegram.api_server", func(c *Config) { c.Telegram.APIServer = "ftp://example.com" }},
		{"telegram.poll_timeout", func(c *Config) { c.Telegram.PollTimeout = 0 }},
		{"telegram.settle_timeout", func(c *Config) { c.Telegram.SettleTimeout = -1 }},
		{"telegram.transient_budget", func(c *Config) { c.Telegram.TransientBudget = 0 }},
		{"telegram.poison_threshold", func(c *Config) { c.Telegram.PoisonThreshold = 0 }},
		{"http.listen", func(c *Config) { c.HTTP.Listen = "127.0.0.1" }},
		{"http.public_base_url", func(c *Config) { c.HTTP.PublicBaseURL = "http://yabot.example.com" }},
		{"http.trusted_proxy_header", func(c *Config) { c.HTTP.TrustedProxyHeader = " " }},
		{"http.max_inflight", func(c *Config) { c.HTTP.MaxInflight = -1 }},
		{"http.ip_rps", func(c *Config) { c.HTTP.IPRPS = math.Inf(1) }},
		{"http.ip_burst", func(c *Config) { c.HTTP.IPBurst = 0 }},
		{"http.user_rps", func(c *Config) { c.HTTP.UserRPS = math.NaN() }},
		{"http.user_burst", func(c *Config) { c.HTTP.UserBurst = -1 }},
		{"admin.listen", func(c *Config) { c.Admin.Listen = "0.0.0.0:9090" }},
		{"log.level", func(c *Config) { c.Log.Level = "trace" }},
		{"log.format", func(c *Config) { c.Log.Format = "xml" }},
		{"store.dir", func(c *Config) { c.Store.Dir = "" }},
		{"store.cache_kib", func(c *Config) { c.Store.CacheKiB = -1 }},
		{"store.checkpoint_interval", func(c *Config) { c.Store.CheckpointInterval = 0 }},
		{"store.write_queue", func(c *Config) { c.Store.WriteQueue = -1 }},
		{"store.backup_keep", func(c *Config) { c.Store.BackupKeep = 0 }},
		{"dispatch.shards", func(c *Config) { c.Dispatch.Shards = -1 }},
		{"dispatch.inbox", func(c *Config) { c.Dispatch.Inbox = 0 }},
		{"dispatch.tickets_per_shard", func(c *Config) { c.Dispatch.TicketsPerShard = -1 }},
		{"dispatch.mailbox_per_entity", func(c *Config) { c.Dispatch.MailboxPerEntity = 0 }},
		{"sender.workers", func(c *Config) { c.Sender.Workers = 0 }},
		{"sender.reserved.deadline", func(c *Config) { c.Sender.Reserved.Deadline = 0 }},
		{"sender.reserved.decision", func(c *Config) { c.Sender.Reserved.Decision = -1 }},
		{"sender.cap.interactive", func(c *Config) { c.Sender.Cap.Interactive = 0 }},
		{"sender.cap.background", func(c *Config) { c.Sender.Cap.Background = -1 }},
		{"sender.global_rps", func(c *Config) { c.Sender.GlobalRPS = 0 }},
		{"sender.global_burst", func(c *Config) { c.Sender.GlobalBurst = -1 }},
		{"sender.class_weights.deadline", func(c *Config) { c.Sender.ClassWeights.Deadline = 0 }},
		{"sender.class_weights.decision", func(c *Config) { c.Sender.ClassWeights.Decision = -1 }},
		{"sender.class_weights.interactive", func(c *Config) { c.Sender.ClassWeights.Interactive = 0 }},
		{"sender.class_weights.background", func(c *Config) { c.Sender.ClassWeights.Background = -1 }},
		{"sender.chat_queue", func(c *Config) { c.Sender.ChatQueue = 0 }},
		{"sender.raid_weight", func(c *Config) { c.Sender.RaidWeight = 0 }},
		{"sender.raid_weight", func(c *Config) { c.Sender.RaidWeight = 1.01 }},
		{"sender.max_retries", func(c *Config) { c.Sender.MaxRetries = 0 }},
		{"guard.present_budget", func(c *Config) { c.Guard.PresentBudget = -1 }},
		{"guard.backlog_cutoff", func(c *Config) { c.Guard.BacklogCutoff = 0 }},
		{"guard.max_active_per_chat", func(c *Config) { c.Guard.MaxActivePerChat = -1 }},
		{"guard.max_active_total", func(c *Config) { c.Guard.MaxActiveTotal = 0 }},
		{"guard.raid_chat_rps", func(c *Config) { c.Guard.RaidChatRPS = -1 }},
		{"guard.raid_global_wait", func(c *Config) { c.Guard.RaidGlobalWait = 0 }},
		{"guard.recent_closed_ttl", func(c *Config) { c.Guard.RecentClosedTTL = -1 }},
		{"turnstile.site_key", func(c *Config) { c.Turnstile.SiteKey = "" }},
		{"turnstile.secret", func(c *Config) { c.Turnstile.Secret = Secret{} }},
		{"turnstile.hostname", func(c *Config) { c.Turnstile.Hostname = "" }},
		{"turnstile.timeout", func(c *Config) { c.Turnstile.Timeout = 0 }},
		{"turnstile.concurrency", func(c *Config) { c.Turnstile.Concurrency = -1 }},
		{"security.hmac_key", func(c *Config) { c.Security.HMACKey = Secret{} }},
		{"security.initdata_max_age", func(c *Config) { c.Security.InitDataMaxAge = 0 }},
		{"tenant.idle_leave_after", func(c *Config) { c.Tenant.IdleLeaveAfter = -1 }},
		{"tenant.probe_periodic", func(c *Config) { c.Tenant.ProbePeriodic = 0 }},
		{"tenant.probe_periodic_rps", func(c *Config) { c.Tenant.ProbePeriodicRPS = -1 }},
		{"retention.guard_history", func(c *Config) { c.Retention.GuardHistory = 0 }},
		{"retention.settings_audit", func(c *Config) { c.Retention.SettingsAudit = -1 }},
		{"retention.dead_letter", func(c *Config) { c.Retention.DeadLetter = 0 }},
		{"retention.removed_chat", func(c *Config) { c.Retention.RemovedChat = -1 }},
		{"runtime.memory_limit_mib", func(c *Config) { c.Runtime.MemoryLimitMiB = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			c := validConfigForValidation()
			tc.change(&c)
			requireValidation(t, c, tc.field+": ")
		})
	}
}

func TestValidateFieldBoundaries(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Config)
		want   string
	}{
		{"poll exactly 50 seconds", func(c *Config) { c.Telegram.PollTimeout = Duration(50 * time.Second) }, ""},
		{"poll above 50 seconds", func(c *Config) { c.Telegram.PollTimeout = Duration(51 * time.Second) }, "telegram.poll_timeout: must be <= 50s"},
		{"poll fractional seconds", func(c *Config) { c.Telegram.PollTimeout = Duration(1500 * time.Millisecond) }, "telegram.poll_timeout: must be a whole number of seconds"},
		{"Telegram must be absolute", func(c *Config) { c.Telegram.APIServer = "//example.com" }, "telegram.api_server: "},
		{"public URL must be absolute", func(c *Config) { c.HTTP.PublicBaseURL = "/app" }, "http.public_base_url: "},
		{"public URL must have a hostname", func(c *Config) { c.HTTP.PublicBaseURL = "https://:443/" }, "http.public_base_url: "},
		{"admin custom port", func(c *Config) { c.Admin.Listen = "127.0.0.1:9191" }, ""},
		{"admin missing port", func(c *Config) { c.Admin.Listen = "127.0.0.1" }, "admin.listen: "},
		{"raid weight one", func(c *Config) { c.Sender.RaidWeight = 1 }, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfigForValidation()
			tc.change(&c)
			requireValidation(t, c, tc.want)
		})
	}
}

func TestValidateMemoryLimitMiBBoundary(t *testing.T) {
	if strconv.IntSize == 32 {
		t.Skip("memory limit boundary exceeds the range of a 32-bit int")
	}
	const maxMemoryLimitMiB = math.MaxInt64 / (1 << 20)
	cases := []struct {
		name     string
		limitMiB int64
		want     string
	}{
		{"at limit", maxMemoryLimitMiB, ""},
		{"above limit", maxMemoryLimitMiB + 1, "runtime.memory_limit_mib: "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfigForValidation()
			c.Runtime.MemoryLimitMiB = int(tc.limitMiB)
			requireValidation(t, c, tc.want)
			if tc.want == "" {
				if got := c.MemoryLimitBytes(); got <= 0 {
					t.Errorf("MemoryLimitBytes() = %d, want a positive value", got)
				}
			}
		})
	}
}

func TestValidateListeners(t *testing.T) {
	listeners := []struct {
		field string
		set   func(*Config, string)
	}{
		{"http.listen", func(c *Config, value string) { c.HTTP.Listen = value }},
		{"admin.listen", func(c *Config, value string) {
			c.HTTP.Listen = "127.0.0.1:8081"
			c.Admin.Listen = value
		}},
	}
	cases := []struct {
		addr string
		want string
	}{
		{"0.0.0.0:8080", "host must be the IPv4 literal 127.0.0.1"},
		{":8080", "host must be the IPv4 literal 127.0.0.1"},
		{"[::]:8080", "host must be the IPv4 literal 127.0.0.1"},
		{"192.0.2.1:8080", "host must be the IPv4 literal 127.0.0.1"},
		{"localhost:8080", "host must be the IPv4 literal 127.0.0.1"},
		{"127.0.0.1:8080", ""},
		{"127.1.2.3:8080", "host must be the IPv4 literal 127.0.0.1"},
		{"[::1]:8080", "host must be the IPv4 literal 127.0.0.1"},
		{"[::ffff:127.0.0.1]:8080", "host must be the IPv4 literal 127.0.0.1"},
		{"[127.0.0.1]:8080", "host must be the IPv4 literal 127.0.0.1"},
		{"127.0.0.1:1", ""},
		{"127.0.0.1:65535", ""},
		{"127.0.0.1:65536", "port must be a decimal number"},
		{"127.0.0.1:0", "port must be a decimal number"},
		{"127.0.0.1:http", "port must be a decimal number"},
		{"127.0.0.1:", "port must be a decimal number"},
		{"127.0.0.1:+8080", "port must be a decimal number"},
		{"127.0.0.1:-1", "port must be a decimal number"},
		{"127.0.0.1:8_080", "port must be a decimal number"},
	}
	for _, listener := range listeners {
		for _, tc := range cases {
			t.Run(listener.field+"/"+tc.addr, func(t *testing.T) {
				c := validConfigForValidation()
				listener.set(&c, tc.addr)
				want := tc.want
				if want != "" {
					want = listener.field + ": " + want
				}
				requireValidation(t, c, want)
			})
		}
	}
}

func TestValidateTrustedProxyHeader(t *testing.T) {
	cases := []struct {
		header string
		want   string
	}{
		{"X-Forwarded-For", ""},
		{"Aa09!#$%&'*+-.^_`|~", ""},
		{"", "http.trusted_proxy_header: "},
		{"X Forwarded For", "http.trusted_proxy_header: "},
		{"X-Forwarded-For:", "http.trusted_proxy_header: "},
		{"X-Forwarded\r\nFor", "http.trusted_proxy_header: "},
		{"X-Request\u2013ID", "http.trusted_proxy_header: "},
	}
	for _, tc := range cases {
		t.Run(tc.header, func(t *testing.T) {
			c := validConfigForValidation()
			c.HTTP.TrustedProxyHeader = tc.header
			requireValidation(t, c, tc.want)
		})
	}
}

func TestValidateEndpointURLs(t *testing.T) {
	endpoints := []struct {
		field string
		set   func(*Config, string)
	}{
		{"telegram.api_server", func(c *Config, value string) { c.Telegram.APIServer = value }},
		{"http.public_base_url", func(c *Config, value string) { c.HTTP.PublicBaseURL = value }},
	}
	suffixes := []string{":", ":0", ":65536", ":https", "?", "?a=b", "#", "#section", "@yabot.example.com"}
	for _, endpoint := range endpoints {
		for _, suffix := range suffixes {
			t.Run(endpoint.field+"/"+suffix, func(t *testing.T) {
				c := validConfigForValidation()
				endpoint.set(&c, "https://yabot.example.com"+suffix)
				requireValidation(t, c, endpoint.field+": ")
			})
		}
	}
	cases := []struct {
		name   string
		change func(*Config)
		want   string
	}{
		{"Telegram IPv6 with proxy prefix", func(c *Config) { c.Telegram.APIServer = "http://[::1]:8081/telegram" }, ""},
		{"Telegram rejects invalid IP literal", func(c *Config) { c.Telegram.APIServer = "http://[example.com]:8081" }, "telegram.api_server: "},
		{"Telegram rejects invalid DNS labels", func(c *Config) { c.Telegram.APIServer = "https://api..example.com" }, "telegram.api_server: "},
		{"Telegram rejects whitespace", func(c *Config) { c.Telegram.APIServer = "https://example.com/api path" }, "telegram.api_server: "},
		{"public origin permits port and slash", func(c *Config) { c.HTTP.PublicBaseURL = "https://yabot.example.com:8443/" }, ""},
		{"public origin rejects path prefix", func(c *Config) { c.HTTP.PublicBaseURL = "https://yabot.example.com/miniapp" }, "http.public_base_url: "},
		{"public origin rejects encoded path", func(c *Config) { c.HTTP.PublicBaseURL = "https://yabot.example.com/%2F" }, "http.public_base_url: "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfigForValidation()
			tc.change(&c)
			requireValidation(t, c, tc.want)
		})
	}
}

func TestValidateTelegramAPIServerTransport(t *testing.T) {
	const remoteHTTP = "telegram.api_server: HTTP is only allowed for localhost or a loopback IP"
	cases := []struct {
		url  string
		want string
	}{
		{"https://api.telegram.org", ""},
		{"https://example.com:8443/bot-api", ""},
		{"http://api.telegram.org", remoteHTTP},
		{"http://localhost.example.com:8081", remoteHTTP},
		{"http://LOCALHOST:8081", remoteHTTP},
		{"http://192.0.2.1:8081", remoteHTTP},
		{"http://[2001:db8::1]:8081", remoteHTTP},
		{"http://127.0.0.1:8081", ""},
		{"http://127.1.2.3:8081", ""},
		{"http://localhost:8081", ""},
		{"http://[::1]:8081", ""},
	}
	for _, tc := range cases {
		t.Run(tc.url, func(t *testing.T) {
			c := validConfigForValidation()
			c.Telegram.APIServer = tc.url
			requireValidation(t, c, tc.want)
		})
	}
}

func TestValidateLocalValues(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Config)
		want   string
	}{
		{"data directory allows spaces", func(c *Config) { c.Store.Dir = "/var/lib/yabot data" }, ""},
		{"data directory rejects NUL", func(c *Config) { c.Store.Dir = "/var/lib/yabot\x00" }, "store.dir: "},
		{"Telegram token rejects whitespace", func(c *Config) { c.Telegram.Token = newSecret("test\n-token") }, "telegram.token: "},
		{"Telegram token rejects URL delimiters", func(c *Config) { c.Telegram.Token = newSecret("test/token") }, "telegram.token: "},
		{"Turnstile site key rejects whitespace", func(c *Config) { c.Turnstile.SiteKey = "test key" }, "turnstile.site_key: "},
		{"Turnstile secret rejects controls", func(c *Config) { c.Turnstile.Secret = newSecret("test\x00secret") }, "turnstile.secret: "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfigForValidation()
			tc.change(&c)
			requireValidation(t, c, tc.want)
		})
	}
}

func TestHMACKeyBytes(t *testing.T) {
	keyBytes := bytes.Repeat([]byte{0xff, 0xef}, 16)
	cases := []struct {
		name string
		key  string
		want string
	}{
		{"standard", base64.StdEncoding.EncodeToString(keyBytes), ""},
		{"raw standard", base64.RawStdEncoding.EncodeToString(keyBytes), ""},
		{"URL-safe", base64.URLEncoding.EncodeToString(keyBytes), ""},
		{"raw URL-safe", base64.RawURLEncoding.EncodeToString(keyBytes), ""},
		{"unset", "", "security.hmac_key: is required"},
		{"blank", " \t ", "security.hmac_key: is required"},
		{"invalid alphabet", "bad-test-key#", "security.hmac_key: must be base64 encoded"},
		{"short", base64.StdEncoding.EncodeToString(keyBytes[:31]), "security.hmac_key: must decode to exactly 32 bytes"},
		{"long", base64.StdEncoding.EncodeToString(append(bytes.Clone(keyBytes), 0)), "security.hmac_key: must decode to exactly 32 bytes"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfigForValidation()
			c.Security.HMACKey = newSecret(tc.key)
			validationErr := requireValidation(t, c, tc.want)
			decoded, err := c.Security.HMACKeyBytes()
			if tc.want != "" {
				if tc.key != "" && strings.Contains(validationErr.Error(), tc.key) {
					t.Error("Validate error exposed the HMAC key")
				}
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("HMACKeyBytes did not return an error containing %q", tc.want)
				}
				if tc.key != "" && strings.Contains(err.Error(), tc.key) {
					t.Error("HMACKeyBytes error exposed the HMAC key")
				}
				if decoded != nil {
					t.Error("HMACKeyBytes returned key data on error")
				}
				return
			}
			if err != nil || !bytes.Equal(decoded, keyBytes) {
				t.Fatal("HMACKeyBytes did not return the expected key")
			}
			second, err := c.Security.HMACKeyBytes()
			if err != nil {
				t.Fatal("second HMACKeyBytes call failed")
			}
			decoded[0] ^= 0xff
			if !bytes.Equal(second, keyBytes) {
				t.Error("HMACKeyBytes did not return independent key bytes")
			}
		})
	}
}

func TestValidateCrossChecks(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Config)
		want   string
	}{
		{"guard strict upper limits", func(c *Config) {
			c.Guard.PresentBudget = Duration(10*time.Second - 2*time.Nanosecond)
			c.Guard.BacklogCutoff = Duration(10*time.Second - time.Nanosecond)
		}, ""},
		{"guard equal budget and cutoff", func(c *Config) { c.Guard.PresentBudget = c.Guard.BacklogCutoff }, "guard.present_budget: must be < guard.backlog_cutoff"},
		{"guard cutoff exactly 10 seconds", func(c *Config) { c.Guard.BacklogCutoff = Duration(10 * time.Second) }, "guard.backlog_cutoff: must be < 10s"},
		{"recent TTL just above cutoff and history retention equal to TTL", func(c *Config) {
			c.Guard.RecentClosedTTL = c.Guard.BacklogCutoff + Duration(time.Nanosecond)
			c.Retention.GuardHistory = c.Guard.RecentClosedTTL
		}, ""},
		{"recent TTL equal to cutoff", func(c *Config) {
			c.Guard.RecentClosedTTL = c.Guard.BacklogCutoff
		}, "guard.recent_closed_ttl: must be > guard.backlog_cutoff"},
		{"recent TTL below cutoff", func(c *Config) {
			c.Guard.RecentClosedTTL = c.Guard.BacklogCutoff - Duration(time.Nanosecond)
		}, "guard.recent_closed_ttl: must be > guard.backlog_cutoff"},
		{"history retention below recent TTL", func(c *Config) {
			c.Retention.GuardHistory = c.Guard.RecentClosedTTL - Duration(time.Nanosecond)
		}, "retention.guard_history: must be >= guard.recent_closed_ttl"},
		{"workers exactly at throughput minimum", func(c *Config) {
			c.Sender.GlobalRPS = 40
			c.Sender.Workers = 18
		}, ""},
		{"workers below throughput minimum", func(c *Config) {
			c.Sender.GlobalRPS = 40
			c.Sender.Workers = 17
		}, "sender.workers: must be >= sender.global_rps"},
		{"worker count cannot round down", func(c *Config) {
			c.Sender.GlobalRPS = 41
			c.Sender.Workers = 18
		}, "sender.workers: must be >= sender.global_rps"},
		{"matching bare host ignores case and URL port", func(c *Config) {
			c.HTTP.PublicBaseURL = "https://YaBot.Example.COM:8443/"
			c.Turnstile.Hostname = "yabot.example.com"
		}, ""},
		{"Turnstile hostname rejects port", func(c *Config) {
			c.HTTP.PublicBaseURL = "https://YaBot.Example.COM:8443/"
			c.Turnstile.Hostname = "yabot.example.com:443"
		}, "turnstile.hostname: must be a bare hostname"},
		{"Turnstile hostname rejects scheme", func(c *Config) { c.Turnstile.Hostname = "https://yabot.example.com" }, "turnstile.hostname: must be a bare hostname"},
		{"Turnstile hostname rejects path", func(c *Config) { c.Turnstile.Hostname = "yabot.example.com/v/" }, "turnstile.hostname: must be a bare hostname"},
		{"Turnstile hostname rejects whitespace", func(c *Config) { c.Turnstile.Hostname = " yabot.example.com" }, "turnstile.hostname: must be a bare hostname"},
		{"Turnstile hostname rejects invalid labels", func(c *Config) { c.Turnstile.Hostname = "yabot..example.com" }, "turnstile.hostname: must be a bare hostname"},
		{"listeners cannot share the same socket", func(c *Config) { c.Admin.Listen = "127.0.0.1:08080" }, "admin.listen: must not use the same address and port as http.listen"},
		{"different hosts", func(c *Config) { c.Turnstile.Hostname = "other.example.com" }, "turnstile.hostname: "},
		{"each cap may use the entire shared pool", func(c *Config) {
			c.Sender.Reserved.Deadline = 11
			c.Sender.Cap.Interactive = 1
			c.Sender.Cap.Background = 1
		}, ""},
		{"overflowing reservation", func(c *Config) { c.Sender.Reserved.Deadline = math.MaxInt }, "sender.reserved.deadline: must be <= sender.workers"},
		{"reservations exceed workers", func(c *Config) { c.Sender.Reserved.Deadline = 13 }, "sender.reserved: "},
		{"interactive cap exceeds shared pool", func(c *Config) { c.Sender.Cap.Interactive = 7 }, "sender.cap.interactive: "},
		{"background cap exceeds shared pool", func(c *Config) { c.Sender.Cap.Background = 7 }, "sender.cap.background: "},
		{"dispatch exactly fills write queue", func(c *Config) { c.Store.WriteQueue = 4096 }, ""},
		{"dispatch exceeds write queue", func(c *Config) { c.Store.WriteQueue = 4095 }, "store.write_queue: "},
		{"dispatch product would overflow", func(c *Config) {
			c.Dispatch.Shards = math.MaxInt
			c.Dispatch.TicketsPerShard = 2
			c.Store.WriteQueue = math.MaxInt
		}, "store.write_queue: "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfigForValidation()
			tc.change(&c)
			requireValidation(t, c, tc.want)
		})
	}
}

func TestValidateReportsIndependentConflicts(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Config)
		want   []string
	}{
		{"blacklist reports each non-positive user ID", func(c *Config) {
			c.Guard.Blacklist = []int64{123, 0, -456}
		}, []string{"guard.blacklist[1]: must be a positive user ID", "guard.blacklist[2]: must be a positive user ID"}},
		{"negative guard durations report positivity and cross-checks", func(c *Config) {
			c.Guard.RecentClosedTTL = -Duration(time.Second)
			c.Retention.GuardHistory = -Duration(2 * time.Second)
		}, []string{
			"guard.recent_closed_ttl: must be > 0",
			"guard.recent_closed_ttl: must be > guard.backlog_cutoff",
			"retention.guard_history: must be > 0",
			"retention.guard_history: must be >= guard.recent_closed_ttl",
		}},
		{"combined reservations and caps exceed available workers", func(c *Config) {
			c.Sender.Workers = 12
			c.Sender.Reserved.Deadline = 9
			c.Sender.Reserved.Decision = 4
			c.Sender.Cap.Interactive = 12
		}, []string{
			"sender.reserved: deadline + decision must be <= sender.workers",
			"sender.cap.interactive: must be <= the number of shared workers",
			"sender.cap.background: must be <= the number of shared workers",
		}},
		{"oversized deadline and nonpositive decision", func(c *Config) {
			c.Sender.Workers = 16
			c.Sender.Reserved.Deadline = math.MaxInt
			c.Sender.Reserved.Decision = 0
		}, []string{
			"sender.reserved.deadline: must be <= sender.workers",
			"sender.reserved.decision: must be > 0",
			"sender.cap.interactive: must be <= the number of shared workers",
		}},
		{"nonpositive deadline and oversized decision", func(c *Config) {
			c.Sender.Reserved.Deadline = math.MinInt
			c.Sender.Reserved.Decision = math.MaxInt
		}, []string{
			"sender.reserved.deadline: must be > 0",
			"sender.reserved.decision: must be <= sender.workers",
			"sender.cap.background: must be <= the number of shared workers",
		}},
		{"both reservations exceed a small pool without overflow", func(c *Config) {
			c.Sender.Reserved.Deadline = math.MaxInt
			c.Sender.Reserved.Decision = math.MaxInt
		}, []string{
			"sender.reserved.deadline: must be <= sender.workers",
			"sender.reserved.decision: must be <= sender.workers",
			"sender.cap.interactive: must be <= the number of shared workers",
			"sender.cap.background: must be <= the number of shared workers",
		}},
		{"nonpositive decision cannot leave enough room for interactive cap", func(c *Config) {
			c.Sender.Reserved.Deadline = 12
			c.Sender.Reserved.Decision = 0
		}, []string{"sender.reserved.decision: must be > 0", "sender.cap.interactive: must be <= the number of shared workers"}},
		{"nonpositive workers also fail throughput and quota checks", func(c *Config) {
			c.Sender.Workers = 0
		}, []string{"sender.workers: must be > 0", "sender.workers: must be >= sender.global_rps × 0.3 × 1.5", "sender.reserved.deadline: must be <= sender.workers"}},
		{"invalid shard count and insufficient queue even with one shard", func(c *Config) {
			c.Dispatch.Shards = 0
			c.Store.WriteQueue = 1023
		}, []string{"dispatch.shards: must be > 0", "store.write_queue: must be >= dispatch.shards × dispatch.tickets_per_shard"}},
		{"nonpositive queue cannot fit tickets", func(c *Config) {
			c.Store.WriteQueue = 0
		}, []string{"store.write_queue: must be > 0", "store.write_queue: must be >= dispatch.shards × dispatch.tickets_per_shard"}},
		{"invalid public origin and mismatched hostname", func(c *Config) {
			c.HTTP.PublicBaseURL = "http://other.example.com/miniapp"
		}, []string{"http.public_base_url: must be an HTTPS origin with an optional trailing slash", "turnstile.hostname: must match the hostname in http.public_base_url"}},
		{"forbidden listen spelling and duplicate socket", func(c *Config) {
			c.Admin.Listen = "[127.0.0.1]:08080"
		}, []string{"admin.listen: host must be the IPv4 literal 127.0.0.1 for the local listener", "admin.listen: must not use the same address and port as http.listen"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfigForValidation()
			tc.change(&c)
			err := c.Validate()
			if err == nil {
				t.Fatalf("Validate succeeded, want errors %q", tc.want)
			}
			messages := strings.Split(err.Error(), "\n")
			for _, want := range tc.want {
				if !slices.Contains(messages, want) {
					t.Errorf("Validate returned %v, missing error %q", err, want)
				}
			}
		})
	}
}

func TestValidateJoinsErrors(t *testing.T) {
	c := Default()
	c.HTTP.Listen = "invalid"
	c.Sender.Cap.Interactive = 10
	c.Sender.Cap.Background = 10
	c.Dispatch.TicketsPerShard++
	c.Guard.RecentClosedTTL = c.Guard.BacklogCutoff
	c.Retention.GuardHistory = c.Guard.RecentClosedTTL - Duration(time.Nanosecond)
	err := c.Validate()
	if err == nil {
		t.Fatal("Validate accepted a configuration with multiple errors")
	}
	for _, field := range []string{
		"telegram.token", "http.listen", "turnstile.site_key", "turnstile.secret", "security.hmac_key",
		"sender.cap.interactive", "sender.cap.background", "store.write_queue", "guard.recent_closed_ttl", "retention.guard_history",
	} {
		if !strings.Contains(err.Error(), field+": ") {
			t.Errorf("missing %s error in %v", field, err)
		}
	}
}

func requireValidation(t *testing.T, c Config, want string) error {
	t.Helper()
	err := c.Validate()
	if want == "" {
		if err != nil {
			t.Fatalf("Validate returned %v, want success", err)
		}
	} else if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("Validate returned %v, want an error containing %q", err, want)
	}
	return err
}
