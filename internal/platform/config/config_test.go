package config

import (
	"reflect"
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	cfg := Default()
	tests := []struct {
		field string
		got   any
		want  any
	}{
		{"telegram.token", cfg.Telegram.Token.Reveal(), ""},
		{"telegram.api_server", cfg.Telegram.APIServer, "https://api.telegram.org"},
		{"telegram.poll_timeout", cfg.Telegram.PollTimeout.Std(), 25 * time.Second},
		{"telegram.settle_timeout", cfg.Telegram.SettleTimeout.Std(), 5 * time.Second},
		{"telegram.transient_budget", cfg.Telegram.TransientBudget.Std(), 2 * time.Minute},
		{"telegram.poison_threshold", cfg.Telegram.PoisonThreshold, 3},
		{"http.listen", cfg.HTTP.Listen, "127.0.0.1:8080"},
		{"http.public_base_url", cfg.HTTP.PublicBaseURL, "https://yabot.example.com"},
		{"http.trusted_proxy_header", cfg.HTTP.TrustedProxyHeader, "X-Forwarded-For"},
		{"http.max_inflight", cfg.HTTP.MaxInflight, 256},
		{"http.ip_rps", cfg.HTTP.IPRPS, 20.0},
		{"http.ip_burst", cfg.HTTP.IPBurst, 40},
		{"http.user_rps", cfg.HTTP.UserRPS, 5.0},
		{"http.user_burst", cfg.HTTP.UserBurst, 10},
		{"admin.listen", cfg.Admin.Listen, "127.0.0.1:9090"},
		{"log.level", cfg.Log.Level, "info"},
		{"log.format", cfg.Log.Format, "json"},
		{"store.dir", cfg.Store.Dir, "/var/lib/yabot"},
		{"store.cache_kib", cfg.Store.CacheKiB, 8000},
		{"store.checkpoint_interval", cfg.Store.CheckpointInterval.Std(), 500 * time.Millisecond},
		{"store.write_queue", cfg.Store.WriteQueue, 4096},
		{"store.backup_keep", cfg.Store.BackupKeep, 7},
		{"dispatch.shards", cfg.Dispatch.Shards, 4},
		{"dispatch.inbox", cfg.Dispatch.Inbox, 512},
		{"dispatch.tickets_per_shard", cfg.Dispatch.TicketsPerShard, 1024},
		{"dispatch.mailbox_per_entity", cfg.Dispatch.MailboxPerEntity, 16},
		{"sender.workers", cfg.Sender.Workers, 16},
		{"sender.reserved.deadline", cfg.Sender.Reserved.Deadline, 6},
		{"sender.reserved.decision", cfg.Sender.Reserved.Decision, 4},
		{"sender.cap.interactive", cfg.Sender.Cap.Interactive, 4},
		{"sender.cap.background", cfg.Sender.Cap.Background, 4},
		{"sender.global_rps", cfg.Sender.GlobalRPS, 25.0},
		{"sender.global_burst", cfg.Sender.GlobalBurst, 5},
		{"sender.class_weights.deadline", cfg.Sender.ClassWeights.Deadline, 8},
		{"sender.class_weights.decision", cfg.Sender.ClassWeights.Decision, 4},
		{"sender.class_weights.interactive", cfg.Sender.ClassWeights.Interactive, 2},
		{"sender.class_weights.background", cfg.Sender.ClassWeights.Background, 1},
		{"sender.chat_queue", cfg.Sender.ChatQueue, 256},
		{"sender.raid_weight", cfg.Sender.RaidWeight, 0.25},
		{"sender.max_retries", cfg.Sender.MaxRetries, 4},
		{"guard.present_budget", cfg.Guard.PresentBudget.Std(), 8 * time.Second},
		{"guard.backlog_cutoff", cfg.Guard.BacklogCutoff.Std(), 9 * time.Second},
		{"guard.max_active_per_chat", cfg.Guard.MaxActivePerChat, 2000},
		{"guard.max_active_total", cfg.Guard.MaxActiveTotal, 100000},
		{"guard.raid_chat_rps", cfg.Guard.RaidChatRPS, 5.0},
		{"guard.raid_global_wait", cfg.Guard.RaidGlobalWait.Std(), 4 * time.Second},
		{"guard.recent_closed_ttl", cfg.Guard.RecentClosedTTL.Std(), 10 * time.Minute},
		{"guard.blacklist", cfg.Guard.Blacklist, []int64{}},
		{"turnstile.site_key", cfg.Turnstile.SiteKey, ""},
		{"turnstile.secret", cfg.Turnstile.Secret.Reveal(), ""},
		{"turnstile.hostname", cfg.Turnstile.Hostname, "yabot.example.com"},
		{"turnstile.timeout", cfg.Turnstile.Timeout.Std(), 3 * time.Second},
		{"turnstile.concurrency", cfg.Turnstile.Concurrency, 16},
		{"security.hmac_key", cfg.Security.HMACKey.Reveal(), ""},
		{"security.initdata_max_age", cfg.Security.InitDataMaxAge.Std(), 24 * time.Hour},
		{"tenant.accept_new_chats", cfg.Tenant.AcceptNewChats, true},
		{"tenant.idle_leave_after", cfg.Tenant.IdleLeaveAfter.Std(), 168 * time.Hour},
		{"tenant.probe_periodic", cfg.Tenant.ProbePeriodic.Std(), 24 * time.Hour},
		{"tenant.probe_periodic_rps", cfg.Tenant.ProbePeriodicRPS, 0.2},
		{"retention.guard_history", cfg.Retention.GuardHistory.Std(), 720 * time.Hour},
		{"retention.settings_audit", cfg.Retention.SettingsAudit.Std(), 2160 * time.Hour},
		{"retention.dead_letter", cfg.Retention.DeadLetter.Std(), 720 * time.Hour},
		{"retention.removed_chat", cfg.Retention.RemovedChat.Std(), 720 * time.Hour},
		{"runtime.memory_limit_mib", cfg.Runtime.MemoryLimitMiB, 300},
		{"runtime memory limit in bytes", cfg.MemoryLimitBytes(), int64(314572800)},
	}
	for _, tc := range tests {
		t.Run(tc.field, func(t *testing.T) {
			if !reflect.DeepEqual(tc.got, tc.want) {
				t.Errorf("got %v, want %v", tc.got, tc.want)
			}
		})
	}
}
