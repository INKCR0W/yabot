package config

import (
	"encoding/base64"
	"errors"
	"strings"
	"time"
)

const mibInBytes = 1024 * 1024

type Config struct {
	Telegram  TelegramConfig  `toml:"telegram"`
	HTTP      HTTPConfig      `toml:"http"`
	Admin     AdminConfig     `toml:"admin"`
	Log       LogConfig       `toml:"log"`
	Store     StoreConfig     `toml:"store"`
	Dispatch  DispatchConfig  `toml:"dispatch"`
	Sender    SenderConfig    `toml:"sender"`
	Guard     GuardConfig     `toml:"guard"`
	Turnstile TurnstileConfig `toml:"turnstile"`
	Security  SecurityConfig  `toml:"security"`
	Tenant    TenantConfig    `toml:"tenant"`
	Retention RetentionConfig `toml:"retention"`
	Runtime   RuntimeConfig   `toml:"runtime"`
}

type TelegramConfig struct {
	Token           Secret   `toml:"token"`
	APIServer       string   `toml:"api_server"`
	PollTimeout     Duration `toml:"poll_timeout"`
	SettleTimeout   Duration `toml:"settle_timeout"`
	TransientBudget Duration `toml:"transient_budget"`
	PoisonThreshold int      `toml:"poison_threshold"`
}

type HTTPConfig struct {
	Listen             string  `toml:"listen"`
	PublicBaseURL      string  `toml:"public_base_url"`
	TrustedProxyHeader string  `toml:"trusted_proxy_header"`
	MaxInflight        int     `toml:"max_inflight"`
	IPRPS              float64 `toml:"ip_rps"`
	IPBurst            int     `toml:"ip_burst"`
	UserRPS            float64 `toml:"user_rps"`
	UserBurst          int     `toml:"user_burst"`
}

type AdminConfig struct {
	Listen string `toml:"listen"`
}

type LogConfig struct {
	Level  string `toml:"level"`
	Format string `toml:"format"`
}

type StoreConfig struct {
	Dir                string   `toml:"dir"`
	CacheKiB           int      `toml:"cache_kib"`
	CheckpointInterval Duration `toml:"checkpoint_interval"`
	WriteQueue         int      `toml:"write_queue"`
	BackupKeep         int      `toml:"backup_keep"`
}

type DispatchConfig struct {
	Shards           int `toml:"shards"`
	Inbox            int `toml:"inbox"`
	TicketsPerShard  int `toml:"tickets_per_shard"`
	MailboxPerEntity int `toml:"mailbox_per_entity"`
}

type SenderConfig struct {
	Workers      int            `toml:"workers"`
	Reserved     WorkerReserved `toml:"reserved"`
	Cap          WorkerCap      `toml:"cap"`
	GlobalRPS    float64        `toml:"global_rps"`
	GlobalBurst  int            `toml:"global_burst"`
	ClassWeights ClassWeights   `toml:"class_weights"`
	ChatQueue    int            `toml:"chat_queue"`
	RaidWeight   float64        `toml:"raid_weight"`
	MaxRetries   int            `toml:"max_retries"`
}

type WorkerReserved struct {
	Deadline int `toml:"deadline"`
	Decision int `toml:"decision"`
}

type WorkerCap struct {
	Interactive int `toml:"interactive"`
	Background  int `toml:"background"`
}

type ClassWeights struct {
	Deadline    int `toml:"deadline"`
	Decision    int `toml:"decision"`
	Interactive int `toml:"interactive"`
	Background  int `toml:"background"`
}

type GuardConfig struct {
	PresentBudget    Duration `toml:"present_budget"`
	BacklogCutoff    Duration `toml:"backlog_cutoff"`
	MaxActivePerChat int      `toml:"max_active_per_chat"`
	MaxActiveTotal   int      `toml:"max_active_total"`
	RaidChatRPS      float64  `toml:"raid_chat_rps"`
	RaidGlobalWait   Duration `toml:"raid_global_wait"`
	RecentClosedTTL  Duration `toml:"recent_closed_ttl"`
	Blacklist        []int64  `toml:"blacklist"`
}

type TurnstileConfig struct {
	SiteKey     string   `toml:"site_key"`
	Secret      Secret   `toml:"secret"`
	Hostname    string   `toml:"hostname"`
	Timeout     Duration `toml:"timeout"`
	Concurrency int      `toml:"concurrency"`
}

type SecurityConfig struct {
	HMACKey        Secret   `toml:"hmac_key"`
	InitDataMaxAge Duration `toml:"initdata_max_age"`
}

// HMACKeyBytes returns a fresh 32-byte slice on success. Errors omit the key value.
func (c SecurityConfig) HMACKeyBytes() ([]byte, error) {
	const keySize = 32
	key := c.HMACKey.Reveal()
	if strings.TrimSpace(key) == "" {
		return nil, errors.New("security.hmac_key: is required")
	}
	for _, encoding := range [...]*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		decoded, err := encoding.DecodeString(key)
		if err != nil {
			continue
		}
		if len(decoded) != keySize {
			return nil, errors.New("security.hmac_key: must decode to exactly 32 bytes")
		}
		return decoded, nil
	}
	return nil, errors.New("security.hmac_key: must be base64 encoded")
}

type TenantConfig struct {
	AcceptNewChats   bool     `toml:"accept_new_chats"`
	IdleLeaveAfter   Duration `toml:"idle_leave_after"`
	ProbePeriodic    Duration `toml:"probe_periodic"`
	ProbePeriodicRPS float64  `toml:"probe_periodic_rps"`
}

type RetentionConfig struct {
	GuardHistory  Duration `toml:"guard_history"`
	SettingsAudit Duration `toml:"settings_audit"`
	DeadLetter    Duration `toml:"dead_letter"`
	RemovedChat   Duration `toml:"removed_chat"`
}

type RuntimeConfig struct {
	MemoryLimitMiB int `toml:"memory_limit_mib"`
}

// Default leaves credentials and the Turnstile site key unset for deployment.
func Default() Config {
	return Config{
		Telegram: TelegramConfig{
			APIServer:       "https://api.telegram.org",
			PollTimeout:     Duration(25 * time.Second),
			SettleTimeout:   Duration(5 * time.Second),
			TransientBudget: Duration(2 * time.Minute),
			PoisonThreshold: 3,
		},
		HTTP: HTTPConfig{
			Listen:             "127.0.0.1:8080",
			PublicBaseURL:      "https://yabot.example.com",
			TrustedProxyHeader: "X-Forwarded-For",
			MaxInflight:        256,
			IPRPS:              20,
			IPBurst:            40,
			UserRPS:            5,
			UserBurst:          10,
		},
		Admin: AdminConfig{Listen: "127.0.0.1:9090"},
		Log:   LogConfig{Level: "info", Format: "json"},
		Store: StoreConfig{
			Dir:                "/var/lib/yabot",
			CacheKiB:           8000,
			CheckpointInterval: Duration(500 * time.Millisecond),
			WriteQueue:         4096,
			BackupKeep:         7,
		},
		Dispatch: DispatchConfig{
			Shards:           4,
			Inbox:            512,
			TicketsPerShard:  1024,
			MailboxPerEntity: 16,
		},
		Sender: SenderConfig{
			Workers:      16,
			Reserved:     WorkerReserved{Deadline: 6, Decision: 4},
			Cap:          WorkerCap{Interactive: 4, Background: 4},
			GlobalRPS:    25,
			GlobalBurst:  5,
			ClassWeights: ClassWeights{Deadline: 8, Decision: 4, Interactive: 2, Background: 1},
			ChatQueue:    256,
			RaidWeight:   0.25,
			MaxRetries:   4,
		},
		Guard: GuardConfig{
			PresentBudget:    Duration(8 * time.Second),
			BacklogCutoff:    Duration(9 * time.Second),
			MaxActivePerChat: 2000,
			MaxActiveTotal:   100000,
			RaidChatRPS:      5,
			RaidGlobalWait:   Duration(4 * time.Second),
			RecentClosedTTL:  Duration(10 * time.Minute),
			Blacklist:        []int64{},
		},
		Turnstile: TurnstileConfig{
			Hostname:    "yabot.example.com",
			Timeout:     Duration(3 * time.Second),
			Concurrency: 16,
		},
		Security: SecurityConfig{InitDataMaxAge: Duration(24 * time.Hour)},
		Tenant: TenantConfig{
			AcceptNewChats:   true,
			IdleLeaveAfter:   Duration(168 * time.Hour),
			ProbePeriodic:    Duration(24 * time.Hour),
			ProbePeriodicRPS: 0.2,
		},
		Retention: RetentionConfig{
			GuardHistory:  Duration(720 * time.Hour),
			SettingsAudit: Duration(2160 * time.Hour),
			DeadLetter:    Duration(720 * time.Hour),
			RemovedChat:   Duration(720 * time.Hour),
		},
		Runtime: RuntimeConfig{MemoryLimitMiB: 300},
	}
}

// MemoryLimitBytes assumes the configured limit has passed validation.
func (c Config) MemoryLimitBytes() int64 {
	return int64(c.Runtime.MemoryLimitMiB) * mibInBytes
}
