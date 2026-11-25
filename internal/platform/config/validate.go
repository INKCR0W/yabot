package config

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Validate joins field and cross-field validation errors.
func (c *Config) Validate() error {
	var errs validationErrors
	errs.validateTelegram(c.Telegram)
	publicHost := errs.validateHTTP(c.HTTP)
	errs.validateListeners(c.HTTP.Listen, c.Admin.Listen)
	errs.validateLog(c.Log)
	errs.validateStoreDispatch(c.Store, c.Dispatch)
	errs.validateSender(c.Sender)
	errs.validateGuard(c.Guard)
	errs.validateTurnstile(c.Turnstile, publicHost)
	errs.validateSecurity(c.Security)
	errs.positiveDuration("tenant.idle_leave_after", c.Tenant.IdleLeaveAfter)
	errs.positiveDuration("tenant.probe_periodic", c.Tenant.ProbePeriodic)
	errs.positiveRate("tenant.probe_periodic_rps", c.Tenant.ProbePeriodicRPS)
	errs.positiveDuration("retention.guard_history", c.Retention.GuardHistory)
	errs.require(c.Retention.GuardHistory >= c.Guard.RecentClosedTTL,
		"retention.guard_history", "must be >= guard.recent_closed_ttl")
	errs.positiveDuration("retention.settings_audit", c.Retention.SettingsAudit)
	errs.positiveDuration("retention.dead_letter", c.Retention.DeadLetter)
	errs.positiveDuration("retention.removed_chat", c.Retention.RemovedChat)
	errs.positiveInt("runtime.memory_limit_mib", c.Runtime.MemoryLimitMiB)
	errs.require(int64(c.Runtime.MemoryLimitMiB) <= math.MaxInt64/mibInBytes,
		"runtime.memory_limit_mib", "must fit in int64 when converted to bytes")
	return errors.Join(errs...)
}

type validationErrors []error

func (e *validationErrors) add(path, message string) {
	*e = append(*e, fmt.Errorf("%s: %s", path, message))
}

func (e *validationErrors) require(ok bool, path, message string) {
	if !ok {
		e.add(path, message)
	}
}

func (e *validationErrors) validateCredential(path, value string) {
	e.require(strings.TrimSpace(value) != "", path, "is required")
	e.require(!strings.ContainsFunc(value, isSpaceOrControl), path, "must not contain whitespace or control characters")
}

func (e *validationErrors) positiveInt(path string, value int) {
	e.require(value > 0, path, "must be > 0")
}

func (e *validationErrors) positiveDuration(path string, value Duration) {
	e.require(value > 0, path, "must be > 0")
}

func (e *validationErrors) positiveRate(path string, value float64) {
	e.require(isPositiveFinite(value), path, "must be finite and > 0")
}

func isPositiveFinite(value float64) bool {
	return value > 0 && !math.IsInf(value, 1)
}

func (e *validationErrors) validateTelegram(c TelegramConfig) {
	const maxPollTimeout = 50 * time.Second
	const pollTimeoutPath = "telegram.poll_timeout"
	const apiServerPath = "telegram.api_server"
	token := c.Token.Reveal()
	e.validateCredential("telegram.token", token)
	e.require(!strings.ContainsAny(token, `/\?#%`), "telegram.token", "must be safe to use in the Bot API URL path")
	apiServer, validEndpoint := parseEndpointURL(c.APIServer)
	e.require(validEndpoint && (apiServer.Scheme == "http" || apiServer.Scheme == "https"),
		apiServerPath, "must be an absolute HTTP or HTTPS URL with a valid host and optional port, without credentials, query, or fragment")
	if apiServer != nil && apiServer.Scheme == "http" {
		host := apiServer.Hostname()
		ip, err := netip.ParseAddr(host)
		// Go's default HTTP proxy bypass recognizes the exact spelling localhost.
		e.require(host == "localhost" || (err == nil && ip.IsLoopback()), apiServerPath,
			"HTTP is only allowed for localhost or a loopback IP because the Bot API URL contains the token")
	}
	e.positiveDuration(pollTimeoutPath, c.PollTimeout)
	e.require(c.PollTimeout.Std() <= maxPollTimeout, pollTimeoutPath, "must be <= 50s")
	e.require(c.PollTimeout.Std()%time.Second == 0, pollTimeoutPath, "must be a whole number of seconds")
	e.positiveDuration("telegram.settle_timeout", c.SettleTimeout)
	e.positiveDuration("telegram.transient_budget", c.TransientBudget)
	e.positiveInt("telegram.poison_threshold", c.PoisonThreshold)
}

func (e *validationErrors) validateHTTP(c HTTPConfig) string {
	e.require(isHTTPFieldName(c.TrustedProxyHeader), "http.trusted_proxy_header", "must be a valid HTTP field name")
	e.positiveInt("http.max_inflight", c.MaxInflight)
	e.positiveRate("http.ip_rps", c.IPRPS)
	e.positiveInt("http.ip_burst", c.IPBurst)
	e.positiveRate("http.user_rps", c.UserRPS)
	e.positiveInt("http.user_burst", c.UserBurst)

	publicURL, validEndpoint := parseEndpointURL(c.PublicBaseURL)
	e.require(validEndpoint && publicURL.Scheme == "https" && (publicURL.EscapedPath() == "" || publicURL.EscapedPath() == "/"),
		"http.public_base_url", "must be an HTTPS origin with an optional trailing slash")
	if publicURL == nil {
		return ""
	}
	return publicURL.Hostname()
}

// A parsed host can be checked independently of other endpoint URL requirements.
func parseEndpointURL(value string) (*url.URL, bool) {
	parsed, err := url.Parse(value)
	if err != nil {
		return nil, false
	}
	host := parsed.Hostname()
	if strings.HasPrefix(parsed.Host, "[") {
		ip, err := netip.ParseAddr(host)
		if err != nil || !ip.Is6() {
			return nil, false
		}
		host = "[" + host + "]"
	} else if !isBareHostname(strings.TrimSuffix(host, ".")) {
		return nil, false
	}
	valid := parsed.IsAbs() && parsed.User == nil && !parsed.ForceQuery && parsed.RawQuery == "" &&
		!strings.Contains(value, "#") && !strings.ContainsFunc(value, isSpaceOrControl) &&
		(parsed.Host == host || parsePort(parsed.Port()) != 0)
	return parsed, valid
}

func (e *validationErrors) validateListeners(httpListen, adminListen string) {
	httpAddr := e.validateListenAddress("http.listen", httpListen)
	adminAddr := e.validateListenAddress("admin.listen", adminListen)
	if httpAddr.IsValid() && httpAddr == adminAddr {
		e.add("admin.listen", "must not use the same address and port as http.listen")
	}
}

func (e *validationErrors) validateListenAddress(path, value string) netip.AddrPort {
	host, port, err := net.SplitHostPort(value)
	if err != nil {
		e.add(path, "must be in 127.0.0.1:port form for the local IPv4 listener")
		return netip.AddrPort{}
	}
	portNumber := parsePort(port)
	e.require(strings.HasPrefix(value, "127.0.0.1:"), path, "host must be the IPv4 literal 127.0.0.1 for the local listener")
	e.require(portNumber != 0, path, "port must be a decimal number in 1–65535")
	ip, err := netip.ParseAddr(host)
	if err != nil || portNumber == 0 {
		return netip.AddrPort{}
	}
	return netip.AddrPortFrom(ip.Unmap(), uint16(portNumber))
}

func parsePort(value string) uint64 {
	port, err := strconv.ParseUint(value, 10, 16)
	if err != nil {
		return 0
	}
	return port
}

func isHTTPFieldName(value string) bool {
	const tokenPunctuation = "!#$%&'*+-.^_`|~"
	for i := range len(value) {
		b := value[i]
		if !isASCIIAlphanumeric(b) && !strings.ContainsRune(tokenPunctuation, rune(b)) {
			return false
		}
	}
	return value != ""
}

func isBareHostname(value string) bool {
	if len(value) > 253 {
		return false
	}
	for label := range strings.SplitSeq(value, ".") {
		if len(label) == 0 || len(label) > 63 || !isASCIIAlphanumeric(label[0]) || !isASCIIAlphanumeric(label[len(label)-1]) {
			return false
		}
		for i := 1; i < len(label)-1; i++ {
			if !isASCIIAlphanumeric(label[i]) && label[i] != '-' {
				return false
			}
		}
	}
	return true
}

func isASCIIAlphanumeric(b byte) bool {
	return 'a' <= b && b <= 'z' || 'A' <= b && b <= 'Z' || '0' <= b && b <= '9'
}

func isSpaceOrControl(r rune) bool {
	return unicode.IsSpace(r) || unicode.IsControl(r)
}

func (e *validationErrors) validateLog(c LogConfig) {
	switch c.Level {
	case "debug", "info", "warn", "error":
	default:
		e.add("log.level", "must be debug, info, warn, or error")
	}
	e.require(c.Format == "json" || c.Format == "text", "log.format", "must be json or text")
}

func (e *validationErrors) validateStoreDispatch(store StoreConfig, dispatch DispatchConfig) {
	e.require(strings.TrimSpace(store.Dir) != "", "store.dir", "is required")
	e.require(!strings.ContainsRune(store.Dir, 0), "store.dir", "must not contain NUL characters")
	e.positiveInt("store.cache_kib", store.CacheKiB)
	e.positiveDuration("store.checkpoint_interval", store.CheckpointInterval)
	e.positiveInt("store.write_queue", store.WriteQueue)
	e.positiveInt("store.backup_keep", store.BackupKeep)
	e.positiveInt("dispatch.shards", dispatch.Shards)
	e.positiveInt("dispatch.inbox", dispatch.Inbox)
	e.positiveInt("dispatch.tickets_per_shard", dispatch.TicketsPerShard)
	e.positiveInt("dispatch.mailbox_per_entity", dispatch.MailboxPerEntity)
	// Use the smallest valid count for an invalid operand, and divide to avoid overflow.
	e.require(max(dispatch.Shards, 1) <= store.WriteQueue/max(dispatch.TicketsPerShard, 1),
		"store.write_queue", "must be >= dispatch.shards × dispatch.tickets_per_shard")
}

func (e *validationErrors) validateSender(c SenderConfig) {
	const (
		telegramP99RTTSeconds = 0.3
		workerSafetyFactor    = 1.5
	)
	e.positiveInt("sender.workers", c.Workers)
	e.positiveInt("sender.reserved.deadline", c.Reserved.Deadline)
	e.positiveInt("sender.reserved.decision", c.Reserved.Decision)
	e.positiveInt("sender.cap.interactive", c.Cap.Interactive)
	e.positiveInt("sender.cap.background", c.Cap.Background)
	e.positiveRate("sender.global_rps", c.GlobalRPS)
	e.positiveInt("sender.global_burst", c.GlobalBurst)
	e.positiveInt("sender.class_weights.deadline", c.ClassWeights.Deadline)
	e.positiveInt("sender.class_weights.decision", c.ClassWeights.Decision)
	e.positiveInt("sender.class_weights.interactive", c.ClassWeights.Interactive)
	e.positiveInt("sender.class_weights.background", c.ClassWeights.Background)
	e.positiveInt("sender.chat_queue", c.ChatQueue)
	e.require(c.RaidWeight > 0 && c.RaidWeight <= 1, "sender.raid_weight", "must be in (0, 1]")
	e.positiveInt("sender.max_retries", c.MaxRetries)
	if isPositiveFinite(c.GlobalRPS) {
		minWorkers := max(c.GlobalRPS*telegramP99RTTSeconds*workerSafetyFactor, 1)
		e.require(float64(c.Workers) >= minWorkers, "sender.workers", "must be >= sender.global_rps × 0.3 × 1.5")
	}
	e.validateWorkerQuota(c)
}

func (e *validationErrors) validateWorkerQuota(c SenderConfig) {
	deadlineFits := c.Reserved.Deadline <= c.Workers
	decisionFits := c.Reserved.Decision <= c.Workers
	e.require(c.Reserved.Deadline <= 0 || deadlineFits, "sender.reserved.deadline", "must be <= sender.workers")
	e.require(c.Reserved.Decision <= 0 || decisionFits, "sender.reserved.decision", "must be <= sender.workers")
	capLimit := 0
	capLimitMessage := "must be <= sender.workers"
	if c.Workers > 0 {
		// Clamp the first subtraction so the shared pool calculation cannot overflow.
		remaining := max(0, c.Workers-max(c.Reserved.Deadline, 1))
		decision := max(c.Reserved.Decision, 1)
		if deadlineFits && decisionFits && decision > remaining {
			e.add("sender.reserved", "deadline + decision must be <= sender.workers")
		}
		capLimit = max(0, remaining-decision)
		capLimitMessage = "must be <= the number of shared workers"
	}
	e.require(c.Cap.Interactive <= 0 || c.Cap.Interactive <= capLimit, "sender.cap.interactive", capLimitMessage)
	e.require(c.Cap.Background <= 0 || c.Cap.Background <= capLimit, "sender.cap.background", capLimitMessage)
}

func (e *validationErrors) validateGuard(c GuardConfig) {
	const maxBacklogCutoff = 10 * time.Second
	e.positiveDuration("guard.present_budget", c.PresentBudget)
	e.positiveDuration("guard.backlog_cutoff", c.BacklogCutoff)
	e.positiveInt("guard.max_active_per_chat", c.MaxActivePerChat)
	e.positiveInt("guard.max_active_total", c.MaxActiveTotal)
	e.positiveRate("guard.raid_chat_rps", c.RaidChatRPS)
	e.positiveDuration("guard.raid_global_wait", c.RaidGlobalWait)
	e.positiveDuration("guard.recent_closed_ttl", c.RecentClosedTTL)
	e.require(c.PresentBudget < c.BacklogCutoff, "guard.present_budget", "must be < guard.backlog_cutoff")
	e.require(c.BacklogCutoff.Std() < maxBacklogCutoff, "guard.backlog_cutoff", "must be < 10s")
	e.require(c.RecentClosedTTL > c.BacklogCutoff, "guard.recent_closed_ttl", "must be > guard.backlog_cutoff")
	for i, userID := range c.Blacklist {
		if userID <= 0 {
			e.add(fmt.Sprintf("guard.blacklist[%d]", i), "must be a positive user ID")
		}
	}
}

func (e *validationErrors) validateTurnstile(c TurnstileConfig, publicHost string) {
	e.validateCredential("turnstile.site_key", c.SiteKey)
	e.validateCredential("turnstile.secret", c.Secret.Reveal())
	e.positiveDuration("turnstile.timeout", c.Timeout)
	e.positiveInt("turnstile.concurrency", c.Concurrency)
	if !isBareHostname(c.Hostname) {
		e.add("turnstile.hostname", "must be a bare hostname")
		return
	}
	if publicHost != "" {
		e.require(strings.EqualFold(c.Hostname, publicHost),
			"turnstile.hostname", "must match the hostname in http.public_base_url")
	}
}

func (e *validationErrors) validateSecurity(c SecurityConfig) {
	e.positiveDuration("security.initdata_max_age", c.InitDataMaxAge)
	if _, err := c.HMACKeyBytes(); err != nil {
		*e = append(*e, err)
	}
}
