package i18n

import (
	"fmt"
	"html"
	"io/fs"
	"log/slog"
	"maps"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/metrics"
)

const (
	ZhHans = "zh-Hans"
	En     = "en"

	maxLoggedKeys = 256
)

// Resolve returns the first supported language family, or En if none matches.
func Resolve(candidates ...string) string {
	for _, candidate := range candidates {
		switch tag := strings.ToLower(candidate); {
		case tag == "zh", strings.HasPrefix(tag, "zh-"), strings.HasPrefix(tag, "zh_"):
			return ZhHans
		case tag == "en", strings.HasPrefix(tag, "en-"), strings.HasPrefix(tag, "en_"):
			return En
		}
	}
	return En
}

type Key = string

type Args map[string]string

// A nil Metrics or Log disables the corresponding output.
type Deps struct {
	Metrics *metrics.Set
	Log     *slog.Logger
}

type translations struct {
	zh map[string]message
	en map[string]message
}

type catalogSnapshot map[string]translations

// Catalog supports concurrent registration and lookup. It must not be copied after first use.
type Catalog struct {
	registerMu sync.Mutex
	catalog    atomic.Pointer[catalogSnapshot]
	logged     atomic.Pointer[map[string]struct{}]
	log        *slog.Logger
	missingKey *metrics.Counter
	missingArg *metrics.Counter
}

func New(d Deps) *Catalog {
	c := &Catalog{log: d.Log}
	if d.Metrics != nil {
		c.missingKey = d.Metrics.GetOrCreateCounter(`yabot_i18n_missing_total{kind="key"}`)
		c.missingArg = d.Metrics.GetOrCreateCounter(`yabot_i18n_missing_total{kind="arg"}`)
	}
	return c
}

// Register loads both language files from the root of fsys and publishes them atomically.
// A failed registration leaves the catalog unchanged.
func (c *Catalog) Register(namespace string, fsys fs.FS) error {
	if !regexp.MustCompile(`^[a-z][a-z0-9_]*$`).MatchString(namespace) {
		return fmt.Errorf("invalid translation namespace %q", namespace)
	}
	if fsys == nil {
		return fmt.Errorf("translation namespace %q has no filesystem", namespace)
	}

	pattern := regexp.MustCompile(`\{[a-z][a-z0-9_]*\}`)
	zh, err := readTranslations(fsys, ZhHans+".toml", pattern)
	if err != nil {
		return fmt.Errorf("translation namespace %q: %w", namespace, err)
	}
	en, err := readTranslations(fsys, En+".toml", pattern)
	if err != nil {
		return fmt.Errorf("translation namespace %q: %w", namespace, err)
	}
	if err := validateTranslations(zh, en); err != nil {
		return fmt.Errorf("translation namespace %q: %w", namespace, err)
	}
	return c.publish(namespace, translations{zh: zh, en: en})
}

func (c *Catalog) publish(namespace string, values translations) error {
	c.registerMu.Lock()
	defer c.registerMu.Unlock()

	current := c.snapshot()
	if _, ok := current[namespace]; ok {
		return fmt.Errorf("translation namespace %q is already registered", namespace)
	}
	next := make(catalogSnapshot, len(current)+1)
	maps.Copy(next, current)
	next[namespace] = values
	c.catalog.Store(&next)
	return nil
}

func (c *Catalog) snapshot() catalogSnapshot {
	if snapshot := c.catalog.Load(); snapshot != nil {
		return *snapshot
	}
	return nil
}

func (c *Catalog) Has(key string) bool {
	namespace, localKey, ok := strings.Cut(key, ".")
	if !ok {
		return false
	}
	_, ok = c.snapshot()[namespace].zh[localKey]
	return ok
}

func (c *Catalog) T(lang, key string, args Args) string {
	return c.render(lang, key, args, nil)
}

// HTML escapes substituted arguments for Telegram HTML parse mode and preserves template markup.
func (c *Catalog) HTML(lang, key string, args Args) string {
	return c.render(lang, key, args, html.EscapeString)
}

func (c *Catalog) render(lang, key string, args Args, escape func(string) string) string {
	namespace, localKey, hasNamespace := strings.Cut(key, ".")
	values := c.snapshot()[namespace]
	messages := values.en
	if Resolve(lang) == ZhHans {
		messages = values.zh
	}
	msg, found := messages[localKey]
	if !hasNamespace || !found {
		if c.missingKey != nil {
			c.missingKey.Inc()
		}
		if c.log != nil && c.markLogged(key) {
			c.log.Error("missing translation key", "key", key, "kind", "key")
		}
		return key
	}
	if len(msg.slots) == 0 {
		return msg.text
	}

	var out strings.Builder
	start := 0
	for _, slot := range msg.slots {
		out.WriteString(msg.text[start:slot.start])
		value, ok := args[slot.name]
		if !ok {
			if c.missingArg != nil {
				c.missingArg.Inc()
			}
			if c.log != nil && c.markLogged(key) {
				c.log.Error("missing translation argument", "key", key, "kind", "arg", "arg", slot.name)
			}
			value = msg.text[slot.start:slot.end]
		} else if escape != nil {
			value = escape(value)
		}
		out.WriteString(value)
		start = slot.end
	}
	out.WriteString(msg.text[start:])
	return out.String()
}

func (c *Catalog) markLogged(key string) bool {
	for {
		current := c.logged.Load()
		var keys map[string]struct{}
		if current != nil {
			keys = *current
		}
		if _, ok := keys[key]; ok || len(keys) >= maxLoggedKeys {
			return false
		}
		next := make(map[string]struct{}, len(keys)+1)
		maps.Copy(next, keys)
		next[key] = struct{}{}
		if c.logged.CompareAndSwap(current, &next) {
			return true
		}
	}
}
