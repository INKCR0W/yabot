package config

import (
	"bytes"
	"encoding"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/BurntSushi/toml"
)

// Parse validates defaults overridden by TOML and then by nonempty secret environment values.
// It lowercases the Turnstile hostname. A nil lookupEnv skips environment overrides, and any error returns a zero Config.
func Parse(data []byte, lookupEnv func(string) (string, bool)) (Config, error) {
	cfg := Default()
	meta, err := toml.NewDecoder(bytes.NewReader(data)).Decode(&cfg)
	var parseErr toml.ParseError
	if errors.As(err, &parseErr) {
		if len(meta.Keys()) == 0 {
			return Config{}, fmt.Errorf("invalid TOML syntax at line %d, column %d", parseErr.Position.Line, parseErr.Position.Col)
		}
		// The decoder can quote input values. Keep only messages from our unmarshallers.
		switch parseErr.Message {
		case invalidDurationText, durationTOMLType, secretTOMLType:
		default:
			parseErr.Message = "value cannot be decoded into the configured field type"
		}
		err = errors.New(parseErr.Error())
	}

	var unknownErr error
	if keys := unknownTOMLKeys(meta.Keys(), configTOMLKeys()); len(keys) != 0 {
		unknownErr = fmt.Errorf("unknown TOML keys: %s", strings.Join(keys, ", "))
	}
	if err != nil {
		return Config{}, errors.Join(unknownErr, fmt.Errorf("decode TOML: %s", err))
	}

	cfg.applyEnv(lookupEnv)
	if err := errors.Join(unknownErr, cfg.Validate()); err != nil {
		return Config{}, err
	}
	cfg.Turnstile.Hostname = strings.ToLower(cfg.Turnstile.Hostname)
	return cfg, nil
}

func unknownTOMLKeys(defined []toml.Key, declared map[string]bool) []string {
	var unknown []string
	redacted := make(map[string]struct{})
	// Match keys exactly because the decoder matches fields case-insensitively.
	for _, key := range defined {
		name := key.String()
		if _, ok := declared[name]; ok {
			continue
		}
		if len(key) > 2 && isCredentialTOMLKey(key[0], key[1]) {
			name = key[:2].String() + "." + redactedSecret
			if _, ok := redacted[name]; ok {
				continue
			}
			redacted[name] = struct{}{}
		}
		unknown = append(unknown, name)
	}
	return unknown
}

func isCredentialTOMLKey(section, name string) bool {
	switch {
	case strings.EqualFold(section, "telegram"):
		return strings.EqualFold(name, "token")
	case strings.EqualFold(section, "turnstile"):
		return strings.EqualFold(name, "secret") || strings.EqualFold(name, "site_key")
	case strings.EqualFold(section, "security"):
		return strings.EqualFold(name, "hmac_key")
	default:
		return false
	}
}

func configTOMLKeys() map[string]bool {
	keys := make(map[string]bool)
	collectTOMLKeys(reflect.TypeFor[Config](), nil, keys)
	return keys
}

func collectTOMLKeys(typ reflect.Type, prefix toml.Key, keys map[string]bool) {
	for i := range typ.NumField() {
		field := typ.Field(i)
		name, _, _ := strings.Cut(field.Tag.Get("toml"), ",")
		if !field.IsExported() || name == "" || name == "-" {
			continue
		}
		key := append(prefix, name)
		fieldType := field.Type
		if fieldType.Kind() == reflect.Pointer {
			fieldType = fieldType.Elem()
		}
		pointerType := reflect.PointerTo(fieldType)
		isLeaf := fieldType.Kind() != reflect.Struct ||
			pointerType.Implements(reflect.TypeFor[toml.Unmarshaler]()) ||
			pointerType.Implements(reflect.TypeFor[encoding.TextUnmarshaler]())
		keys[key.String()] = isLeaf
		if !isLeaf {
			collectTOMLKeys(fieldType, key, keys)
		}
	}
}

func Load(path string, lookupEnv func(string) (string, bool)) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	cfg, err := Parse(data, lookupEnv)
	if err != nil {
		return Config{}, fmt.Errorf("config %q: %w", path, err)
	}
	return cfg, nil
}

func (c *Config) applyEnv(lookupEnv func(string) (string, bool)) {
	if lookupEnv == nil {
		return
	}
	for _, entry := range []struct {
		name   string
		target *Secret
	}{
		{"YABOT_TELEGRAM_TOKEN", &c.Telegram.Token},
		{"YABOT_TURNSTILE_SECRET", &c.Turnstile.Secret},
		{"YABOT_HMAC_KEY", &c.Security.HMACKey},
	} {
		if value, ok := lookupEnv(entry.name); ok && value != "" {
			*entry.target = newSecret(value)
		}
	}
}
