package config

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
)

func TestParse(t *testing.T) {
	const requiredTOML = "[turnstile]\nsite_key = 'test-site'\n"
	fileKey := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
	envKey := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{2}, 32))
	fileTOML := fmt.Sprintf(`[telegram]
token = "test-file-token"
[turnstile]
site_key = "test-site"
secret = "test-file-secret"
[security]
hmac_key = %q
`, fileKey)
	env := map[string]string{
		"YABOT_TELEGRAM_TOKEN":     "test-env-token",
		"YABOT_TURNSTILE_SECRET":   "test-env-secret",
		"YABOT_HMAC_KEY":           envKey,
		"YABOT_LOG_LEVEL":          "debug",
		"YABOT_TURNSTILE_SITE_KEY": "test-env-site",
	}
	lookup := func(name string) (string, bool) {
		value, ok := env[name]
		return value, ok
	}
	emptyLookup := func(string) (string, bool) { return "", true }
	fromEnv := Default()
	fromEnv.Telegram.Token = newSecret(env["YABOT_TELEGRAM_TOKEN"])
	fromEnv.Turnstile.SiteKey = "test-site"
	fromEnv.Turnstile.Secret = newSecret(env["YABOT_TURNSTILE_SECRET"])
	fromEnv.Security.HMACKey = newSecret(envKey)
	fromFile := Default()
	fromFile.Telegram.Token = newSecret("test-file-token")
	fromFile.Turnstile.SiteKey = "test-site"
	fromFile.Turnstile.Secret = newSecret("test-file-secret")
	fromFile.Security.HMACKey = newSecret(fileKey)
	partial := fromEnv
	partial.Telegram.PollTimeout = Duration(30 * time.Second)
	partial.Sender.Reserved.Decision = 3
	partial.Guard.Blacklist = []int64{12345678901}

	tests := []struct {
		name      string
		input     string
		lookupEnv func(string) (string, bool)
		want      Config
		wantErrs  []string
	}{
		{name: "minimal TOML with env", input: requiredTOML, lookupEnv: lookup, want: fromEnv},
		{
			name:      "Turnstile hostname is lowercased",
			input:     requiredTOML + "hostname = 'YaBot.Example.COM'\n",
			lookupEnv: lookup,
			want:      fromEnv,
		},
		{name: "missing site key", lookupEnv: lookup, wantErrs: []string{"turnstile.site_key: is required"}},
		{
			name: "partial overrides",
			input: requiredTOML + `
[telegram]
poll_timeout = "30s"
[sender]
reserved = { decision = 3 }
[guard]
blacklist = [12345678901]
`,
			lookupEnv: lookup,
			want:      partial,
		},
		{name: "env replaces file secrets only", input: fileTOML, lookupEnv: lookup, want: fromEnv},
		{name: "empty env preserves file secrets", input: fileTOML, lookupEnv: emptyLookup, want: fromFile},
		{name: "nil env uses file secrets", input: fileTOML, want: fromFile},
		{
			name:      "unknown keys and validation",
			input:     "[turnstile]\nsite_ky = 'test-site'\n[sender]\nreserved = { deadine = 3 }\n",
			lookupEnv: lookup,
			wantErrs:  []string{"unknown TOML keys", "turnstile.site_ky", "sender.reserved.deadine", "turnstile.site_key"},
		},
		{
			name:      "case variant key",
			input:     requiredTOML + "[log]\nLEVEL = 'debug'\n",
			lookupEnv: lookup,
			wantErrs:  []string{"unknown TOML keys: log.LEVEL"},
		},
		{
			name:      "case variant key alongside declared key",
			input:     requiredTOML + "[log]\nlevel = 'info'\nLEVEL = 'debug'\n",
			lookupEnv: lookup,
			wantErrs:  []string{"unknown TOML keys: log.LEVEL"},
		},
		{
			name:      "case variant table",
			input:     requiredTOML + "[Log]\nlevel = 'debug'\n",
			lookupEnv: lookup,
			wantErrs:  []string{"unknown TOML keys: Log, Log.level"},
		},
		{
			name:      "case variant nested table",
			input:     requiredTOML + "[sender.Reserved]\ndeadline = 6\n",
			lookupEnv: lookup,
			wantErrs:  []string{"unknown TOML keys: sender.Reserved, sender.Reserved.deadline"},
		},
		{
			name:      "case variant inline table key",
			input:     requiredTOML + "[sender]\nreserved = { Deadline = 6, decision = 4 }\n",
			lookupEnv: lookup,
			wantErrs:  []string{"unknown TOML keys: sender.reserved.Deadline"},
		},
		{
			name:      "literal dot in key",
			input:     `'log.level' = 'debug'` + "\n" + requiredTOML,
			lookupEnv: lookup,
			wantErrs:  []string{`unknown TOML keys: "log.level"`},
		},
		{
			name:      "quoted segments in dotted key",
			input:     `'log'."level" = 'info'` + "\n" + requiredTOML,
			lookupEnv: lookup,
			want:      fromEnv,
		},
		{
			name:      "case variant key with wrong value type",
			input:     requiredTOML + "[log]\nLEVEL = true\n",
			lookupEnv: lookup,
			wantErrs:  []string{"unknown TOML keys: log.LEVEL", "incompatible types"},
		},
		{
			name:      "bad duration string",
			input:     requiredTOML + "[telegram]\npoll_timeout = 'soon'\n",
			lookupEnv: lookup,
			wantErrs:  []string{"telegram.poll_timeout", "duration"},
		},
		{
			name:      "duration requires TOML string",
			input:     requiredTOML + "[telegram]\npoll_timeout = 25\n",
			lookupEnv: lookup,
			wantErrs:  []string{"telegram.poll_timeout", "string"},
		},
		{
			name:      "secret requires TOML string",
			input:     requiredTOML + "[telegram]\ntoken = true\n",
			lookupEnv: lookup,
			wantErrs:  []string{"telegram.token", "string"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse([]byte(tt.input), tt.lookupEnv)
			if len(tt.wantErrs) != 0 {
				if err == nil {
					t.Fatalf("Parse succeeded, want errors containing %q", tt.wantErrs)
				}
				for _, wantErr := range tt.wantErrs {
					if !strings.Contains(err.Error(), wantErr) {
						t.Errorf("Parse error %q does not contain %q", err, wantErr)
					}
				}
				if !reflect.DeepEqual(got, Config{}) {
					t.Errorf("Parse returned a config on error: %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestParseSyntaxErrorOmitsInput(t *testing.T) {
	tests := []struct {
		name       string
		credential string
		input      string
		line       int
	}{
		{"on credential line", "01234567890123456789", "[telegram]\ntoken = %s\n", 2},
		{"after credential line", "fixture-token-before-error", "[telegram]\ntoken = %q\npoll_timeout =", 3},
		{"unclosed multiline string", "fixture-token-in-multiline", "[telegram]\ntoken = \"\"\"%s\nunclosed", 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(fmt.Sprintf(tc.input, tc.credential)), nil)
			requireParseErrorOmitsCredential(t, err, tc.credential, fmt.Sprintf("line %d", tc.line))
		})
	}
}

func TestParseDecodeErrorOmitsInput(t *testing.T) {
	tests := []struct {
		name       string
		credential string
		input      string
		want       []string
	}{
		{"numeric token", "123456789012345678", "[telegram]\ntoken = %s\n", []string{"telegram.token", "must be a TOML string"}},
		{"HMAC array", "fixture-key-in-array", "[security]\nhmac_key = [%q]\n", []string{"security.hmac_key", "must be a TOML string"}},
		{"HMAC table", "fixture-key-as-table-member", "[security]\nhmac_key = {%q = true}\n", []string{"security.hmac_key", "must be a TOML string"}},
		{"case variant HMAC table", "fixture-key-in-case-variant", "[Security]\nHMAC_KEY = {%q = true}\n", []string{"Security.HMAC_KEY", "must be a TOML string"}},
		{"invalid duration", "fixture-token-as-duration", "[telegram]\npoll_timeout = %q\n", []string{"telegram.poll_timeout", "valid duration string"}},
		{"unsafe integer rate", "9007199254740993", "[http]\nip_rps = %s\n", []string{"http.ip_rps", "decode TOML"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(fmt.Sprintf(tc.input, tc.credential)), nil)
			requireParseErrorOmitsCredential(t, err, tc.credential, tc.want...)
		})
	}
}

func TestParseInvalidHMACKeyOmitsInput(t *testing.T) {
	tests := []struct {
		name       string
		credential string
		fromEnv    bool
		want       string
	}{
		{"file invalid base64", "fixture-invalid-file-hmac!", false, "must be base64 encoded"},
		{"file wrong decoded length", "c2hvcnQtZml4dHVyZS1rZXk=", false, "must decode to exactly 32 bytes"},
		{"environment invalid base64", "fixture-invalid-env-hmac!", true, "must be base64 encoded"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := "[security]\n"
			var lookupEnv func(string) (string, bool)
			if tc.fromEnv {
				lookupEnv = func(name string) (string, bool) { return tc.credential, name == "YABOT_HMAC_KEY" }
			} else {
				input += fmt.Sprintf("hmac_key = %q\n", tc.credential)
			}
			_, err := Parse([]byte(input), lookupEnv)
			requireParseErrorOmitsCredential(t, err, tc.credential, "security.hmac_key: "+tc.want)
		})
	}
}

func requireParseErrorOmitsCredential(t *testing.T, err error, credential string, want ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("Parse succeeded with invalid input")
	}
	for _, part := range want {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("Parse error is missing %q", part)
		}
	}
	formats := []struct{ name, text string }{
		{"Error()", err.Error()},
		{"fmt.Sprint", fmt.Sprint(err)},
		{"%+v", fmt.Sprintf("%+v", err)},
	}
	for _, format := range formats {
		if strings.Contains(format.text, credential) {
			t.Errorf("%s exposed the original credential", format.name)
		}
	}
}

func TestExampleConfig(t *testing.T) {
	var got Config
	path := filepath.Join("..", "..", "..", "deploy", "config.example.toml")
	meta, err := toml.DecodeFile(path, &got)
	if err != nil {
		t.Fatal(err)
	}
	declared := configTOMLKeys()
	if keys := unknownTOMLKeys(meta.Keys(), declared); len(keys) != 0 {
		t.Errorf("example config contains unknown keys: %v", keys)
	}
	present := make(map[string]struct{}, len(meta.Keys()))
	for _, key := range meta.Keys() {
		present[key.String()] = struct{}{}
	}
	for key, isLeaf := range declared {
		if _, ok := present[key]; isLeaf && !ok {
			t.Errorf("example config is missing %s", key)
		}
	}
	if want := Default(); !reflect.DeepEqual(got, want) {
		t.Errorf("example config = %+v, want %+v", got, want)
	}
}
