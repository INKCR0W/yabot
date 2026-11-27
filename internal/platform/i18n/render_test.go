package i18n_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/VictoriaMetrics/metrics"

	"github.com/INKCR0W/yabot/internal/platform/i18n"
)

func TestResolve(t *testing.T) {
	cases := []struct {
		name       string
		candidates []string
		want       string
	}{
		{"Chinese", []string{"zh"}, i18n.ZhHans},
		{"lowercase script", []string{"zh-hans"}, i18n.ZhHans},
		{"canonical Chinese", []string{"zh-Hans"}, i18n.ZhHans},
		{"Chinese region", []string{"zh-CN"}, i18n.ZhHans},
		{"Chinese underscore", []string{"zh_TW"}, i18n.ZhHans},
		{"traditional Chinese", []string{"ZH-HANT"}, i18n.ZhHans},
		{"English", []string{"en"}, i18n.En},
		{"English region", []string{"en-US"}, i18n.En},
		{"English underscore", []string{"EN_gb"}, i18n.En},
		{"English before Chinese", []string{"en", "zh"}, i18n.En},
		{"English region before Chinese", []string{"en-US", "zh"}, i18n.En},
		{"English underscore before Chinese", []string{"EN_gb", "zh"}, i18n.En},
		{"Chinese underscore before English", []string{"zh_TW", "en"}, i18n.ZhHans},
		{"Chinese script before English", []string{"zh-hans", "en"}, i18n.ZhHans},
		{"no candidates", nil, i18n.En},
		{"empty", []string{""}, i18n.En},
		{"unsupported", []string{"fr"}, i18n.En},
		{"unknown Chinese prefix", []string{"zhx"}, i18n.En},
		{"unknown English prefix", []string{"english", "zh"}, i18n.ZhHans},
		{"first recognized Chinese", []string{"", "fr", "zh-CN", "en-US"}, i18n.ZhHans},
		{"first recognized English", []string{"zhx", "en-US", "zh"}, i18n.En},
		{"all unsupported", []string{"", "fr", "zhx", "english"}, i18n.En},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := i18n.Resolve(tc.candidates...); got != tc.want {
				t.Errorf("Resolve(%q) = %q, want %q", tc.candidates, got, tc.want)
			}
		})
	}
}

func TestTranslate(t *testing.T) {
	const source = `hello = "Hello, {name}!"
repeat = "{first_name2}/{first_name2}"
literal = 'Literal {} { 1} {A} {2x} {x-y} } {'
mixed = '{} {name} } {'
both = "{first} {other}"
nested = 'such as {example: {name}}'
`
	cases := []struct {
		name    string
		lang    string
		key     string
		args    i18n.Args
		want    string
		keyMiss int
		argMiss int
	}{
		{"replacement", i18n.En, "sample.hello", i18n.Args{"name": "Lee"}, "Hello, Lee!", 0, 0},
		{"repeated replacement", i18n.En, "sample.repeat", i18n.Args{"first_name2": "Lee"}, "Lee/Lee", 0, 0},
		{"extra arguments", i18n.En, "sample.hello", i18n.Args{"name": "Lee", "unused": "ignored"}, "Hello, Lee!", 0, 0},
		{"missing argument", i18n.En, "sample.hello", nil, "Hello, {name}!", 0, 1},
		{"repeated missing argument", i18n.En, "sample.repeat", nil, "{first_name2}/{first_name2}", 0, 2},
		{"empty argument supplied", i18n.En, "sample.hello", i18n.Args{"name": ""}, "Hello, !", 0, 0},
		{"malformed placeholders", i18n.En, "sample.literal", nil, "Literal {} { 1} {A} {2x} {x-y} } {", 0, 0},
		{"valid and malformed braces", i18n.En, "sample.mixed", i18n.Args{"name": "Lee"}, "{} Lee } {", 0, 0},
		{"placeholder inside literal brace", i18n.En, "sample.nested", i18n.Args{"name": "Lee"}, "such as {example: Lee}", 0, 0},
		{"argument is literal", i18n.En, "sample.both", i18n.Args{"first": "{other}", "other": "Lee"}, "{other} Lee", 0, 0},
		{"unsupported language", "fr", "sample.hello", i18n.Args{"name": "Lee"}, "Hello, Lee!", 0, 0},
		{"normalized language", "ZH_hant", "sample.hello", i18n.Args{"name": "小李"}, "你好，小李！", 0, 0},
		{"missing key", i18n.En, "sample.missing", nil, "sample.missing", 1, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			set := metrics.NewSet()
			catalog := i18n.New(i18n.Deps{Metrics: set})
			zh := strings.Replace(source, "Hello, {name}!", "你好，{name}！", 1)
			register(t, catalog, "sample", languageFiles(zh, source))
			if got := catalog.T(tc.lang, tc.key, tc.args); got != tc.want {
				t.Errorf("T(%q, %q) = %q, want %q", tc.lang, tc.key, got, tc.want)
			}
			assertMissingCount(t, set, "key", tc.keyMiss)
			assertMissingCount(t, set, "arg", tc.argMiss)
		})
	}
}

func TestHTML(t *testing.T) {
	const (
		source = `intro = '<b>{name}</b> &amp; <i>{detail}</i>'
markup = '<b>Ready</b>'`
		input = `<>&"'`
	)
	cases := []struct {
		name    string
		lang    string
		key     string
		args    i18n.Args
		want    string
		wantRaw string
		keyMiss int
		argMiss int
	}{
		{"escape arguments only", "en-US", "sample.intro", i18n.Args{"name": input, "detail": input},
			"<b>&lt;&gt;&amp;&#34;&#39;</b> &amp; <i>&lt;&gt;&amp;&#34;&#39;</i>",
			`<b><>&"'</b> &amp; <i><>&"'</i>`, 0, 0},
		{"markup without arguments", i18n.En, "sample.markup", nil, "<b>Ready</b>", "<b>Ready</b>", 0, 0},
		{"preserve missing argument", i18n.En, "sample.intro", i18n.Args{"name": "Lee"},
			"<b>Lee</b> &amp; <i>{detail}</i>", "<b>Lee</b> &amp; <i>{detail}</i>", 0, 2},
		{"preserve missing key", i18n.En, "sample.<missing>", nil,
			"sample.<missing>", "sample.<missing>", 2, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			set := metrics.NewSet()
			catalog := i18n.New(i18n.Deps{Metrics: set})
			register(t, catalog, "sample", languageFiles(source, source))
			if got := catalog.HTML(tc.lang, tc.key, tc.args); got != tc.want {
				t.Errorf("HTML(%q, %q) = %q, want %q", tc.lang, tc.key, got, tc.want)
			}
			if got := catalog.T(tc.lang, tc.key, tc.args); got != tc.wantRaw {
				t.Errorf("T(%q, %q) = %q, want %q", tc.lang, tc.key, got, tc.wantRaw)
			}
			assertMissingCount(t, set, "key", tc.keyMiss)
			assertMissingCount(t, set, "arg", tc.argMiss)
		})
	}
}

func TestHTMLSelectsLanguage(t *testing.T) {
	catalog := i18n.New(i18n.Deps{})
	register(t, catalog, "sample", languageFiles(
		`greet = '<b>{name}</b>，欢迎'`,
		`greet = 'Welcome, <b>{name}</b>'`,
	))
	args := i18n.Args{"name": "<Lee>"}
	for _, tc := range []struct{ lang, want string }{
		{"ZH_hant", "<b>&lt;Lee&gt;</b>，欢迎"},
		{"en-GB", "Welcome, <b>&lt;Lee&gt;</b>"},
		{"fr", "Welcome, <b>&lt;Lee&gt;</b>"},
	} {
		if got := catalog.HTML(tc.lang, "sample.greet", args); got != tc.want {
			t.Errorf("HTML(%q) = %q, want %q", tc.lang, got, tc.want)
		}
	}
	if got, want := catalog.T("zh", "sample.greet", args), "<b><Lee></b>，欢迎"; got != want {
		t.Errorf("T(zh) = %q, want %q", got, want)
	}
}

func assertMissingCount(t *testing.T, set *metrics.Set, kind string, want int) {
	t.Helper()
	var out strings.Builder
	set.WritePrometheus(&out)
	metricName := fmt.Sprintf(`yabot_i18n_missing_total{kind=%q}`, kind)
	for line := range strings.Lines(out.String()) {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == metricName {
			if got := fields[1]; got != strconv.Itoa(want) {
				t.Errorf("%s = %s, want %d", metricName, got, want)
			}
			return
		}
	}
	if want != 0 {
		t.Errorf("missing metric %s, want %d; exposition = %q", metricName, want, out.String())
	}
}

type missingLog struct {
	Level string `json:"level"`
	Kind  string `json:"kind"`
	Key   string `json:"key"`
	Arg   string `json:"arg"`
}

func readMissingLogs(t *testing.T, out *bytes.Buffer) []missingLog {
	t.Helper()
	var logs []missingLog
	decoder := json.NewDecoder(out)
	for {
		var entry missingLog
		err := decoder.Decode(&entry)
		if errors.Is(err, io.EOF) {
			return logs
		}
		if err != nil {
			t.Fatalf("decode missing translation log: %v", err)
		}
		logs = append(logs, entry)
	}
}

func TestMissingTranslationsLogOncePerKey(t *testing.T) {
	const source = `value = "{name}, {other}, {name}"`
	cases := []struct {
		name         string
		missBefore   bool
		missKind     string
		missArg      string
		wantKeyCount int
	}{
		{"missing arguments", false, "arg", "name", 0},
		{"key then arguments", true, "key", "", 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			set := metrics.NewSet()
			log := slog.New(slog.NewJSONHandler(&out, nil))
			catalog := i18n.New(i18n.Deps{Metrics: set, Log: log})
			const key = "sample.value"
			if tc.missBefore {
				catalog.T(i18n.En, key, nil)
				catalog.HTML(i18n.ZhHans, key, nil)
			}
			register(t, catalog, "sample", languageFiles(source, source))
			catalog.T(i18n.En, key, nil)
			catalog.HTML(i18n.ZhHans, key, i18n.Args{"name": "Lee"})

			logs := readMissingLogs(t, &out)
			want := missingLog{Level: "ERROR", Kind: tc.missKind, Key: key, Arg: tc.missArg}
			if len(logs) != 1 || logs[0] != want {
				t.Errorf("logs = %+v, want only %+v", logs, want)
			}
			assertMissingCount(t, set, "key", tc.wantKeyCount)
			assertMissingCount(t, set, "arg", 4)
		})
	}
}

func TestMissingTranslationLogCapacity(t *testing.T) {
	const capacity = 256
	cases := []struct {
		name string
		fill int
		want int
	}{
		{"below capacity", capacity - 1, capacity},
		{"at capacity", capacity, capacity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			set := metrics.NewSet()
			log := slog.New(slog.NewJSONHandler(&out, nil))
			catalog := i18n.New(i18n.Deps{Metrics: set, Log: log})
			const source = `value = "Hello {name}"`
			register(t, catalog, "sample", languageFiles(source, source))
			for i := range tc.fill {
				catalog.T(i18n.En, fmt.Sprintf("sample.missing_%d", i), nil)
			}
			catalog.T(i18n.En, "sample.missing_0", nil)
			catalog.HTML(i18n.En, "sample.value", nil)
			catalog.HTML(i18n.ZhHans, "sample.value", nil)
			catalog.T(i18n.En, "sample.after_capacity", nil)

			logs := readMissingLogs(t, &out)
			if len(logs) != tc.want {
				t.Fatalf("log count = %d, want %d", len(logs), tc.want)
			}
			for i, entry := range logs[:tc.fill] {
				want := missingLog{Level: "ERROR", Kind: "key", Key: fmt.Sprintf("sample.missing_%d", i)}
				if entry != want {
					t.Errorf("log %d = %+v, want %+v", i, entry, want)
				}
			}
			if tc.fill < capacity {
				want := missingLog{Level: "ERROR", Kind: "arg", Key: "sample.value", Arg: "name"}
				if got := logs[tc.fill]; got != want {
					t.Errorf("final log = %+v, want %+v", got, want)
				}
			}
			assertMissingCount(t, set, "key", tc.fill+2)
			assertMissingCount(t, set, "arg", 2)
		})
	}
}

func TestConcurrentMissingKeyLogs(t *testing.T) {
	var out bytes.Buffer
	set := metrics.NewSet()
	log := slog.New(slog.NewJSONHandler(&out, nil))
	catalog := i18n.New(i18n.Deps{Metrics: set, Log: log})
	const (
		key     = "sample.missing"
		readers = 16
	)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range readers {
		workers.Go(func() {
			<-start
			if got := catalog.T(i18n.En, key, nil); got != key {
				t.Errorf("T(%q) = %q, want key", key, got)
			}
		})
	}
	close(start)
	workers.Wait()
	logs := readMissingLogs(t, &out)
	want := missingLog{Level: "ERROR", Kind: "key", Key: key}
	if len(logs) != 1 || logs[0] != want {
		t.Errorf("logs = %+v, want only %+v", logs, want)
	}
	assertMissingCount(t, set, "key", readers)
}
