package i18n_test

import (
	"fmt"
	"io/fs"
	"os"
	"sync"
	"testing"
	"testing/fstest"
	"testing/synctest"

	"github.com/INKCR0W/yabot/internal/platform/i18n"
)

func languageFiles(zh, en string) fstest.MapFS {
	return fstest.MapFS{
		"zh-Hans.toml": {Data: []byte(zh)},
		"en.toml":      {Data: []byte(en)},
	}
}

func register(t *testing.T, catalog *i18n.Catalog, namespace string, files fs.FS) {
	t.Helper()
	if err := catalog.Register(namespace, files); err != nil {
		t.Fatalf("Register(%q): %v", namespace, err)
	}
}

func TestRegisterLanguageFiles(t *testing.T) {
	catalog := i18n.New(i18n.Deps{})
	register(t, catalog, "sample", os.DirFS("testdata"))
	register(t, catalog, "another_2", languageFiles(
		"[welcome]\ntitle = \"问候\"",
		"[welcome]\ntitle = \"Greetings\"",
	))

	cases := []struct {
		name string
		lang string
		key  string
		args i18n.Args
		want string
		has  bool
	}{
		{"nested English", i18n.En, "sample.welcome.title", nil, "Welcome", true},
		{"nested Chinese", i18n.ZhHans, "sample.welcome.title", nil, "欢迎", true},
		{"deep English", i18n.En, "sample.welcome.notice.message", i18n.Args{"name": "Lee", "chat": "Go"}, "Welcome to Go, <b>Lee</b>!", true},
		{"deep Chinese", i18n.ZhHans, "sample.welcome.notice.message", i18n.Args{"name": "小李", "chat": "Go"}, "<b>小李</b>，欢迎来到Go！", true},
		{"different placeholder multiplicity", i18n.ZhHans, "sample.welcome.notice.reminder", i18n.Args{"name": "小李"}, "小李，小李，请查看通知。", true},
		{"second namespace", i18n.En, "another_2.welcome.title", nil, "Greetings", true},
		{"empty translation", i18n.En, "sample.welcome.empty", nil, "", true},
		{"unqualified key", i18n.En, "welcome.title", nil, "welcome.title", false},
		{"foreign key", i18n.En, "another_2.welcome.notice.message", nil, "another_2.welcome.notice.message", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := catalog.T(tc.lang, tc.key, tc.args); got != tc.want {
				t.Errorf("T(%q, %q) = %q, want %q", tc.lang, tc.key, got, tc.want)
			}
			if got := catalog.Has(tc.key); got != tc.has {
				t.Errorf("Has(%q) = %t, want %t", tc.key, got, tc.has)
			}
		})
	}
}

func TestRegisterRejectsWithoutPublishing(t *testing.T) {
	const (
		zhReady = "ready = \"准备\"\n"
		enReady = "ready = \"Ready\"\n"
	)
	ready := languageFiles(zhReady, enReady)
	cases := []struct {
		name      string
		namespace string
		files     fs.FS
	}{
		{"empty namespace", "", ready},
		{"uppercase namespace", "Invalid", ready},
		{"numeric namespace start", "1invalid", ready},
		{"underscore namespace start", "_invalid", ready},
		{"dotted namespace", "not.valid", ready},
		{"hyphen in namespace", "not-valid", ready},
		{"duplicate namespace", "base", languageFiles(zhReady+"stable = \"替换\"", enReady+"stable = \"Replacement\"")},
		{"missing Chinese file", "candidate", fstest.MapFS{"en.toml": {Data: []byte(enReady)}}},
		{"missing English file", "candidate", fstest.MapFS{"zh-Hans.toml": {Data: []byte(zhReady)}}},
		{"extra Chinese key", "candidate", languageFiles(zhReady+"zh_only = \"额外\"", enReady)},
		{"extra English key", "candidate", languageFiles(zhReady, enReady+"en_only = \"Extra\"")},
		{"different keys with equal counts", "candidate", languageFiles(zhReady+"zh_only = \"中文\"", enReady+"en_only = \"English\"")},
		{"extra Chinese placeholder", "candidate", languageFiles(zhReady+"notice = \"{name} {extra}\"", enReady+"notice = \"{name}\"")},
		{"extra English placeholder", "candidate", languageFiles(zhReady+"notice = \"{name}\"", enReady+"notice = \"{name} {extra}\"")},
		{"different placeholders with equal counts", "candidate", languageFiles(zhReady+"notice = \"{first}\"", enReady+"notice = \"{second}\"")},
		{"placeholders swapped between keys", "candidate", languageFiles(zhReady+"notice = \"{name}\"\nwhere = \"{chat}\"", enReady+"notice = \"{chat}\"\nwhere = \"{name}\"")},
		{"numeric value", "candidate", languageFiles(zhReady+"value = 42", enReady+"value = \"42\"")},
		{"array value", "candidate", languageFiles(zhReady+"value = \"item\"", enReady+"value = [\"item\"]")},
		{"boolean value", "candidate", languageFiles(zhReady+"value = true", enReady+"value = \"true\"")},
		{"syntax error in Chinese", "candidate", languageFiles(zhReady+"value = ", enReady+"value = \"ok\"")},
		{"syntax error in English", "candidate", languageFiles(zhReady+"value = \"好\"", enReady+"value = [")},
		{"ambiguous flattened key", "candidate", languageFiles(zhReady+"\"nested.value\" = \"literal\"\n[nested]\nvalue = \"table\"", enReady+"[nested]\nvalue = \"table\"")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			catalog := i18n.New(i18n.Deps{})
			register(t, catalog, "base", languageFiles("stable = \"保留\"", "stable = \"Keep\""))
			if err := catalog.Register(tc.namespace, tc.files); err == nil {
				t.Error("Register accepted invalid translations")
			}
			for lang, want := range map[string]string{i18n.ZhHans: "保留", i18n.En: "Keep"} {
				if got := catalog.T(lang, "base.stable", nil); got != want {
					t.Errorf("T(%q, base.stable) = %q after failed Register, want %q", lang, got, want)
				}
			}
			for _, leaf := range []string{"ready", "zh_only", "en_only", "notice", "where", "value", "nested.value"} {
				key := tc.namespace + "." + leaf
				if catalog.Has(key) {
					t.Errorf("Has(%q) after failed Register", key)
				}
				for _, lang := range []string{i18n.ZhHans, i18n.En} {
					if got := catalog.T(lang, key, nil); got != key {
						t.Errorf("T(%q, %q) = %q after failed Register, want key", lang, key, got)
					}
				}
			}
			if tc.namespace == "candidate" {
				register(t, catalog, tc.namespace, ready)
				if got := catalog.T(i18n.En, "candidate.ready", nil); got != "Ready" {
					t.Errorf("T after retry = %q, want Ready", got)
				}
			}
		})
	}
}

type gatedFS struct {
	fs.FS
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *gatedFS) Open(name string) (fs.File, error) {
	if name == "en.toml" {
		f.once.Do(func() { close(f.entered) })
		<-f.release
	}
	return f.FS.Open(name)
}

func TestLookupsDuringRegistration(t *testing.T) {
	cases := []struct {
		name, en, wantZH, wantEN string
		wantErr                  bool
	}{
		{"success", "value = \"New\"", "新增", "New", false},
		{"failure", "other = \"Unmatched\"", "candidate.value", "candidate.value", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				catalog := i18n.New(i18n.Deps{})
				register(t, catalog, "base", languageFiles("stable = \"保留\"", "stable = \"Keep\""))
				files := &gatedFS{
					FS:      languageFiles("value = \"新增\"", tc.en),
					entered: make(chan struct{}),
					release: make(chan struct{}),
				}
				result := make(chan error, 1)
				go func() { result <- catalog.Register("candidate", files) }()
				<-files.entered

				var readers sync.WaitGroup
				for range 8 {
					readers.Go(func() {
						if got := catalog.T(i18n.En, "base.stable", nil); got != "Keep" {
							t.Errorf("T during Register = %q, want Keep", got)
						}
						if got := catalog.HTML(i18n.ZhHans, "candidate.value", nil); got != "candidate.value" {
							t.Errorf("HTML of pending key = %q, want key", got)
						}
					})
				}
				readers.Wait()
				close(files.release)
				if err := <-result; (err != nil) != tc.wantErr {
					t.Errorf("Register returned %v, want error = %t", err, tc.wantErr)
				}
				for lang, want := range map[string]string{i18n.ZhHans: tc.wantZH, i18n.En: tc.wantEN} {
					if got := catalog.T(lang, "candidate.value", nil); got != want {
						t.Errorf("T(%q, candidate.value) after Register = %q, want %q", lang, got, want)
					}
				}
			})
		})
	}
}

func TestConcurrentRegistrations(t *testing.T) {
	const workerCount = 8
	cases := []struct {
		name          string
		namespace     func(int) string
		wantSuccesses int
	}{
		{"distinct namespaces", func(i int) string { return fmt.Sprintf("bundle_%d", i) }, workerCount},
		{"same namespace", func(int) string { return "bundle" }, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			catalog := i18n.New(i18n.Deps{})
			register(t, catalog, "base", languageFiles("stable = \"保留\"", "stable = \"Keep\""))
			start := make(chan struct{})
			var workers sync.WaitGroup
			results := make([]error, workerCount)
			for i := range workerCount {
				workers.Go(func() {
					<-start
					files := languageFiles(fmt.Sprintf("value = \"中文 %d\"", i), fmt.Sprintf("value = \"English %d\"", i))
					results[i] = catalog.Register(tc.namespace(i), files)
				})
			}
			for range workerCount {
				workers.Go(func() {
					<-start
					for range 32 {
						if got := catalog.T(i18n.En, "base.stable", nil); got != "Keep" {
							t.Errorf("T during concurrent registrations = %q, want Keep", got)
						}
					}
				})
			}
			close(start)
			workers.Wait()
			var successes int
			for i, err := range results {
				if err != nil {
					continue
				}
				successes++
				key := tc.namespace(i) + ".value"
				for lang, want := range map[string]string{i18n.ZhHans: fmt.Sprintf("中文 %d", i), i18n.En: fmt.Sprintf("English %d", i)} {
					if got := catalog.T(lang, key, nil); got != want {
						t.Errorf("T(%q, %q) = %q, want %q", lang, key, got, want)
					}
				}
			}
			if successes != tc.wantSuccesses {
				t.Errorf("successful registrations = %d, want %d (%v)", successes, tc.wantSuccesses, results)
			}
		})
	}
}
