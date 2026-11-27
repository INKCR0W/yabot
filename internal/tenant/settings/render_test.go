package settings

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/INKCR0W/yabot/internal/platform/i18n"
)

func TestMain(m *testing.M) {
	flag.Bool("update", false, "update the settings schema golden file")
	flag.Parse()
	os.Exit(m.Run())
}

func TestRenderSettingsSchemaGolden(t *testing.T) {
	r := newRenderRegistry(t)
	Register[renderFixtureSettings](r, renderFixtureSpec("example"))
	r.Freeze()

	rendered := r.Render(-42, i18n.En)
	got := marshalSchema(t, rendered)
	got = append(got, '\n')
	path := filepath.Join("..", "..", "..", "testdata", "settings_schema.golden.json")
	if flag.Lookup("update").Value.String() == "true" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("rendered schema differs from %s\n got:\n%s\nwant:\n%s", path, got, want)
	}

	spec := &rendered.Specs[0]
	spec.Groups[0].Title = "modified"
	spec.Fields[0].Default[0] = 'x'
	spec.Fields[1].Options[0].Label = "modified"
	*spec.Fields[2].Min = -1
	spec.Fields[2].VisibleIf.All[0].Eq[0] = 'x'
	spec.Fields[2].VisibleIf.All[1].In[0][0] = 'x'
	spec.Values["enabled"][0] = 'x'
	if again := append(marshalSchema(t, r.Render(-42, i18n.En)), '\n'); !bytes.Equal(again, want) {
		t.Fatalf("rendered schema changed between calls:\n%s", again)
	}
}

func TestRenderPreservesSpecOrderAndLanguage(t *testing.T) {
	r := newRenderRegistry(t)
	Register[renderFixtureSettings](r, renderFixtureSpec("z_example"))
	Register[renderFixtureSettings](r, renderFixtureSpec("a_example"))
	r.Freeze()
	updated, err := r.Patch(t.Context(), "z_example", -42, Actor{}, map[string]json.RawMessage{
		"enabled": json.RawMessage(`false`),
	}, `"0"`)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		lang, title, group, label, help, option string
	}{
		{i18n.En, "Example settings", "General", "Enabled", "Turn this feature on or off.", "Normal"},
		{i18n.ZhHans, "示例设置", "常规", "启用", "开启或关闭此功能。", "普通"},
	} {
		t.Run(tc.lang, func(t *testing.T) {
			response := r.Render(-42, tc.lang)
			if len(response.Specs) != 2 || response.Specs[0].Feature != "z_example" || response.Specs[1].Feature != "a_example" {
				t.Fatalf("rendered Specs are out of registration order: %v", response.Specs)
			}
			if spec := response.Specs[0]; spec.ETag != updated.ETag || string(spec.Values["enabled"]) != "false" {
				t.Errorf("rendered update = (%q, %s), want (%q, false)", spec.ETag, spec.Values["enabled"], updated.ETag)
			}
			if spec := response.Specs[1]; spec.ETag != `"0"` || string(spec.Values["enabled"]) != "true" {
				t.Errorf("rendered defaults = (%q, %s), want (%q, true)", spec.ETag, spec.Values["enabled"], `"0"`)
			}
			for _, spec := range response.Specs {
				if spec.Title != tc.title || spec.Groups[0].Title != tc.group || spec.Fields[0].Label != tc.label ||
					spec.Fields[0].Help != tc.help || spec.Fields[1].Options[0].Label != tc.option {
					t.Errorf("rendered translations for %s: %+v", tc.lang, spec)
				}
			}
		})
	}
}

func TestLocalizePatchFieldFailures(t *testing.T) {
	r := newRenderRegistry(t)
	spec := renderFixtureSpec("example")
	spec.Validate = func(v any) []FieldError {
		config := v.(*renderFixtureSettings)
		if !config.Enabled && config.Mode == "strict" {
			return []FieldError{{Key: "mode", Message: "example.mode_invalid", Args: i18n.Args{"mode": config.Mode}}}
		}
		return nil
	}
	Register[renderFixtureSettings](r, spec)
	r.Freeze()

	result, err := r.Patch(t.Context(), "example", -42, Actor{}, map[string]json.RawMessage{
		"enabled": json.RawMessage(`false`),
		"limit":   json.RawMessage(`2.0`),
		"mode":    json.RawMessage(`"strict"`),
		"typo":    json.RawMessage(`0`),
	}, `"0"`)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Patch returned error %v, want %v", err, ErrInvalid)
	}
	en := r.LocalizeFields(i18n.En, result)
	zh := r.LocalizeFields(i18n.ZhHans, result)
	if len(en) != 3 || len(zh) != 3 {
		t.Fatalf("expected failures only, got English %v and Chinese %v", en, zh)
	}
	encoded, err := json.Marshal(en)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]map[string]string
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ key, code string }{
		{"limit", "invalid_value"},
		{"mode", "invalid_value"},
		{"typo", "unknown_key"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			if got := wire[tc.key]; len(got) != 2 || got["code"] != tc.code || got["message"] != en[tc.key].Message {
				t.Fatalf("JSON field failure = %v, want code %q and message", got, tc.code)
			}
			if string(zh[tc.key].Code) != tc.code || en[tc.key].Message == "" || zh[tc.key].Message == "" || en[tc.key].Message == zh[tc.key].Message {
				t.Errorf("expected different localized messages with code %q, got English %v and Chinese %v", tc.code, en[tc.key], zh[tc.key])
			}
		})
	}
	if en["mode"].Message != "Mode strict requires this feature to be enabled." || zh["mode"].Message != "模式 strict 要求启用此功能。" {
		t.Errorf("validation argument was not localized: English %v, Chinese %v", en["mode"], zh["mode"])
	}
}

func marshalSchema(t *testing.T, rendered Schema) []byte {
	t.Helper()
	data, err := json.MarshalIndent(rendered, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func newRenderRegistry(t *testing.T) *Registry {
	t.Helper()
	catalog := i18n.New(i18n.Deps{})
	if err := catalog.Register("example", fstest.MapFS{
		"en.toml": {Data: []byte(`
title = "Example settings"
general = "General"
limits = "Limits"
enabled = "Enabled"
enabled_help = "Turn this feature on or off."
mode = "Mode"
mode_invalid = "Mode {mode} requires this feature to be enabled."
normal = "Normal"
strict = "Strict"
timeout = "Timeout"
limit = "Limit"
`)},
		"zh-Hans.toml": {Data: []byte(`
title = "示例设置"
general = "常规"
limits = "限制"
enabled = "启用"
enabled_help = "开启或关闭此功能。"
mode = "模式"
mode_invalid = "模式 {mode} 要求启用此功能。"
normal = "普通"
strict = "严格"
timeout = "超时"
limit = "上限"
`)},
	}); err != nil {
		t.Fatal(err)
	}
	r, _ := newTestRegistry(t, func(d *Deps) { d.Catalog = catalog })
	return r
}

type renderFixtureSettings struct {
	Enabled bool    `json:"enabled"`
	Mode    string  `json:"mode"`
	Timeout Seconds `json:"timeout"`
	Limit   int     `json:"limit"`
}

func renderFixtureSpec(feature string) Spec {
	var minTimeout, maxTimeout int64 = 60, 1800
	var minLimit, maxLimit int64 = 0, 20
	return Spec{
		Feature: feature, PermGroup: feature, Version: 1, Title: "example.title",
		Groups: []GroupMeta{{ID: "general", Title: "example.general"}, {ID: "limits", Title: "example.limits"}},
		Fields: []Field{
			{Key: "enabled", Type: FieldBool, Group: "general", Label: "example.enabled", Help: "example.enabled_help", Default: json.RawMessage(`true`)},
			{
				Key: "mode", Type: FieldEnum, Group: "general", Label: "example.mode", Default: json.RawMessage(`"normal"`),
				Options: []Option{{Value: "normal", Label: "example.normal"}, {Value: "strict", Label: "example.strict"}},
			},
			{
				Key: "timeout", Type: FieldDuration, Group: "limits", Label: "example.timeout", Default: json.RawMessage(`300`),
				Min: &minTimeout, Max: &maxTimeout, Step: 30,
				VisibleIf: &Cond{All: []Clause{
					{Field: "enabled", Eq: json.RawMessage(`true`)},
					{Field: "mode", In: []json.RawMessage{json.RawMessage(`"strict"`)}},
				}},
			},
			{
				Key: "limit", Type: FieldInt, Group: "limits", Label: "example.limit", Default: json.RawMessage(`2`),
				Min: &minLimit, Max: &maxLimit, Step: 2,
			},
		},
	}
}
