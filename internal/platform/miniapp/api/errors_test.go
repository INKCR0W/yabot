package api

import (
	"net/http"
	"strings"
	"testing"

	"github.com/INKCR0W/yabot/internal/platform/i18n"
)

func TestErrorsUseOnlyTheApplicableLanguage(t *testing.T) {
	for _, tc := range []struct {
		name          string
		authenticated bool
		identityLang  string
		accept        string
		message       string
	}{
		{"Chinese identity", true, "zh", "en", "没有访问权限。"},
		{"English identity", true, "en", "zh-CN", "Access denied."},
		{"unsupported identity", true, "fr", "zh-CN", "Access denied."},
		{"empty identity language", true, "", "zh-CN", "Access denied."},
		{"first Chinese tag", false, "", "zh-CN,zh;q=0.9,en;q=0.8", "身份验证失败。"},
		{"first English tag", false, "", "en-US;q=0.1,zh;q=1", "Authentication failed."},
		{"first unsupported tag", false, "", "fr,zh;q=0.9", "Authentication failed."},
		{"no language header", false, "", "", "Authentication failed."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, mux, _, _ := newTestAPI(t)
			a.Mount(testRoutes{{http.MethodGet, "/restricted", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				a.Error(w, r, http.StatusForbidden, CodeForbidden)
			})}})
			r := makeRequest(http.MethodGet, "/api/v1/restricted", "")
			if tc.authenticated {
				r.Header.Set("Authorization", validAuthorization(t, 42, tc.identityLang))
			}
			if tc.accept != "" {
				r.Header.Set("Accept-Language", tc.accept)
			}
			status, code := http.StatusUnauthorized, "unauthorized"
			if tc.authenticated {
				status, code = http.StatusForbidden, "forbidden"
			}
			assertAPIError(t, serve(mux, r), status, code, tc.message)
		})
	}
}

func TestEmbeddedTranslationsCoverExportedErrorCodes(t *testing.T) {
	catalog := i18n.New(i18n.Deps{})
	if err := catalog.Register("api", translations); err != nil {
		t.Fatal(err)
	}
	codes := []string{
		CodeBadRequest,
		CodeUnauthorized,
		CodeForbidden,
		CodeBotNotAdmin,
		CodeNotFound,
		CodeConflict,
		CodeRateLimited,
		CodeBusy,
		CodeInvalid,
		CodeInternal,
	}
	for _, code := range codes {
		key := "api.error." + code
		for _, lang := range []string{i18n.ZhHans, i18n.En} {
			if message := catalog.T(lang, key, nil); message == key || strings.TrimSpace(message) == "" {
				t.Errorf("missing translation for %s in %s", key, lang)
			}
		}
	}
}

func TestNewRejectsDuplicateTranslationNamespace(t *testing.T) {
	d, mux, _, _ := testDeps()
	if err := d.Catalog.Register("api", translations); err != nil {
		t.Fatal(err)
	}
	if a, err := New(d); a != nil || err == nil {
		t.Fatal("New succeeded with a duplicate translation namespace")
	}
	_, pattern := mux.Handler(makeRequest(http.MethodGet, "/api/v1/missing", ""))
	if pattern != "" {
		t.Fatalf("failed construction registered pattern %q", pattern)
	}
}
