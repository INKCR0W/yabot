package api

import (
	"embed"
	"net/http"
	"strings"

	"github.com/INKCR0W/yabot/internal/platform/httpx"
	"github.com/INKCR0W/yabot/internal/platform/i18n"
)

const (
	CodeBadRequest   = "bad_request"
	CodeUnauthorized = "unauthorized"
	CodeForbidden    = "forbidden"
	CodeBotNotAdmin  = "bot_not_admin"
	CodeNotFound     = "not_found"
	CodeConflict     = "conflict"
	CodeRateLimited  = "rate_limited"
	CodeBusy         = "busy"
	CodeInvalid      = "invalid"
	CodeInternal     = "internal"
)

//go:embed zh-Hans.toml en.toml
var translations embed.FS

// Error writes a localized response. The code must identify a bundled translation.
func (a *API) Error(w http.ResponseWriter, r *http.Request, status int, code string) {
	a.writeError(w, requestLanguage(r), status, code)
}

// ErrorBody is the shared error object. Responses that carry more than the error, such as
// per-field validation results, put it under "error" and write the whole value with WriteJSON.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ErrorFor returns the localized error object for code without writing a response.
func (a *API) ErrorFor(r *http.Request, code string) ErrorBody {
	return a.errorBody(requestLanguage(r), code)
}

func (a *API) errorBody(lang, code string) ErrorBody {
	return ErrorBody{Code: code, Message: a.catalog.T(lang, "api.error."+code, nil)}
}

func (a *API) writeError(w http.ResponseWriter, lang string, status int, code string) {
	body := a.errorBody(lang, code)
	httpx.WriteError(w, status, body.Code, body.Message)
}

func requestLanguage(r *http.Request) string {
	if identity, ok := IdentityFrom(r.Context()); ok {
		return i18n.Resolve(identity.Lang)
	}
	first, _, _ := strings.Cut(r.Header.Get("Accept-Language"), ",")
	tag, _, _ := strings.Cut(first, ";")
	return i18n.Resolve(strings.TrimSpace(tag))
}
