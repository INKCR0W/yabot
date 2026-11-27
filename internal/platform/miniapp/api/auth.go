package api

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/INKCR0W/yabot/internal/platform/httpx"
	"github.com/INKCR0W/yabot/internal/platform/miniapp/initdata"
)

const (
	authMissing   = "missing"
	authMalformed = "malformed"
	authBadHash   = "bad_hash"
	authExpired   = "expired"
	authFuture    = "future"
	authNoUser    = "no_user"
)

type identityKey struct{}

func IdentityFrom(ctx context.Context) (initdata.Identity, bool) {
	identity, ok := ctx.Value(identityKey{}).(initdata.Identity)
	return identity, ok
}

func (a *API) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization := r.Header.Values("Authorization")
		if len(authorization) == 0 {
			a.rejectAuth(w, r, authMissing)
			return
		}
		scheme, raw, found := strings.Cut(authorization[0], " ")
		if len(authorization) != 1 || !found || !strings.EqualFold(scheme, "tma") ||
			raw == "" || raw[0] == ' ' || raw[0] == '\t' {
			a.rejectAuth(w, r, authMalformed)
			return
		}
		identity, err := initdata.Verify(raw, a.botToken, a.now(), a.maxAge)
		if err != nil {
			if reason := authFailureReason(err); reason != "" {
				a.rejectAuth(w, r, reason)
			} else {
				a.authErrors.Inc()
				a.log.ErrorContext(r.Context(), "miniapp initData verification failed unexpectedly",
					"request_id", httpx.RequestID(r.Context()))
				a.Error(w, r, http.StatusInternalServerError, CodeInternal)
			}
			return
		}
		ctx := context.WithValue(r.Context(), identityKey{}, identity)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (a *API) rejectAuth(w http.ResponseWriter, r *http.Request, reason string) {
	a.authFailures[reason].Inc()
	a.log.InfoContext(r.Context(), "miniapp authentication failed",
		"reason", reason, "request_id", httpx.RequestID(r.Context()))
	a.Error(w, r, http.StatusUnauthorized, CodeUnauthorized)
}

func authFailureReason(err error) string {
	switch {
	case errors.Is(err, initdata.ErrMalformed):
		return authMalformed
	case errors.Is(err, initdata.ErrMissingHash), errors.Is(err, initdata.ErrBadHash):
		return authBadHash
	case errors.Is(err, initdata.ErrExpired):
		return authExpired
	case errors.Is(err, initdata.ErrFuture):
		return authFuture
	case errors.Is(err, initdata.ErrNoUser):
		return authNoUser
	default:
		return ""
	}
}
