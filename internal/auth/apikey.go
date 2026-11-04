package auth

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/nynrathod/inference-guard/internal/apierror"
)

type ctxKey struct{}

// Authenticator resolves API keys to tenants and guards tenant-facing routes.
type Authenticator struct {
	keys map[string]string
	log  *slog.Logger
}

// New returns an Authenticator backed by the given key-to-tenant mappings.
func New(keys map[string]string, log *slog.Logger) *Authenticator {
	return &Authenticator{keys: keys, log: log}
}

// Middleware rejects requests that do not carry a valid bearer token and
// binds the resolved tenant to the request context.
func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r)
		if !ok {
			a.reject(w, r, "missing API key")
			return
		}
		tenant, ok := a.keys[token]
		if !ok {
			a.reject(w, r, "invalid API key")
			return
		}
		ctx := context.WithValue(r.Context(), ctxKey{}, tenant)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (a *Authenticator) reject(w http.ResponseWriter, r *http.Request, reason string) {
	a.log.Warn("request rejected",
		"method", r.Method, "path", r.URL.Path, "remote", remoteIP(r), "reason", reason)
	w.Header().Set("WWW-Authenticate", "Bearer")
	apierror.Write(w, http.StatusUnauthorized, "invalid_request_error", "invalid_api_key", reason)
}

func remoteIP(r *http.Request) string {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

// TenantFromContext returns the tenant bound to the request by Middleware.
func TenantFromContext(ctx context.Context) (string, bool) {
	tenant, ok := ctx.Value(ctxKey{}).(string)
	return tenant, ok
}

func bearerToken(r *http.Request) (string, bool) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return "", false
	}
	token = strings.TrimSpace(token)
	return token, token != ""
}
