package router

import (
	"log/slog"
	"net/http"

	"github.com/nynrathod/inference-guard/internal/admission"
	"github.com/nynrathod/inference-guard/internal/auth"
	"github.com/nynrathod/inference-guard/internal/handler"
	"github.com/nynrathod/inference-guard/internal/middleware"
)

// New assembles the gateway's root handler: OpenAI-compatible routes behind
// authentication and per-tenant admission control, wrapped in cross-cutting
// middleware. The access logger sits below authentication so every log line
// is attributed to a tenant, while authentication failures are reported by
// the auth layer itself.
func New(log *slog.Logger, authenticator *auth.Authenticator, admissionCtl *admission.Controller, upstream http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handler.Health)
	mux.Handle("/v1/", authenticator.Middleware(
		middleware.RequestLogger(log)(
			admissionCtl.Middleware(upstream))))

	var h http.Handler = mux
	h = middleware.Recover(log)(h)
	return h
}
