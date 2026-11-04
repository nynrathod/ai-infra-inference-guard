package router

import (
	"log/slog"
	"net/http"

	"github.com/nynrathod/inference-guard/internal/admission"
	"github.com/nynrathod/inference-guard/internal/auth"
	"github.com/nynrathod/inference-guard/internal/handler"
	"github.com/nynrathod/inference-guard/internal/metering"
	"github.com/nynrathod/inference-guard/internal/metrics"
	"github.com/nynrathod/inference-guard/internal/middleware"
)

// New assembles the gateway's root handler: OpenAI-compatible routes behind
// authentication, per-tenant admission control and per-tenant token budgets,
// wrapped in cross-cutting middleware. Metering sits directly above the
// proxy (only admitted requests are metered, and it needs the tenant from
// auth), while metrics and the access logger stay above so budget
// rejections are logged and counted like any other response.
func New(log *slog.Logger, authenticator *auth.Authenticator, admissionCtl *admission.Controller, m *metrics.Metrics, meter *metering.Metering, upstream http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handler.Health)
	mux.Handle("GET /metrics", m.Handler())
	mux.Handle("/v1/", authenticator.Middleware(
		m.Middleware(
			middleware.RequestLogger(log)(
				admissionCtl.Middleware(
					meter.Middleware(upstream))))))

	var h http.Handler = mux
	h = middleware.Recover(log)(h)
	return h
}
