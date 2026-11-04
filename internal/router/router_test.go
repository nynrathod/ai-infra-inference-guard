package router_test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nynrathod/inference-guard/internal/admission"
	"github.com/nynrathod/inference-guard/internal/auth"
	"github.com/nynrathod/inference-guard/internal/metering"
	"github.com/nynrathod/inference-guard/internal/metrics"
	"github.com/nynrathod/inference-guard/internal/router"
)

func newTestHandler(upstream http.Handler, tenantLimit int, budgets map[string]int64) http.Handler {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := metrics.New()
	meter := metering.New(
		metering.Options{
			Window:        time.Hour,
			ReserveTokens: 8,
			Budgets:       budgets,
		},
		metering.NewMemoryBudgetStore(time.Hour),
		metering.NoopSink{},
		log,
	)
	return router.New(
		log,
		auth.New(map[string]string{"sk-test": "tenant-a"}, log, m),
		admission.New(tenantLimit),
		m,
		meter,
		upstream,
	)
}

func TestRouterHealthWithoutAuth(t *testing.T) {
	h := newTestHandler(http.NotFoundHandler(), 8, nil)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestRouterExposesMetrics(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := newTestHandler(upstream, 8, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer sk-test")
	h.ServeHTTP(httptest.NewRecorder(), req)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "gateway_requests_total") {
		t.Error("metrics output missing gateway_requests_total")
	}
}

func TestRouterRejectsMissingKey(t *testing.T) {
	h := newTestHandler(http.NotFoundHandler(), 8, nil)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/models", nil))

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestRouterProxiesAuthenticated(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"proxied":true}`))
	})
	h := newTestHandler(upstream, 8, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer sk-test")

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "proxied") {
		t.Errorf("body = %q, want proxied payload", rec.Body.String())
	}
}

func TestRouterRejectsOverLimit(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	})
	h := newTestHandler(upstream, 1, nil)

	firstDone := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		req.Header.Set("Authorization", "Bearer sk-test")
		h.ServeHTTP(rec, req)
		firstDone <- rec.Code
	}()

	<-started

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer sk-test")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("Retry-After header missing")
	}
	if !strings.Contains(rec.Body.String(), "tenant_concurrency_limit") {
		t.Errorf("body = %q, want tenant_concurrency_limit envelope", rec.Body.String())
	}

	close(release)
	if code := <-firstDone; code != http.StatusOK {
		t.Fatalf("first request status = %d, want 200", code)
	}
}

func TestRouterRejectsOverBudget(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// ReserveTokens is 8, so a 4-token budget rejects any request.
	h := newTestHandler(upstream, 8, map[string]int64{"tenant-a": 4})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("Authorization", "Bearer sk-test")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("Retry-After header missing")
	}
	if !strings.Contains(rec.Body.String(), "tenant_budget_exhausted") {
		t.Errorf("body = %q, want tenant_budget_exhausted envelope", rec.Body.String())
	}
}
