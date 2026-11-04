package auth

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestMiddleware(t *testing.T) {
	a := New(map[string]string{"sk-valid": "tenant-a"}, testLogger(), nil)

	captured := make(chan string, 1)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant, ok := TenantFromContext(r.Context())
		if !ok {
			t.Error("TenantFromContext = false for admitted request")
		}
		captured <- tenant
		w.WriteHeader(http.StatusOK)
	})

	tests := []struct {
		name       string
		authHeader string
		want       int
	}{
		{"no authorization header", "", http.StatusUnauthorized},
		{"wrong scheme", "Basic sk-valid", http.StatusUnauthorized},
		{"empty token", "Bearer ", http.StatusUnauthorized},
		{"unknown key", "Bearer sk-invalid", http.StatusUnauthorized},
		{"valid key", "Bearer sk-valid", http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}

			a.Middleware(next).ServeHTTP(rec, req)

			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
			if tt.want == http.StatusUnauthorized {
				if rec.Header().Get("WWW-Authenticate") != "Bearer" {
					t.Error("WWW-Authenticate header missing")
				}
				if !strings.Contains(rec.Body.String(), "invalid_api_key") {
					t.Errorf("body = %q, want invalid_api_key envelope", rec.Body.String())
				}
			}
		})
	}

	if tenant := <-captured; tenant != "tenant-a" {
		t.Fatalf("captured tenant = %q, want tenant-a", tenant)
	}
}

func TestTenantFromContextEmpty(t *testing.T) {
	if _, ok := TenantFromContext(context.Background()); ok {
		t.Error("TenantFromContext(empty context) = true")
	}
}

type recordingRejections struct {
	mu     sync.Mutex
	reason []string
}

func (r *recordingRejections) RecordRejection(reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reason = append(r.reason, reason)
}

func (r *recordingRejections) reasons() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.reason...)
}

func TestMiddlewareReportsRejections(t *testing.T) {
	tests := []struct {
		name       string
		authHeader string
		want       string
	}{
		{"missing API key", "", "missing API key"},
		{"invalid API key", "Bearer sk-invalid", "invalid API key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rejections := &recordingRejections{}
			a := New(map[string]string{"sk-valid": "tenant-a"}, testLogger(), rejections)

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}

			a.Middleware(http.NotFoundHandler()).ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", rec.Code)
			}
			got := rejections.reasons()
			if len(got) != 1 || got[0] != tt.want {
				t.Fatalf("rejections = %v, want [%s]", got, tt.want)
			}
		})
	}
}
