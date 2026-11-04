package proxy

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestProxyForwardsRequestAndResponse(t *testing.T) {
	var gotMethod, gotPath, gotAuth, gotXFF string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotXFF = r.Header.Get("X-Forwarded-For")
		w.Header().Set("X-Upstream", "yes")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	u, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer sk-test")

	New(u, testLogger()).Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if gotMethod != http.MethodPost || gotPath != "/v1/chat/completions" {
		t.Errorf("upstream saw %s %s", gotMethod, gotPath)
	}
	if gotAuth != "Bearer sk-test" {
		t.Errorf("Authorization forwarded = %q", gotAuth)
	}
	if gotXFF == "" {
		t.Error("X-Forwarded-For not set")
	}
	if rec.Header().Get("X-Upstream") != "yes" {
		t.Error("upstream response header lost")
	}
	if !strings.Contains(rec.Body.String(), `"ok":true`) {
		t.Errorf("body = %q", rec.Body.String())
	}
}

func TestProxyUpstreamUnreachable(t *testing.T) {
	u, err := url.Parse("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	New(u, testLogger()).Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/models", nil))

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "upstream_unavailable") {
		t.Errorf("body = %q, want upstream_unavailable envelope", rec.Body.String())
	}
}
