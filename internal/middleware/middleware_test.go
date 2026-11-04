package middleware

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRecoverPanickingHandler(t *testing.T) {
	rec := httptest.NewRecorder()
	handler := Recover(testLogger())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))

	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/models", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "internal_error") {
		t.Errorf("body = %q, want internal_error envelope", rec.Body.String())
	}
}

func TestRecoverPassesThrough(t *testing.T) {
	rec := httptest.NewRecorder()
	handler := Recover(testLogger())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))

	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/models", nil))

	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want 418", rec.Code)
	}
}

func TestRequestLoggerEchoesRequestID(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("X-Request-ID", "fixed-id")
	handler := RequestLogger(testLogger())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("X-Request-ID"); got != "fixed-id" {
		t.Fatalf("X-Request-ID = %q, want fixed-id", got)
	}
}

func TestRequestLoggerGeneratesRequestID(t *testing.T) {
	rec := httptest.NewRecorder()
	handler := RequestLogger(testLogger())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/models", nil))

	if got := rec.Header().Get("X-Request-ID"); len(got) != 16 {
		t.Fatalf("X-Request-ID = %q, want 16 hex characters", got)
	}
}

func TestStatusRecorder(t *testing.T) {
	inner := httptest.NewRecorder()
	rec := &statusRecorder{ResponseWriter: inner, status: http.StatusOK}

	rec.WriteHeader(http.StatusNotFound)
	rec.Write([]byte("abc"))
	rec.Write([]byte("de"))
	rec.Flush()

	if rec.status != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.status)
	}
	if rec.bytes != 5 {
		t.Errorf("bytes = %d, want 5", rec.bytes)
	}
}
