package apierror

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWriteEnvelope(t *testing.T) {
	rec := httptest.NewRecorder()

	Write(rec, http.StatusTooManyRequests, "rate_limit_error", "tenant_concurrency_limit", "slow down")

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}

	var body Body
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}
	if body.Error.Type != "rate_limit_error" {
		t.Errorf("type = %q", body.Error.Type)
	}
	if body.Error.Code != "tenant_concurrency_limit" {
		t.Errorf("code = %q", body.Error.Code)
	}
	if body.Error.Message != "slow down" {
		t.Errorf("message = %q", body.Error.Message)
	}
}
