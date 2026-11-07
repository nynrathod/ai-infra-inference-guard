package metering

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nynrathod/inference-guard/internal/auth"
)

// capturingSink collects emitted events for assertions.
type capturingSink struct {
	mu     sync.Mutex
	events []Usage
}

func (s *capturingSink) Emit(u Usage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, u)
}

func (s *capturingSink) Close() error { return nil }

func (s *capturingSink) snapshot() []Usage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Usage(nil), s.events...)
}

// brokenStore simulates an unavailable budget store (fail-open path).
type brokenStore struct{}

func (brokenStore) Reserve(context.Context, string, int64, int64) error {
	return errors.New("connection refused")
}

func (brokenStore) Commit(context.Context, string, int64, int64) error {
	return errors.New("connection refused")
}

func (brokenStore) Close() error { return nil }

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// chain builds auth -> metering -> next, mirroring the router wiring.
func chain(meter *Metering, next http.Handler) http.Handler {
	return auth.New(map[string]string{"sk-test": "tenant-a"}, testLogger(), nil).Middleware(
		meter.Middleware(next))
}

func TestMiddlewareMetersStreamedUsage(t *testing.T) {
	store := NewMemoryBudgetStore(time.Hour)
	sink := &capturingSink{}
	meter := New(Options{Window: time.Hour, ReserveTokens: 64}, store, sink, testLogger())

	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"he\"}}]}\n\n"))
		w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"llo\"}}]}\n\n"))
		w.Write([]byte(`data: {"id":"1","model":"m","usage":{"prompt_tokens":10,"completion_tokens":5,`))
		w.Write([]byte(`"total_tokens":15}}` + "\n\n"))
		w.Write([]byte("data: [DONE]\n\n"))
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true}`))
	req.Header.Set("Authorization", "Bearer sk-test")
	req.Header.Set("Content-Type", "application/json")
	chain(meter, upstream).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "total_tokens\":15") {
		t.Errorf("body = %q, want pass-through of the usage chunk", rec.Body.String())
	}

	events := sink.snapshot()
	if len(events) != 1 {
		t.Fatalf("emitted %d events, want 1", len(events))
	}
	e := events[0]
	if e.TenantID != "tenant-a" || e.PromptTokens != 10 || e.CompletionTokens != 5 || e.TotalTokens != 15 {
		t.Fatalf("event = %+v, want tenant-a 10/5/15", e)
	}
	if !e.Streamed {
		t.Error("event.Streamed = false, want true")
	}
	if e.TTFTMS < 0 {
		t.Errorf("TTFTMS = %d, want >= 0", e.TTFTMS)
	}
	if got := store.used("tenant-a"); got != 15 {
		t.Errorf("store used = %d, want 15 (settled to actual)", got)
	}
}

func TestMiddlewareRejectsOverBudget(t *testing.T) {
	store := NewMemoryBudgetStore(time.Hour)
	sink := &capturingSink{}
	meter := New(Options{
		Window:        time.Hour,
		ReserveTokens: 64,
		Budgets:       map[string]int64{"tenant-a": 32},
	}, store, sink, testLogger())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("Authorization", "Bearer sk-test")
	chain(meter, http.NotFoundHandler()).ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("Retry-After header missing")
	}
	if !strings.Contains(rec.Body.String(), "tenant_budget_exhausted") {
		t.Errorf("body = %q, want tenant_budget_exhausted envelope", rec.Body.String())
	}
	events := sink.snapshot()
	if len(events) != 1 || events[0].Reason != "budget_exhausted" {
		t.Fatalf("events = %+v, want one budget_exhausted event", events)
	}
}

func TestMiddlewareFailsOpenWhenStoreDown(t *testing.T) {
	sink := &capturingSink{}
	meter := New(Options{Window: time.Hour, ReserveTokens: 64}, brokenStore{}, sink, testLogger())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer sk-test")
	chain(meter, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (fail-open)", rec.Code)
	}
	if len(sink.snapshot()) != 1 {
		t.Error("expected an event despite store failure")
	}
}

func TestMiddlewareInjectsIncludeUsage(t *testing.T) {
	meter := New(Options{Window: time.Hour, ReserveTokens: 64},
		NewMemoryBudgetStore(time.Hour), NoopSink{}, testLogger())

	var gotBody string
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`))
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true,"max_tokens":8}`))
	req.Header.Set("Authorization", "Bearer sk-test")
	req.Header.Set("Content-Type", "application/json")
	chain(meter, upstream).ServeHTTP(httptest.NewRecorder(), req)

	if !strings.Contains(gotBody, `"include_usage":true`) {
		t.Fatalf("upstream body = %q, want stream_options.include_usage=true", gotBody)
	}
	if !strings.Contains(gotBody, `"stream":true`) || !strings.Contains(gotBody, `"max_tokens":8`) {
		t.Errorf("rewritten body lost fields: %q", gotBody)
	}
}

func TestMiddlewareLeavesNonStreamingBodiesAlone(t *testing.T) {
	meter := New(Options{}, NewMemoryBudgetStore(time.Hour), NoopSink{}, testLogger())

	var gotBody string
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":false}`))
	req.Header.Set("Authorization", "Bearer sk-test")
	req.Header.Set("Content-Type", "application/json")
	chain(meter, upstream).ServeHTTP(httptest.NewRecorder(), req)

	if gotBody != `{"model":"m","stream":false}` {
		t.Fatalf("body = %q, want unchanged", gotBody)
	}
}

func TestChars4EstimatorReservesPromptPlusMaxTokens(t *testing.T) {
	store := NewMemoryBudgetStore(time.Hour)
	sink := &capturingSink{}
	meter := New(Options{
		Window:         time.Hour,
		ReserveTokens:  64,
		TokenEstimator: TokenEstimatorChars4,
		Budgets:        map[string]int64{"tenant-a": 500},
	}, store, sink, testLogger())

	// 400 chars of prompt -> ~100 estimated prompt tokens, plus max_tokens 100
	// -> reservation 200, well inside the 500 budget.
	content := strings.Repeat("abcd", 100)
	body := `{"model":"m","messages":[{"role":"user","content":"` + content + `"}],"max_tokens":100}`
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ := io.ReadAll(r.Body)
		if string(got) != body {
			t.Errorf("upstream body changed: %d bytes, want the original %d", len(got), len(body))
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"usage":{"prompt_tokens":400,"completion_tokens":100,"total_tokens":500}}`))
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer sk-test")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	chain(meter, upstream).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := store.used("tenant-a"); got != 500 {
		t.Errorf("store used = %d, want 500 (settled to actual usage)", got)
	}
}

func TestChars4EstimatorRejectsLongPromptsEarly(t *testing.T) {
	store := NewMemoryBudgetStore(time.Hour)
	meter := New(Options{
		Window:         time.Hour,
		ReserveTokens:  64,
		TokenEstimator: TokenEstimatorChars4,
		Budgets:        map[string]int64{"tenant-a": 150},
	}, store, NoopSink{}, testLogger())

	// 400 chars -> ~100 prompt tokens + max_tokens 100 = 200 > 150 budget:
	// the request must be rejected up front instead of under-reserving.
	content := strings.Repeat("abcd", 100)
	body := `{"model":"m","messages":[{"role":"user","content":"` + content + `"}],"max_tokens":100}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer sk-test")
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	chain(meter, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("upstream reached despite estimated over-budget request")
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
}

func TestFixedEstimatorKeepsFlatReservation(t *testing.T) {
	store := NewMemoryBudgetStore(time.Hour)
	meter := New(Options{Window: time.Hour, ReserveTokens: 64}, store, NoopSink{}, testLogger())

	// A long prompt with the default (fixed) estimator still reserves the
	// flat amount: direct construction keeps the historic behavior.
	content := strings.Repeat("abcd", 100)
	body := `{"model":"m","messages":[{"role":"user","content":"` + content + `"}],"max_tokens":100}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer sk-test")
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	chain(meter, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (flat reservation well inside budget)", rec.Code)
	}
}
