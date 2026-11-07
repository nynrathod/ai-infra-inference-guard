package ratelimit

import (
        "io"
        "log/slog"
        "net/http"
        "net/http/httptest"
        "testing"
        "time"

        "github.com/nynrathod/inference-guard/internal/auth"
)

func TestBucketAdmitsUpToBurstThenRejects(t *testing.T) {
        l := New(2, 3) // 2 rps, burst 3
        now := time.Now()

        for i := 0; i < 3; i++ {
                if !l.Allow("tenant-a", now) {
                        t.Fatalf("request %d rejected inside burst", i+1)
                }
        }
        if l.Allow("tenant-a", now) {
                t.Fatal("4th request admitted with an empty bucket")
        }
        // Independent tenant: own bucket, starts full.
        if !l.Allow("tenant-b", now) {
                t.Fatal("other tenant affected by tenant-a's bucket")
        }
}

func TestBucketRefillsOverTime(t *testing.T) {
        l := New(10, 1) // 10 rps, burst 1
        now := time.Now()

        if !l.Allow("tenant-a", now) {
                t.Fatal("first request rejected")
        }
        if l.Allow("tenant-a", now) {
                t.Fatal("second request admitted before refill")
        }
        if !l.Allow("tenant-a", now.Add(150*time.Millisecond)) {
                t.Fatal("request rejected after refill window")
        }
}

func TestRetryAfterIsPositiveWhenEmpty(t *testing.T) {
        l := New(2, 1)
        now := time.Now()
        l.Allow("tenant-a", now)

        if got := l.retryAfter("tenant-a"); got <= 0 {
                t.Fatalf("retryAfter = %v, want > 0", got)
        }
}

func TestMiddlewareRejectsWith429AndRetryAfter(t *testing.T) {
        l := New(1, 1)
        logger := slog.New(slog.NewTextHandler(io.Discard, nil))
        authn := auth.New(map[string]string{"sk-test": "tenant-a"}, logger, nil)
        h := authn.Middleware(l.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                w.WriteHeader(http.StatusOK)
        })))

        serve := func() *httptest.ResponseRecorder {
                rec := httptest.NewRecorder()
                req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
                req.Header.Set("Authorization", "Bearer sk-test")
                h.ServeHTTP(rec, req)
                return rec
        }

        if rec := serve(); rec.Code != http.StatusOK {
                t.Fatalf("first request status = %d, want 200", rec.Code)
        }
        rec := serve()
        if rec.Code != http.StatusTooManyRequests {
                t.Fatalf("second request status = %d, want 429", rec.Code)
        }
        if rec.Header().Get("Retry-After") == "" {
                t.Fatal("429 response missing Retry-After")
        }
}
