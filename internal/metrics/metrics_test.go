package metrics

import (
        "io"
        "log/slog"
        "net/http"
        "net/http/httptest"
        "strings"
        "testing"
        "time"

        "github.com/nynrathod/inference-guard/internal/auth"
        "github.com/prometheus/client_golang/prometheus/testutil"
)

func testLogger() *slog.Logger {
        return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// chain builds auth over metrics over next, mirroring the ordering the
// router wires in production.
func chain(m *Metrics, next http.Handler) http.Handler {
        authn := auth.New(map[string]string{"sk-test": "tenant-a"}, testLogger(), m)
        return authn.Middleware(m.Middleware(next))
}

func serve(h http.Handler) *httptest.ResponseRecorder {
        rec := httptest.NewRecorder()
        req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
        req.Header.Set("Authorization", "Bearer sk-test")
        h.ServeHTTP(rec, req)
        return rec
}

func TestMiddlewareRecordsCompletedRequest(t *testing.T) {
        m := New()
        h := chain(m, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                w.WriteHeader(http.StatusOK)
                w.Write([]byte("chunk"))
        }))

        if rec := serve(h); rec.Code != http.StatusOK {
                t.Fatalf("status = %d, want 200", rec.Code)
        }

        if got := testutil.ToFloat64(m.requestsTotal.WithLabelValues("tenant-a", "200")); got != 1 {
                t.Errorf("requests_total{tenant-a,200} = %v, want 1", got)
        }
        if got := testutil.ToFloat64(m.requestsInFlight.WithLabelValues("tenant-a")); got != 0 {
                t.Errorf("requests_in_flight{tenant-a} = %v, want 0 after completion", got)
        }
        if got := testutil.CollectAndCount(m.requestDuration); got != 1 {
                t.Errorf("request_duration series = %d, want 1", got)
        }
        if got := testutil.CollectAndCount(m.timeToFirstByte); got != 1 {
                t.Errorf("time_to_first_byte series = %d, want 1", got)
        }
}

func TestMiddlewareRecordsRejectionStatus(t *testing.T) {
        m := New()
        h := chain(m, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                w.WriteHeader(http.StatusTooManyRequests)
        }))

        serve(h)

        if got := testutil.ToFloat64(m.requestsTotal.WithLabelValues("tenant-a", "429")); got != 1 {
                t.Errorf("requests_total{tenant-a,429} = %v, want 1", got)
        }
        if got := testutil.CollectAndCount(m.timeToFirstByte); got != 0 {
                t.Errorf("time_to_first_byte series = %d, want 0 when nothing was written", got)
        }
}

func TestMiddlewareTracksInFlight(t *testing.T) {
        m := New()
        entered := make(chan struct{})
        release := make(chan struct{})
        h := chain(m, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                close(entered)
                <-release
                w.Write([]byte("done"))
        }))

        done := make(chan struct{})
        go func() {
                serve(h)
                close(done)
        }()

        <-entered
        if got := testutil.ToFloat64(m.requestsInFlight.WithLabelValues("tenant-a")); got != 1 {
                t.Errorf("requests_in_flight during request = %v, want 1", got)
        }

        close(release)
        <-done
        if got := testutil.ToFloat64(m.requestsInFlight.WithLabelValues("tenant-a")); got != 0 {
                t.Errorf("requests_in_flight after completion = %v, want 0", got)
        }
}

func TestMiddlewareCountsUnwrittenResponseAsOK(t *testing.T) {
        m := New()
        h := chain(m, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

        serve(h)

        if got := testutil.ToFloat64(m.requestsTotal.WithLabelValues("tenant-a", "200")); got != 1 {
                t.Errorf("requests_total{tenant-a,200} = %v, want 1", got)
        }
}

func TestMiddlewareMeasuresFirstTokenNotFirstByte(t *testing.T) {
        m := New()
        notified := make(chan time.Duration, 8)
        m.SetTTFTObserver(func(tenant string, d time.Duration) {
                if tenant == "tenant-a" {
                        notified <- d
                }
        })

        h := chain(m, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                w.Header().Set("Content-Type", "text/event-stream")
                w.WriteHeader(http.StatusOK)
                // Role-only chunk: first byte arrives here, but no content token yet.
                w.Write([]byte("data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n"))
                if f, ok := w.(http.Flusher); ok {
                        f.Flush()
                }
                time.Sleep(40 * time.Millisecond)
                // First content token.
                w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"he\"}}]}\n\n"))
                w.Write([]byte("data: [DONE]\n\n"))
        }))

        serve(h)

        if got := testutil.CollectAndCount(m.timeToFirstByte); got != 1 {
                t.Errorf("time_to_first_byte series = %d, want 1", got)
        }
        if got := testutil.CollectAndCount(m.timeToFirstToken); got != 1 {
                t.Fatalf("time_to_first_token series = %d, want 1", got)
        }

        // The observer must carry the token time, which is strictly later than
        // the first byte because of the sleep between the two frames.
        select {
        case d := <-notified:
                if d < 40*time.Millisecond {
                        t.Errorf("observer duration = %v, want >= 40ms (token, not first byte)", d)
                }
        case <-time.After(2 * time.Second):
                t.Fatal("TTFT observer was never notified")
        }
}

func TestMiddlewareExcludesRejectionsFromTTFT(t *testing.T) {
        m := New()
        notified := make(chan time.Duration, 8)
        m.SetTTFTObserver(func(tenant string, d time.Duration) {
                if tenant == "tenant-a" {
                        notified <- d
                }
        })

        h := chain(m, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                w.Header().Set("Content-Type", "application/json")
                w.WriteHeader(http.StatusTooManyRequests)
                w.Write([]byte("{\"error\":{\"message\":\"limit reached\"}}"))
        }))

        serve(h)

        // A 429 writes a body, so first byte is recorded, but it is not a
        // content token: the controller's latency window stays clean.
        if got := testutil.CollectAndCount(m.timeToFirstByte); got != 1 {
                t.Errorf("time_to_first_byte series = %d, want 1", got)
        }
        if got := testutil.CollectAndCount(m.timeToFirstToken); got != 0 {
                t.Errorf("time_to_first_token series = %d, want 0 for a rejection", got)
        }
        select {
        case d := <-notified:
                t.Fatalf("observer notified for a rejected request: %v", d)
        case <-time.After(100 * time.Millisecond):
        }
}

func TestRecordRejection(t *testing.T) {
        m := New()

        m.RecordRejection("missing API key")

        if got := testutil.ToFloat64(m.authRejections.WithLabelValues("missing API key")); got != 1 {
                t.Errorf("auth_rejections_total{missing API key} = %v, want 1", got)
        }
}

func TestHandlerExposesMetrics(t *testing.T) {
        m := New()
        h := chain(m, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                w.Header().Set("Content-Type", "application/json")
                w.Write([]byte("chunk"))
        }))
        serve(h)
        m.RecordRejection("missing API key")

        rec := httptest.NewRecorder()
        m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

        if rec.Code != http.StatusOK {
                t.Fatalf("status = %d, want 200", rec.Code)
        }
        for _, want := range []string{
                "gateway_requests_total",
                "gateway_requests_in_flight",
                "gateway_request_duration_seconds",
                "gateway_time_to_first_byte_seconds",
                "gateway_time_to_first_token_seconds",
                "gateway_auth_rejections_total",
        } {
                if !strings.Contains(rec.Body.String(), want) {
                        t.Errorf("metrics output missing %s", want)
                }
        }
}
