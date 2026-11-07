package bench

import (
        "io"
        "net/http"
        "net/http/httptest"
        "strings"
        "testing"
)

// serveMetricsHTTP exposes a Prometheus text payload for scraper tests.
func serveMetricsHTTP(t *testing.T, body string) *httptest.Server {
        t.Helper()
        srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                w.Header().Set("Content-Type", "text/plain; version=0.0.4")
                io.WriteString(w, body)
        }))
        t.Cleanup(srv.Close)
        return srv
}

func TestModeSettingsFor(t *testing.T) {
        cases := []struct {
                mode       ControlMode
                rateRPS    float64
                want       ModeSettings
                wantErr    bool
        }{
                {ModeNone, 5, ModeSettings{Concurrency: 1000, AIMD: false, RateLimitRPS: 0}, false},
                {ModeStatic, 5, ModeSettings{Concurrency: 8, AIMD: false, RateLimitRPS: 0}, false},
                {ModeRateLimit, 5, ModeSettings{Concurrency: 1000, AIMD: false, RateLimitRPS: 5}, false},
                {ModeAdaptive, 5, ModeSettings{Concurrency: 8, AIMD: true, RateLimitRPS: 0}, false},
                {ModeRateLimit, 0, ModeSettings{}, true}, // needs a rate
                {ControlMode("bogus"), 5, ModeSettings{}, true},
        }
        for _, tc := range cases {
                got, err := ModeSettingsFor(tc.mode, 8, 1000, tc.rateRPS)
                if tc.wantErr {
                        if err == nil {
                                t.Errorf("%s: expected an error, got %+v", tc.mode, got)
                        }
                        continue
                }
                if err != nil {
                        t.Errorf("%s: unexpected error: %v", tc.mode, err)
                        continue
                }
                if got != tc.want {
                        t.Errorf("%s = %+v, want %+v", tc.mode, got, tc.want)
                }
        }
}

func TestModeSettingsEnvRendersAssignments(t *testing.T) {
        env := ModeSettings{Concurrency: 8, AIMD: true, RateLimitRPS: 0}.Env()
        if len(env) != 3 {
                t.Fatalf("expected 3 assignments, got %v", env)
        }
        for _, want := range []string{
                "GATEWAY_TENANT_CONCURRENCY=8",
                "GATEWAY_AIMD_ENABLED=true",
                "GATEWAY_RATE_LIMIT_RPS=0",
        } {
                found := false
                for _, e := range env {
                        if e == want {
                                found = true
                        }
                }
                if !found {
                        t.Errorf("env %v missing %q", env, want)
                }
        }
}

func TestRestoreEnvRemovesUnsetVariables(t *testing.T) {
        got := restoreEnv(map[string]string{
                "GATEWAY_TENANT_CONCURRENCY": "8",
                "GATEWAY_AIMD_ENABLED":       "true",
                // no GATEWAY_RATE_LIMIT_RPS before the matrix ran
        })
        want := []string{
                "GATEWAY_TENANT_CONCURRENCY=8",
                "GATEWAY_AIMD_ENABLED=true",
                "GATEWAY_RATE_LIMIT_RPS-",
        }
        for i := range want {
                if got[i] != want[i] {
                        t.Fatalf("restore env = %v, want %v", got, want)
                }
        }
}

func TestVLLMDeltaCountsAndHandlesReset(t *testing.T) {
        before := vllmSnapshot{ttftCount: 100, ttftSum: 50, kvCache: 0.8, queue: 3}
        after := vllmSnapshot{ttftCount: 160, ttftSum: 92, kvCache: 0.9, queue: 7}

        d := after.delta(before)
        if d.TTFTCount != 60 || d.TTFTSumSeconds != 42 {
                t.Fatalf("delta = %+v, want 60 requests / 42s", d)
        }
        if d.MeanTTFTSeconds != 0.7 {
                t.Fatalf("mean = %v, want 0.7", d.MeanTTFTSeconds)
        }
        if d.KVCacheUsage != 0.9 || d.QueueDepth != 7 {
                t.Fatalf("last-observed values not carried: %+v", d)
        }

        // Server restart mid-run: counters reset, delta falls back to absolute.
        reset := vllmSnapshot{ttftCount: 10, ttftSum: 4}.delta(before)
        if reset.TTFTCount != 10 || reset.TTFTSumSeconds != 4 {
                t.Fatalf("reset handling = %+v", reset)
        }
}

func TestScrapeVLLMParsesHistogramsAndGauges(t *testing.T) {
        srv := serveMetricsHTTP(t, strings.Join([]string{
                "# TYPE vllm:time_to_first_token_seconds histogram",
                `vllm:time_to_first_token_seconds_bucket{le="0.01"} 4`,
                `vllm:time_to_first_token_seconds_bucket{le="+Inf"} 10`,
                `vllm:time_to_first_token_seconds_sum 1.25`,
                `vllm:time_to_first_token_seconds_count 10`,
                "# TYPE vllm:kv_cache_usage_perc gauge",
                "vllm:kv_cache_usage_perc 0.42",
                "# TYPE vllm:num_requests_waiting gauge",
                "vllm:num_requests_waiting 6",
        }, "\n"))
        defer srv.Close()

        snap, err := scrapeVLLM(t.Context(), srv.URL)
        if err != nil {
                t.Fatalf("scrape: %v", err)
        }
        if snap.ttftCount != 10 || snap.ttftSum != 1.25 {
                t.Fatalf("ttft counters = %v/%v, want 10/1.25", snap.ttftCount, snap.ttftSum)
        }
        if snap.kvCache != 0.42 || snap.queue != 6 {
                t.Fatalf("gauges = %v/%v, want 0.42/6", snap.kvCache, snap.queue)
        }
}

func TestMatrixSummaryMarkdownRendersTable(t *testing.T) {
        summary := &MatrixSummary{
                Scenario:        "02-saturation",
                Model:           "test-model",
                LatencyTargetMS: 2500,
                QPSScale:        1.5,
                SustainableQPS:  8,
                SaturationQPS:   16,
                Modes: []ModeResult{
                        {Mode: ModeNone, Report: RunReport{ActiveSeconds: 10, Tenants: []TenantStats{{Requests: 10, Offered: 15, Successes: 4, QualifiedSuccesses: 3, CompletionTokens: 100, TTFTP95Ms: 4000, RateLimited: 6}}}},
                        {Mode: ModeAdaptive, Report: RunReport{ActiveSeconds: 10, Tenants: []TenantStats{{Requests: 10, Offered: 15, Successes: 9, QualifiedSuccesses: 8, CompletionTokens: 200, TTFTP95Ms: 800}}}},
                },
        }
        md := summary.Markdown()
        for _, want := range []string{
                "| mode |",
                "| none |",
                "| adaptive |",
                "measured sustainable capacity: 8.0 req/s",
        } {
                if !strings.Contains(md, want) {
                        t.Errorf("markdown missing %q:\n%s", want, md)
                }
        }
}
