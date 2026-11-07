package bench

import (
        "context"
        "encoding/json"
        "fmt"
        "io"
        "net/http"
        "net/http/httptest"
        "strings"
        "testing"
        "time"
)

// streamHandler routes on the API key: streaming keys get an SSE completion
// (optionally slowed between tokens), the limited key gets a 429 with
// Retry-After, the plain key gets a single-shot JSON body.
func streamHandler(slow bool) http.HandlerFunc {
        return func(w http.ResponseWriter, r *http.Request) {
                key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
                switch key {
                case "stream-key":
                        w.Header().Set("Content-Type", "text/event-stream")
                        w.WriteHeader(http.StatusOK)
                        flusher := w.(http.Flusher)
                        // deterministic first-token latency: sub-millisecond responses measure as 0 on coarse clocks
                        time.Sleep(50 * time.Millisecond)
                        fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"alpha\"}}]}\n\n")
                        flusher.Flush()
                        if slow {
                                time.Sleep(200 * time.Millisecond)
                        }
                        fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"beta\"}}]}\n\n")
                        flusher.Flush()
                        if slow {
                                time.Sleep(200 * time.Millisecond)
                        }
                        fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{}}],\"usage\":{\"completion_tokens\":2}}\n\n")
                        fmt.Fprint(w, "data: [DONE]\n\n")
                        flusher.Flush()
                case "limited-key":
                        w.Header().Set("Retry-After", "7")
                        w.WriteHeader(http.StatusTooManyRequests)
                        fmt.Fprint(w, "{\"error\":{\"message\":\"concurrency limit reached\"}}")
                case "plain-key":
                        w.Header().Set("Content-Type", "application/json")
                        w.WriteHeader(http.StatusOK)
                        fmt.Fprint(w, "{\"choices\":[{\"message\":{\"content\":\"ok\"}}],\"usage\":{\"completion_tokens\":2}}")
                default:
                        w.WriteHeader(http.StatusUnauthorized)
                }
        }
}

func TestRunnerMeasuresStreamsRejectionsAndPlainResponses(t *testing.T) {
        srv := httptest.NewServer(streamHandler(false))
        defer srv.Close()

        scenario := Scenario{
                Name:                  "selftest",
                Model:                 "test-model",
                DurationSeconds:       1,
                CooldownSeconds:       2,
                RequestTimeoutSeconds: 5,
                Tenants: []TenantConfig{
                        {Name: "steady", APIKey: "stream-key", QPS: 20, MaxInFlight: 4, Stream: true, MaxTokens: 4, PromptTokens: 4},
                        {Name: "throttled", APIKey: "limited-key", QPS: 20, MaxInFlight: 4, Stream: true, MaxTokens: 4, PromptTokens: 4},
                        {Name: "oneshot", APIKey: "plain-key", QPS: 20, MaxInFlight: 4, Stream: false, MaxTokens: 4, PromptTokens: 4},
                },
        }

        report, err := NewRunner(srv.URL).Run(context.Background(), scenario)
        if err != nil {
                t.Fatalf("run: %v", err)
        }

        byName := make(map[string]TenantStats)
        for _, st := range report.Tenants {
                byName[st.Name] = st
        }

        steady := byName["steady"]
        if steady.Successes < 1 {
                t.Fatalf("expected at least one streaming success, got %+v", steady)
        }
        if steady.TTFTP50Ms <= 0 {
                t.Fatalf("expected a positive TTFT, got %v", steady.TTFTP50Ms)
        }
        if steady.CompletionTokens < int64(2*steady.Successes) {
                t.Fatalf("expected >= 2 tokens per success, got %d tokens for %d successes",
                        steady.CompletionTokens, steady.Successes)
        }

        throttled := byName["throttled"]
        if throttled.RateLimited < 1 {
                t.Fatalf("expected rate limiting, got %+v", throttled)
        }
        if throttled.Successes != 0 || throttled.RateLimited != throttled.Requests {
                t.Fatalf("every throttled request should be a 429, got %+v", throttled)
        }
        if throttled.MaxRetryAfterSeconds != 7 {
                t.Fatalf("Retry-After not captured: %v", throttled.MaxRetryAfterSeconds)
        }

        oneshot := byName["oneshot"]
        if oneshot.Successes < 1 {
                t.Fatalf("expected at least one non-stream success, got %+v", oneshot)
        }
        if oneshot.CompletionTokens < int64(2*oneshot.Successes) {
                t.Fatalf("expected >= 2 tokens per success, got %+v", oneshot)
        }
        if oneshot.TTFTP50Ms != 0 {
                t.Fatalf("non-stream requests have no TTFT, got %v", oneshot.TTFTP50Ms)
        }
}

func TestRunnerDisconnectStormAbortsAfterFirstToken(t *testing.T) {
        srv := httptest.NewServer(streamHandler(true))
        defer srv.Close()

        scenario := Scenario{
                Name:                  "disconnect-storm",
                Model:                 "test-model",
                DurationSeconds:       1,
                CooldownSeconds:       3,
                RequestTimeoutSeconds: 5,
                Tenants: []TenantConfig{{
                        Name: "abandoner", APIKey: "stream-key", QPS: 10, MaxInFlight: 4,
                        Stream: true, MaxTokens: 4, PromptTokens: 4,
                        DisconnectProbability: 1.0,
                }},
        }

        report, err := NewRunner(srv.URL).Run(context.Background(), scenario)
        if err != nil {
                t.Fatalf("run: %v", err)
        }
        if len(report.Tenants) != 1 {
                t.Fatalf("expected one tenant, got %d", len(report.Tenants))
        }
        st := report.Tenants[0]
        if st.ClientAborted < 1 {
                t.Fatalf("expected client aborts, got %+v", st)
        }
        if st.Successes != 0 {
                t.Fatalf("streams should be abandoned before completion, got %+v", st)
        }
        if st.TTFTP50Ms <= 0 {
                t.Fatalf("aborts must still record time to first token, got %+v", st)
        }
        if st.CompletionTokens < int64(st.ClientAborted) {
                t.Fatalf("each abort saw at least one token, got %+v", st)
        }
}

func TestOpenLoopCountsHarnessDrops(t *testing.T) {
        // A slow upstream with a one-slot harness ceiling: the generator must
        // keep its schedule and count the sends it could not make instead of
        // silently slowing down (closed loop).
        srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                time.Sleep(300 * time.Millisecond)
                w.Header().Set("Content-Type", "application/json")
                w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}],"usage":{"completion_tokens":2}}`))
        }))
        defer srv.Close()

        scenario := Scenario{
                Name:                  "open-loop",
                Model:                 "test-model",
                DurationSeconds:       1,
                CooldownSeconds:       2,
                RequestTimeoutSeconds: 5,
                Tenants: []TenantConfig{{
                        Name: "capped", APIKey: "plain-key", QPS: 50, MaxInFlight: 1,
                        Stream: false, MaxTokens: 4, PromptTokens: 4,
                }},
        }

        report, err := NewRunner(srv.URL).Run(context.Background(), scenario)
        if err != nil {
                t.Fatalf("run: %v", err)
        }
        st := report.Tenants[0]

        if st.HarnessDropped < 5 {
                t.Fatalf("expected the harness to drop most sends (1 slot, ~50 offered), got %+v", st)
        }
        if st.Offered < st.Requests+st.HarnessDropped {
                t.Fatalf("offered (%d) < recorded (%d) + dropped (%d): accounting broken", st.Offered, st.Requests, st.HarnessDropped)
        }
        // Everything actually sent through the single slot succeeded.
        if st.Successes != st.Requests {
                t.Fatalf("successes (%d) should equal recorded requests (%d)", st.Successes, st.Requests)
        }
}

func TestRunnerScalesOfferedLoad(t *testing.T) {
        srv := httptest.NewServer(streamHandler(false))
        defer srv.Close()

        scenario := Scenario{
                Name:                  "scaled",
                Model:                 "test-model",
                DurationSeconds:       1,
                CooldownSeconds:       2,
                RequestTimeoutSeconds: 5,
                Tenants: []TenantConfig{{
                        Name: "steady", APIKey: "plain-key", QPS: 10, MaxInFlight: 64,
                        Stream: false, MaxTokens: 4, PromptTokens: 4,
                }},
        }

        base := NewRunner(srv.URL)
        baseReport, err := base.Run(context.Background(), scenario)
        if err != nil {
                t.Fatalf("base run: %v", err)
        }

        scaled := NewRunner(srv.URL)
        scaled.QPSScale = 2
        scaledReport, err := scaled.Run(context.Background(), scenario)
        if err != nil {
                t.Fatalf("scaled run: %v", err)
        }

        baseOffered := baseReport.Tenants[0].Offered
        scaledOffered := scaledReport.Tenants[0].Offered
        if scaledOffered < baseOffered*3/2 {
                t.Fatalf("scaling did not raise offered load: %d vs %d", scaledOffered, baseOffered)
        }
}

func TestRunnerDiscoversModelFromGateway(t *testing.T) {
        var gotModel string
        srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                if r.URL.Path == "/v1/models" {
                        w.Header().Set("Content-Type", "application/json")
                        w.Write([]byte(`{"data":[{"id":"Qwen/Qwen2.5-0.5B-Instruct"},{"id":"other"}]}`))
                        return
                }
                var payload struct {
                        Model string `json:"model"`
                }
                body, _ := io.ReadAll(r.Body)
                if json.Unmarshal(body, &payload) == nil {
                        gotModel = payload.Model
                }
                w.Header().Set("Content-Type", "application/json")
                w.Write([]byte(`{"usage":{"completion_tokens":1}}`))
        }))
        defer srv.Close()

        scenario := Scenario{
                Name:            "discovery",
                DurationSeconds: 1,
                CooldownSeconds: 1,
                Tenants: []TenantConfig{{
                        Name: "oneshot", APIKey: "plain-key", QPS: 5, MaxInFlight: 4,
                        Stream: false, MaxTokens: 4, PromptTokens: 4,
                }},
        }

        report, err := NewRunner(srv.URL).Run(context.Background(), scenario)
        if err != nil {
                t.Fatalf("run: %v", err)
        }
        if report.Model != "Qwen/Qwen2.5-0.5B-Instruct" {
                t.Fatalf("report model = %q, want the discovered id", report.Model)
        }
        if gotModel != "Qwen/Qwen2.5-0.5B-Instruct" {
                t.Fatalf("requests sent model %q, want the discovered id", gotModel)
        }
}
