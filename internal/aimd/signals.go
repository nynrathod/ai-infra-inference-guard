package aimd

import (
        "bytes"
        "context"
        "fmt"
        "io"
        "net/http"
        "strings"
        "time"

        dto "github.com/prometheus/client_model/go"
        "github.com/prometheus/common/expfmt"
)

// Signals is a point-in-time snapshot of the model server's saturation
// signals. Flag fields distinguish "observed as zero" from "not exposed".
type Signals struct {
        QueueDepth    float64
        KVCacheUsage  float64
        HasQueueDepth bool
        HasKVCache    bool
}

// Candidate metric names across model server versions: vLLM renamed the
// KV-cache gauge twice — gpu_cache_usage_perc (percentage, older),
// kv_cache_usage_ratio (0-1) and finally kv_cache_usage_perc (current
// documented name) — each with and without the "vllm:" prefix. The scraper
// accepts every name so the controller keeps working across upgrades.
var (
        queueDepthNames = []string{
                "vllm:num_requests_waiting",
                "num_requests_waiting",
        }
        kvCacheNames = []string{
                "vllm:kv_cache_usage_perc",
                "vllm:gpu_cache_usage_perc",
                "vllm:kv_cache_usage_ratio",
                "kv_cache_usage_perc",
                "gpu_cache_usage_perc",
                "kv_cache_usage_ratio",
        }
)

// SignalScraper polls the model server's Prometheus endpoint and extracts
// the saturation signals the controller reacts to.
type SignalScraper struct {
        endpoint string
        client   *http.Client
}

// NewSignalScraper returns a scraper for the model server's metrics endpoint.
func NewSignalScraper(endpoint string, timeout time.Duration) *SignalScraper {
        return &SignalScraper{
                endpoint: strings.TrimSuffix(endpoint, "/"),
                client:   &http.Client{Timeout: timeout},
        }
}

// Scrape fetches and parses the model server's metrics once.
func (s *SignalScraper) Scrape(ctx context.Context) (Signals, error) {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.endpoint, nil)
        if err != nil {
                return Signals{}, fmt.Errorf("build scrape request: %w", err)
        }
        resp, err := s.client.Do(req)
        if err != nil {
                return Signals{}, fmt.Errorf("scrape model server metrics: %w", err)
        }
        defer resp.Body.Close()
        if resp.StatusCode != http.StatusOK {
                return Signals{}, fmt.Errorf("scrape model server metrics: status %d", resp.StatusCode)
        }

        body, err := io.ReadAll(resp.Body)
        if err != nil {
                return Signals{}, fmt.Errorf("read model server metrics: %w", err)
        }
        // The exposition format terminates every record with a newline; several
        // exporters omit the final one and the parser then rejects the last
        // record as truncated, so normalize before parsing.
        if len(body) > 0 && body[len(body)-1] != '\n' {
                body = append(body, '\n')
        }

        var parser expfmt.TextParser
        families, err := parser.TextToMetricFamilies(bytes.NewReader(body))
        if err != nil {
                return Signals{}, fmt.Errorf("parse model server metrics: %w", err)
        }

        sig := Signals{}
        if v, ok := gaugeValue(families, queueDepthNames); ok {
                sig.QueueDepth, sig.HasQueueDepth = v, true
        }
        if v, ok := gaugeValue(families, kvCacheNames); ok {
                sig.KVCacheUsage, sig.HasKVCache = v, true
        }
        return sig, nil
}

// gaugeValue returns the first sample of the first candidate metric name
// present in families that is actually a gauge.
func gaugeValue(families map[string]*dto.MetricFamily, names []string) (float64, bool) {
        for _, name := range names {
                family, ok := families[name]
                if !ok || len(family.Metric) == 0 {
                        continue
                }
                if gauge := family.Metric[0].GetGauge(); gauge != nil {
                        return gauge.GetValue(), true
                }
        }
        return 0, false
}
