package aimd

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func serveMetrics(t *testing.T, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestScrapeParsesPrefixedVLLMMetrics(t *testing.T) {
	srv := serveMetrics(t, strings.Join([]string{
		"# HELP vllm:num_requests_waiting Number of requests waiting in the queue.",
		"# TYPE vllm:num_requests_waiting gauge",
		"vllm:num_requests_waiting 7",
		"# HELP vllm:gpu_cache_usage_perc GPU KV-cache usage.",
		"# TYPE vllm:gpu_cache_usage_perc gauge",
		"vllm:gpu_cache_usage_perc 0.9234",
		"# HELP vllm:request_success_total Successfully finished requests.",
		"# TYPE vllm:request_success_total counter",
		`vllm:request_success_total{finished_reason="stop"} 42`,
	}, "\n"))

	sig, err := NewSignalScraper(srv.URL, time.Second).Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape() error = %v", err)
	}
	if !sig.HasQueueDepth || sig.QueueDepth != 7 {
		t.Errorf("QueueDepth = (%v, %v), want (true, 7)", sig.HasQueueDepth, sig.QueueDepth)
	}
	if !sig.HasKVCache || sig.KVCacheUsage != 0.9234 {
		t.Errorf("KVCacheUsage = (%v, %v), want (true, 0.9234)", sig.HasKVCache, sig.KVCacheUsage)
	}
}

func TestScrapeParsesUnprefixedVLLMMetrics(t *testing.T) {
	srv := serveMetrics(t, strings.Join([]string{
		"# TYPE num_requests_waiting gauge",
		"num_requests_waiting 3",
		"# TYPE kv_cache_usage_ratio gauge",
		"kv_cache_usage_ratio 0.5",
	}, "\n"))

	sig, err := NewSignalScraper(srv.URL, time.Second).Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape() error = %v", err)
	}
	if !sig.HasQueueDepth || sig.QueueDepth != 3 {
		t.Errorf("QueueDepth = (%v, %v), want (true, 3)", sig.HasQueueDepth, sig.QueueDepth)
	}
	if !sig.HasKVCache || sig.KVCacheUsage != 0.5 {
		t.Errorf("KVCacheUsage = (%v, %v), want (true, 0.5)", sig.HasKVCache, sig.KVCacheUsage)
	}
}

func TestScrapeToleratesMissingSignals(t *testing.T) {
	srv := serveMetrics(t, strings.Join([]string{
		"# TYPE vllm:num_requests_running gauge",
		"vllm:num_requests_running 4",
	}, "\n"))

	sig, err := NewSignalScraper(srv.URL, time.Second).Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape() error = %v", err)
	}
	if sig.HasQueueDepth || sig.HasKVCache {
		t.Errorf("missing signals reported as present: %+v", sig)
	}
}

func TestScrapeHTTPFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	if _, err := NewSignalScraper(srv.URL, time.Second).Scrape(context.Background()); err == nil {
		t.Fatal("Scrape() = nil error, want failure")
	}
}
