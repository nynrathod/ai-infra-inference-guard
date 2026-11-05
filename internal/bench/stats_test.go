package bench

import (
	"math"
	"testing"
)

func TestPercentile(t *testing.T) {
	xs := make([]float64, 100)
	for i := range xs {
		xs[i] = float64(i + 1)
	}
	cases := []struct {
		p    float64
		want float64
	}{
		{50, 50}, {95, 95}, {99, 99}, {100, 100}, {1, 1}, {0.5, 1},
	}
	for _, tc := range cases {
		if got := Percentile(xs, tc.p); got != tc.want {
			t.Errorf("Percentile(p=%v) = %v, want %v", tc.p, got, tc.want)
		}
	}
	if got := Percentile([]float64{3, 1, 2}, 50); got != 2 {
		t.Errorf("p50 of [1 2 3] = %v, want 2", got)
	}
	if got := Percentile(nil, 95); got != 0 {
		t.Errorf("p95 of empty = %v, want 0", got)
	}
	in := []float64{2, 1}
	Percentile(in, 50)
	if in[0] != 2 || in[1] != 1 {
		t.Errorf("input was mutated: %v", in)
	}
}

func TestSummarizeCountsAndGoodput(t *testing.T) {
	results := []RequestResult{
		{Tenant: "a", Outcome: OutcomeSuccess, TTFTMs: 100, TotalMs: 900, Tokens: 10},
		{Tenant: "a", Outcome: OutcomeSuccess, TTFTMs: 300, TotalMs: 1100, Tokens: 10},
		{Tenant: "a", Outcome: OutcomeRateLimited, TTFTMs: -1, TotalMs: 5, RetryAfterSeconds: 2},
		{Tenant: "b", Outcome: OutcomeClientAborted, TTFTMs: 120, TotalMs: 200, Tokens: 3},
	}
	stats := Summarize(results, map[string]int{"a": 4, "b": 1}, 10)
	if len(stats) != 2 {
		t.Fatalf("expected 2 tenants, got %d", len(stats))
	}
	a, b := stats[0], stats[1]
	if a.Name != "a" || b.Name != "b" {
		t.Fatalf("expected first-seen order, got %q then %q", a.Name, b.Name)
	}
	if a.Requests != 3 || a.Successes != 2 || a.RateLimited != 1 {
		t.Fatalf("bad counts for a: %+v", a)
	}
	if a.CompletionTokens != 20 {
		t.Fatalf("completion tokens = %d, want 20", a.CompletionTokens)
	}
	if a.TTFTP50Ms != 100 || a.TTFTP95Ms != 300 {
		t.Fatalf("bad TTFT percentiles: %+v", a)
	}
	if a.E2EP95Ms != 1100 {
		t.Fatalf("e2e p95 = %v, want 1100", a.E2EP95Ms)
	}
	if a.GoodputTokensPerSec != 2 {
		t.Fatalf("goodput = %v, want 2", a.GoodputTokensPerSec)
	}
	if a.MaxInFlightObserved != 4 || a.MaxRetryAfterSeconds != 2 {
		t.Fatalf("peak/retry-after not carried: %+v", a)
	}
	if b.ClientAborted != 1 || b.Successes != 0 {
		t.Fatalf("bad counts for b: %+v", b)
	}
	if math.Abs(b.GoodputTokensPerSec-0.3) > 1e-9 {
		t.Fatalf("goodput = %v, want 0.3", b.GoodputTokensPerSec)
	}
}
