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
		{Tenant: "a", Outcome: OutcomeSuccess, Streamed: true, TTFTMs: 100, TotalMs: 900, Tokens: 10},
		{Tenant: "a", Outcome: OutcomeSuccess, Streamed: true, TTFTMs: 300, TotalMs: 1100, Tokens: 10},
		{Tenant: "a", Outcome: OutcomeRateLimited, TTFTMs: -1, TotalMs: 5, RetryAfterSeconds: 2},
		{Tenant: "b", Outcome: OutcomeClientAborted, Streamed: true, TTFTMs: 120, TotalMs: 200, Tokens: 3},
	}
	stats := Summarize(SummaryInput{
		Results:       results,
		PeakInFlight:  map[string]int{"a": 4, "b": 1},
		Load:          map[string]TenantLoad{"a": {Offered: 3, HarnessDropped: 0}, "b": {Offered: 1, HarnessDropped: 0}},
		ActiveSeconds: 10,
	})
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
	if a.Offered != 3 || a.HarnessDropped != 0 {
		t.Fatalf("bad offered load for a: %+v", a)
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

func TestSummarizeQualifiesByLatencyTarget(t *testing.T) {
	results := []RequestResult{
		// Streaming: judged on time to first token. Fast token, slow tail.
		{Tenant: "a", Outcome: OutcomeSuccess, Streamed: true, TTFTMs: 200, TotalMs: 5000, Tokens: 8},
		// Streaming success whose first token missed the 500ms deadline.
		{Tenant: "a", Outcome: OutcomeSuccess, Streamed: true, TTFTMs: 800, TotalMs: 900, Tokens: 8},
		// Non-streaming: judged on the whole response time.
		{Tenant: "a", Outcome: OutcomeSuccess, Streamed: false, TotalMs: 400, Tokens: 8},
		{Tenant: "a", Outcome: OutcomeSuccess, Streamed: false, TotalMs: 900, Tokens: 8},
		// A 429 never qualifies.
		{Tenant: "a", Outcome: OutcomeRateLimited, TTFTMs: -1, TotalMs: 3},
	}
	stats := Summarize(SummaryInput{
		Results:         results,
		ActiveSeconds:   10,
		LatencyTargetMS: 500,
	})
	if len(stats) != 1 {
		t.Fatalf("expected 1 tenant, got %d", len(stats))
	}
	st := stats[0]
	if st.Successes != 4 {
		t.Fatalf("successes = %d, want 4", st.Successes)
	}
	if st.QualifiedSuccesses != 2 {
		t.Fatalf("qualified successes = %d, want 2 (fast token + fast non-stream)", st.QualifiedSuccesses)
	}
	if st.GoodputQualifiedRPS != 0.2 {
		t.Fatalf("qualified goodput = %v, want 0.2 req/s", st.GoodputQualifiedRPS)
	}
}

func TestSummarizeWithoutTargetQualifiesEverySuccess(t *testing.T) {
	results := []RequestResult{
		{Tenant: "a", Outcome: OutcomeSuccess, Streamed: true, TTFTMs: 9000, TotalMs: 9000, Tokens: 1},
		{Tenant: "a", Outcome: OutcomeRateLimited, TTFTMs: -1, TotalMs: 1},
	}
	stats := Summarize(SummaryInput{Results: results, ActiveSeconds: 2})
	st := stats[0]
	if st.QualifiedSuccesses != 1 || st.GoodputQualifiedRPS != 0.5 {
		t.Fatalf("qualification without a target: %+v", st)
	}
}

func TestTotalsAggregatesAcrossTenants(t *testing.T) {
	report := RunReport{
		ActiveSeconds: 10,
		Tenants: []TenantStats{
			{Name: "a", Requests: 10, Offered: 12, HarnessDropped: 2, Successes: 8, QualifiedSuccesses: 6, CompletionTokens: 100, TTFTP95Ms: 300},
			{Name: "b", Requests: 5, Offered: 6, HarnessDropped: 1, Successes: 4, QualifiedSuccesses: 4, CompletionTokens: 50, TTFTP95Ms: 900},
		},
	}
	total := report.Totals()
	if total.Requests != 15 || total.Offered != 18 || total.HarnessDropped != 3 {
		t.Fatalf("bad totals: %+v", total)
	}
	if total.Successes != 12 || total.QualifiedSuccesses != 10 {
		t.Fatalf("bad success totals: %+v", total)
	}
	if total.TTFTP95Ms != 900 {
		t.Fatalf("worst ttft p95 = %v, want 900", total.TTFTP95Ms)
	}
	if total.GoodputQualifiedRPS != 1 {
		t.Fatalf("qualified goodput = %v, want 1", total.GoodputQualifiedRPS)
	}
}
