package bench

import (
	"math"
	"sort"
	"time"
)

// Outcome classifies one request attempt.
type Outcome string

const (
	OutcomeSuccess       Outcome = "success"
	OutcomeRateLimited   Outcome = "rate_limited" // HTTP 429 from admission control
	OutcomeServerError   Outcome = "server_error" // HTTP 5xx, or a 200 with a broken body
	OutcomeHTTPErr       Outcome = "http_error"   // any other non-200
	OutcomeTimeout       Outcome = "timeout"
	OutcomeClientAborted Outcome = "client_aborted" // the harness cut the stream on purpose
	OutcomeTransport     Outcome = "transport_error"
)

// RequestResult records one request attempt.
type RequestResult struct {
	Tenant            string
	Outcome           Outcome
	TTFTMs            float64 // time to first content token; -1 when never seen
	TotalMs           float64 // whole request; -1 when no response was received
	Tokens            int     // completion tokens observed
	RetryAfterSeconds float64 // > 0 only on 429 with a Retry-After header
}

// TenantStats summarizes one tenant's outcomes over a run.
type TenantStats struct {
	Name                 string  `json:"name"`
	Requests             int     `json:"requests"`
	Successes            int     `json:"successes"`
	RateLimited          int     `json:"rate_limited"`
	ServerErrors         int     `json:"server_errors"`
	HTTPErrors           int     `json:"http_errors"`
	Timeouts             int     `json:"timeouts"`
	ClientAborted        int     `json:"client_aborted"`
	TransportErrors      int     `json:"transport_errors"`
	CompletionTokens     int64   `json:"completion_tokens"`
	TTFTP50Ms            float64 `json:"ttft_p50_ms"`
	TTFTP95Ms            float64 `json:"ttft_p95_ms"`
	TTFTP99Ms            float64 `json:"ttft_p99_ms"`
	E2EP50Ms             float64 `json:"e2e_p50_ms"`
	E2EP95Ms             float64 `json:"e2e_p95_ms"`
	E2EP99Ms             float64 `json:"e2e_p99_ms"`
	GoodputTokensPerSec  float64 `json:"goodput_tokens_per_sec"`
	MaxInFlightObserved  int     `json:"max_in_flight_observed"`
	MaxRetryAfterSeconds float64 `json:"max_retry_after_seconds"`
}

// RunReport is the full result of one scenario run, written to disk as JSON.
type RunReport struct {
	Scenario      string        `json:"scenario"`
	Description   string        `json:"description"`
	Target        string        `json:"target"`
	Model         string        `json:"model"`
	StartedAt     time.Time     `json:"started_at"`
	WallSeconds   float64       `json:"wall_seconds"`
	ActiveSeconds float64       `json:"active_seconds"`
	Tenants       []TenantStats `json:"tenants"`
}

// Percentile returns the nearest-rank p-th percentile of xs (p in (0,100]).
// An empty slice yields 0. The input is not modified.
func Percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sorted := append([]float64(nil), xs...)
	sort.Float64s(sorted)
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// Summarize reduces raw results into per-tenant stats, in first-seen tenant
// order. activeSeconds is the window requests were issued over; goodput is
// completion tokens divided by it, so rejected and aborted requests dilute
// it - the honest number for useful work per second.
func Summarize(results []RequestResult, peakInFlight map[string]int, activeSeconds float64) []TenantStats {
	var order []string
	byName := make(map[string]*TenantStats)
	ttfts := make(map[string][]float64)
	e2es := make(map[string][]float64)

	for _, r := range results {
		st, ok := byName[r.Tenant]
		if !ok {
			st = &TenantStats{Name: r.Tenant}
			byName[r.Tenant] = st
			order = append(order, r.Tenant)
		}
		st.Requests++
		switch r.Outcome {
		case OutcomeSuccess:
			st.Successes++
		case OutcomeRateLimited:
			st.RateLimited++
		case OutcomeServerError:
			st.ServerErrors++
		case OutcomeHTTPErr:
			st.HTTPErrors++
		case OutcomeTimeout:
			st.Timeouts++
		case OutcomeClientAborted:
			st.ClientAborted++
		case OutcomeTransport:
			st.TransportErrors++
		}
		st.CompletionTokens += int64(r.Tokens)
		if r.RetryAfterSeconds > st.MaxRetryAfterSeconds {
			st.MaxRetryAfterSeconds = r.RetryAfterSeconds
		}
		if r.TTFTMs >= 0 {
			ttfts[r.Tenant] = append(ttfts[r.Tenant], r.TTFTMs)
		}
		if r.TotalMs >= 0 {
			e2es[r.Tenant] = append(e2es[r.Tenant], r.TotalMs)
		}
	}

	stats := make([]TenantStats, 0, len(order))
	for _, name := range order {
		st := byName[name]
		st.TTFTP50Ms = Percentile(ttfts[name], 50)
		st.TTFTP95Ms = Percentile(ttfts[name], 95)
		st.TTFTP99Ms = Percentile(ttfts[name], 99)
		st.E2EP50Ms = Percentile(e2es[name], 50)
		st.E2EP95Ms = Percentile(e2es[name], 95)
		st.E2EP99Ms = Percentile(e2es[name], 99)
		st.MaxInFlightObserved = peakInFlight[name]
		if activeSeconds > 0 {
			st.GoodputTokensPerSec = float64(st.CompletionTokens) / activeSeconds
		}
		stats = append(stats, *st)
	}
	return stats
}
