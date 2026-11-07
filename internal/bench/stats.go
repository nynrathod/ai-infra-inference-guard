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
	TTFTMs            float64 // time to first content token; -1 when never seen (non-streaming)
	TotalMs           float64 // whole request; -1 when no response was received
	Streamed          bool    // the request asked for SSE streaming
	Tokens            int     // completion tokens observed
	RetryAfterSeconds float64 // > 0 only on 429 with a Retry-After header
}

// TenantLoad records the load the harness offered for one tenant, before
// any gateway behavior: how many sends were scheduled and how many the
// harness itself had to skip because its slot ceiling was full. A non-zero
// HarnessDropped means max_in_flight was too small for the offered rate and
// the results understate the intended load.
type TenantLoad struct {
	Offered        int `json:"offered"`
	HarnessDropped int `json:"harness_dropped"`
}

// TenantStats summarizes one tenant's outcomes over a run.
type TenantStats struct {
	Name                 string  `json:"name"`
	Requests             int     `json:"requests"`
	Offered              int     `json:"offered"`
	HarnessDropped       int     `json:"harness_dropped"`
	Successes            int     `json:"successes"`
	QualifiedSuccesses   int     `json:"qualified_successes"`
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
	GoodputQualifiedRPS  float64 `json:"goodput_qualified_rps"`
	MaxInFlightObserved  int     `json:"max_in_flight_observed"`
	MaxRetryAfterSeconds float64 `json:"max_retry_after_seconds"`
}

// RunReport is the full result of one scenario run, written to disk as JSON.
type RunReport struct {
	Scenario        string        `json:"scenario"`
	Description     string        `json:"description"`
	Target          string        `json:"target"`
	Model           string        `json:"model"`
	StartedAt       time.Time     `json:"started_at"`
	WallSeconds     float64       `json:"wall_seconds"`
	ActiveSeconds   float64       `json:"active_seconds"`
	LatencyTargetMS float64       `json:"latency_target_ms"`
	Tenants         []TenantStats `json:"tenants"`
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

// SummaryInput carries everything Summarize needs.
type SummaryInput struct {
	// Results are the recorded request outcomes.
	Results []RequestResult
	// PeakInFlight is the harness-observed concurrency peak per tenant.
	PeakInFlight map[string]int
	// Load is the offered/dropped accounting per tenant.
	Load map[string]TenantLoad
	// ActiveSeconds is the window requests were issued over.
	ActiveSeconds float64
	// LatencyTargetMS is the scenario's SLO deadline: a successful request
	// counts toward the qualified goodput only when its first token (for
	// streaming) or whole response (non-streaming) arrived within it. Zero
	// disables qualification and every success is qualified.
	LatencyTargetMS float64
}

// qualified reports whether one successful request met the latency target.
func (in SummaryInput) qualified(r RequestResult) bool {
	if r.Outcome != OutcomeSuccess {
		return false
	}
	if in.LatencyTargetMS <= 0 {
		return true
	}
	if r.Streamed {
		return r.TTFTMs >= 0 && r.TTFTMs <= in.LatencyTargetMS
	}
	return r.TotalMs >= 0 && r.TotalMs <= in.LatencyTargetMS
}

// Summarize reduces raw results into per-tenant stats, in first-seen tenant
// order. Two goodput numbers are reported: tokens per second (raw useful
// work) and qualified requests per second — successes that met the latency
// target, the number the thesis actually argues about. Rejected, errored
// and deadline-missing requests dilute both.
func Summarize(in SummaryInput) []TenantStats {
	var order []string
	byName := make(map[string]*TenantStats)
	ttfts := make(map[string][]float64)
	e2es := make(map[string][]float64)

	for _, r := range in.Results {
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
		if in.qualified(r) {
			st.QualifiedSuccesses++
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
		st.MaxInFlightObserved = in.PeakInFlight[name]
		if load, ok := in.Load[name]; ok {
			st.Offered = load.Offered
			st.HarnessDropped = load.HarnessDropped
		}
		if in.ActiveSeconds > 0 {
			st.GoodputTokensPerSec = float64(st.CompletionTokens) / in.ActiveSeconds
			st.GoodputQualifiedRPS = float64(st.QualifiedSuccesses) / in.ActiveSeconds
		}
		stats = append(stats, *st)
	}
	return stats
}

// Totals aggregates one report's headline numbers across all tenants; used
// by the comparison matrix to chart one bar per control mode.
func (r RunReport) Totals() TenantStats {
	var total TenantStats
	total.Name = "total"
	for _, st := range r.Tenants {
		total.Requests += st.Requests
		total.Offered += st.Offered
		total.HarnessDropped += st.HarnessDropped
		total.Successes += st.Successes
		total.QualifiedSuccesses += st.QualifiedSuccesses
		total.RateLimited += st.RateLimited
		total.ServerErrors += st.ServerErrors
		total.HTTPErrors += st.HTTPErrors
		total.Timeouts += st.Timeouts
		total.ClientAborted += st.ClientAborted
		total.TransportErrors += st.TransportErrors
		total.CompletionTokens += st.CompletionTokens
		if st.TTFTP95Ms > total.TTFTP95Ms {
			total.TTFTP95Ms = st.TTFTP95Ms
		}
		if st.E2EP95Ms > total.E2EP95Ms {
			total.E2EP95Ms = st.E2EP95Ms
		}
		if st.MaxRetryAfterSeconds > total.MaxRetryAfterSeconds {
			total.MaxRetryAfterSeconds = st.MaxRetryAfterSeconds
		}
	}
	if r.ActiveSeconds > 0 {
		total.GoodputTokensPerSec = float64(total.CompletionTokens) / r.ActiveSeconds
		total.GoodputQualifiedRPS = float64(total.QualifiedSuccesses) / r.ActiveSeconds
	}
	return total
}
