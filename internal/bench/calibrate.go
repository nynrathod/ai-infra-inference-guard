package bench

import (
	"context"
	"fmt"
	"time"
)

// CalibrationStep is one measured point of the capacity curve.
type CalibrationStep struct {
	OfferedQPS       float64 `json:"offered_qps"`
	Requests         int     `json:"requests"`
	SuccessRate      float64 `json:"success_rate"`
	TTFTP95Ms        float64 `json:"ttft_p95_ms"`
	E2EP95Ms         float64 `json:"e2e_p95_ms"`
	QualifiedPerSec  float64 `json:"qualified_per_sec"`
	Sustainable      bool    `json:"sustainable"`
}

// CalibrationResult is the measured sustainable capacity of the deployed
// stack under the calibrated request shape, plus the full ramp curve.
type CalibrationResult struct {
	Target          string            `json:"target"`
	Model           string            `json:"model"`
	StartedAt       time.Time         `json:"started_at"`
	StepSeconds     int               `json:"step_seconds"`
	LatencyTargetMS float64           `json:"latency_target_ms"`
	Steps           []CalibrationStep `json:"steps"`
	// SustainableQPS is the highest offered rate that still met the
	// acceptance criteria (success rate and, when set, the latency target).
	SustainableQPS float64 `json:"sustainable_qps"`
	// SuggestedSaturationQPS is 2x SustainableQPS: the offered load the
	// saturation scenario should use so "2x overload" is measured, not
	// asserted.
	SuggestedSaturationQPS float64 `json:"suggested_saturation_qps"`
	// MeasuredE2EP95Seconds feeds harness-capacity sizing for later runs.
	MeasuredE2EP95Seconds float64 `json:"measured_e2e_p95_seconds"`
}

// CalibrateOptions bounds the calibration ramp. Zero values derive sensible
// defaults from the scenario: the ramp starts at the scenario's total
// offered rate and doubles twice, in steps of the scenario's own rate.
type CalibrateOptions struct {
	FromQPS     float64
	StepQPS     float64
	MaxQPS      float64
	StepSeconds int
	// LatencyTargetMS is the deadline a sustainable step must meet (0 =
	// success rate only).
	LatencyTargetMS float64
	// AcceptSuccessRate is the minimum fraction of 2xx responses a
	// sustainable step must deliver. Default 0.95.
	AcceptSuccessRate float64
}

// minSuccessRate is the floor for AcceptSuccessRate; the criterion is meant
// to find where quality starts collapsing, not to be tuned away.
const minSuccessRate = 0.5

// Calibrate ramps a single-tenant load derived from the scenario's first
// tenant (same prompt shape, stream settings and token budget) through
// increasing offered rates and reports the highest sustainable one. The
// harness slot ceiling is sized from the previous step's measured latency so
// the generator stays open loop even while the server degrades.
func (r *Runner) Calibrate(ctx context.Context, s Scenario, opts CalibrateOptions) (CalibrationResult, error) {
	if len(s.Tenants) == 0 {
		return CalibrationResult{}, fmt.Errorf("calibration needs at least one tenant to derive the request shape")
	}
	base := s.Tenants[0]
	if opts.FromQPS <= 0 {
		opts.FromQPS = s.TotalQPS()
	}
	if opts.FromQPS <= 0 {
		opts.FromQPS = 1
	}
	if opts.StepQPS <= 0 {
		opts.StepQPS = opts.FromQPS
	}
	if opts.MaxQPS <= 0 {
		opts.MaxQPS = 4 * opts.FromQPS
	}
	if opts.StepSeconds < 5 {
		opts.StepSeconds = 20
	}
	if opts.AcceptSuccessRate < minSuccessRate {
		opts.AcceptSuccessRate = 0.95
	}
	target := opts.LatencyTargetMS
	if target <= 0 {
		target = s.LatencyTargetMS
	}

	keys, err := s.ResolveAPIKeys()
	if err != nil {
		return CalibrationResult{}, err
	}
	model, err := r.ResolveModel(ctx, s, keys)
	if err != nil {
		return CalibrationResult{}, err
	}

	result := CalibrationResult{
		Target:          r.Target,
		Model:           model,
		StartedAt:       time.Now().UTC(),
		StepSeconds:     opts.StepSeconds,
		LatencyTargetMS: target,
	}

	for qps := opts.FromQPS; qps <= opts.MaxQPS+1e-9; qps += opts.StepQPS {
		step := Scenario{
			Name:                  fmt.Sprintf("%s-calibration-%.0fqps", s.Name, qps),
			Description:           fmt.Sprintf("capacity ramp step at %.0f offered qps", qps),
			Model:                 model,
			ChatTemplateKwargs:    s.ChatTemplateKwargs,
			DurationSeconds:       opts.StepSeconds,
			CooldownSeconds:       5,
			RequestTimeoutSeconds: s.RequestTimeoutSeconds,
			LatencyTargetMS:       target,
			Tenants: []TenantConfig{{
				Name:           base.Name,
				APIKey:         base.APIKey,
				QPS:            qps,
				MaxInFlight:    int(qps)*10 + 16, // harness headroom, not a control
				Stream:         base.Stream,
				MaxTokens:      base.MaxTokens,
				PromptTokens:   base.PromptTokens,
			}},
		}
		if err := step.Validate(); err != nil {
			return CalibrationResult{}, fmt.Errorf("calibration step %.0f qps: %w", qps, err)
		}
		// Keep the harness open-loop as latency grows: size the ceiling
		// from the worst latency seen so far.
		step.EnsureHarnessCapacity(result.MeasuredE2EP95Seconds)

		report, err := r.Run(ctx, step)
		if err != nil {
			return CalibrationResult{}, err
		}
		total := report.Totals()

		cs := CalibrationStep{
			OfferedQPS:      qps,
			Requests:        total.Requests,
			TTFTP95Ms:       total.TTFTP95Ms,
			E2EP95Ms:        total.E2EP95Ms,
			QualifiedPerSec: total.GoodputQualifiedRPS,
		}
		if total.Requests > 0 {
			cs.SuccessRate = float64(total.Successes) / float64(total.Requests)
		}
		if total.E2EP95Ms > 0 {
			result.MeasuredE2EP95Seconds = total.E2EP95Ms / 1000
		}
		cs.Sustainable = cs.SuccessRate >= opts.AcceptSuccessRate &&
			(total.HarnessDropped == 0) &&
			(target <= 0 || total.TTFTP95Ms <= target || !base.Stream)
		if cs.Sustainable {
			result.SustainableQPS = qps
		}
		result.Steps = append(result.Steps, cs)
	}

	result.SuggestedSaturationQPS = 2 * result.SustainableQPS
	return result, nil
}
