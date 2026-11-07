package bench

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ControlMode is one overload policy the comparison matrix can deploy on
// the gateway via environment variables.
type ControlMode string

const (
	// ModeNone disables every control: concurrency effectively unbounded,
	// AIMD off, no rate limit. The baseline the others must beat.
	ModeNone ControlMode = "none"
	// ModeStatic applies a fixed per-tenant concurrency cap, never adapted.
	ModeStatic ControlMode = "static"
	// ModeRateLimit caps each tenant's request arrival rate instead of
	// concurrency.
	ModeRateLimit ControlMode = "ratelimit"
	// ModeAdaptive is the full system: static cap plus the AIMD controller
	// tightening and relaxing it from live saturation signals.
	ModeAdaptive ControlMode = "adaptive"
)

// DefaultModes is the comparison the thesis argues about, in run order.
var DefaultModes = []ControlMode{ModeNone, ModeStatic, ModeRateLimit, ModeAdaptive}

// ModeSettings is the gateway configuration one control mode maps to.
type ModeSettings struct {
	Concurrency  int     `json:"concurrency"`
	AIMD         bool    `json:"aimd"`
	RateLimitRPS float64 `json:"rate_limit_rps"`
}

// ModeSettingsFor maps a control mode onto gateway settings.
//   - staticConcurrency is the deployment's configured cap (used by static
//     and adaptive modes);
//   - unboundedConcurrency is a deliberately high cap that keeps concurrency
//     control out of the way for the none and ratelimit modes;
//   - rateRPS is the per-tenant rate limit for the ratelimit mode.
func ModeSettingsFor(mode ControlMode, staticConcurrency, unboundedConcurrency int, rateRPS float64) (ModeSettings, error) {
	switch mode {
	case ModeNone:
		return ModeSettings{Concurrency: unboundedConcurrency, AIMD: false, RateLimitRPS: 0}, nil
	case ModeStatic:
		return ModeSettings{Concurrency: staticConcurrency, AIMD: false, RateLimitRPS: 0}, nil
	case ModeRateLimit:
		if rateRPS <= 0 {
			return ModeSettings{}, fmt.Errorf("ratelimit mode needs a positive rate (calibrate first or pass -rate-rps)")
		}
		return ModeSettings{Concurrency: unboundedConcurrency, AIMD: false, RateLimitRPS: rateRPS}, nil
	case ModeAdaptive:
		return ModeSettings{Concurrency: staticConcurrency, AIMD: true, RateLimitRPS: 0}, nil
	default:
		return ModeSettings{}, fmt.Errorf("unknown control mode %q", mode)
	}
}

// Env renders the settings as kubectl set env assignments.
func (s ModeSettings) Env() []string {
	return []string{
		"GATEWAY_TENANT_CONCURRENCY=" + strconv.Itoa(s.Concurrency),
		"GATEWAY_AIMD_ENABLED=" + strconv.FormatBool(s.AIMD),
		"GATEWAY_RATE_LIMIT_RPS=" + strconv.FormatFloat(s.RateLimitRPS, 'f', -1, 64),
	}
}

// VLLMDelta is the model server's own view of one mode's run, captured from
// its /metrics endpoint before and after: an independent diagnostic next to
// the harness's measurements.
type VLLMDelta struct {
	// TTFTCount is how many requests the server measured TTFT for; TTFTSum
	// is their total, so MeanTTFT is the server-side mean time to first
	// token.
	TTFTCount       int64   `json:"ttft_count"`
	TTFTSumSeconds  float64 `json:"ttft_sum_seconds"`
	MeanTTFTSeconds float64 `json:"mean_ttft_seconds"`
	// KVCacheUsage and QueueDepth are the last observed values of the run.
	KVCacheUsage float64 `json:"kv_cache_usage"`
	QueueDepth   float64 `json:"queue_depth"`
}

// ModeResult is one mode's full measurement.
type ModeResult struct {
	Mode     ControlMode  `json:"mode"`
	Settings ModeSettings `json:"settings"`
	Report   RunReport    `json:"report"`
	VLLM     VLLMDelta    `json:"vllm"`
}

// MatrixSummary aggregates the whole comparison.
type MatrixSummary struct {
	StartedAt       time.Time     `json:"started_at"`
	Scenario        string        `json:"scenario"`
	Model           string        `json:"model"`
	LatencyTargetMS float64       `json:"latency_target_ms"`
	QPSScale        float64       `json:"qps_scale"`
	SustainableQPS  float64       `json:"sustainable_qps"`
	SaturationQPS   float64       `json:"saturation_qps"`
	Modes           []ModeResult  `json:"modes"`
}

// MatrixOptions configures a matrix run.
type MatrixOptions struct {
	// Kubectl is the cluster CLI executable (default "kubectl").
	Kubectl string
	// Namespace and Deployment locate the gateway.
	Namespace  string
	Deployment string
	// Scenario is run identically under every mode.
	Scenario Scenario
	// Modes lists the control modes to compare (default DefaultModes).
	Modes []ControlMode
	// QPSScale scales the scenario's offered load (calibration output).
	QPSScale float64
	// StaticConcurrency is the cap for static/adaptive modes. 0 = read the
	// deployment's current GATEWAY_TENANT_CONCURRENCY.
	StaticConcurrency int
	// UnboundedConcurrency keeps concurrency out of the way in none and
	// ratelimit modes (default 1000).
	UnboundedConcurrency int
	// RateLimitRPS is the per-tenant rate for ratelimit mode. 0 = derive
	// from Calibration: sustainable capacity split across tenants.
	RateLimitRPS float64
	// Calibration supplies measured capacity; may be nil.
	Calibration *CalibrationResult
	// VLLMTarget is the model server's base URL for diagnostic scrapes; may
	// be empty to skip server-side capture.
	VLLMTarget string
	// OutDir receives per-mode JSON, the summary and the charts; may be
	// empty to only return results.
	OutDir string
	// RolloutTimeout bounds each gateway reconfiguration (default 3m).
	RolloutTimeout time.Duration
}

// RunMatrix runs the scenario under every requested control mode against
// the same gateway, reconfiguring the gateway via kubectl between runs and
// restoring its original environment afterwards. The runner's Target must
// already point at the gateway (typically through a managed port-forward).
func RunMatrix(ctx context.Context, r *Runner, opts MatrixOptions) (*MatrixSummary, error) {
	if opts.Kubectl == "" {
		opts.Kubectl = "kubectl"
	}
	if opts.Namespace == "" {
		opts.Namespace = "inference-guard"
	}
	if opts.Deployment == "" {
		opts.Deployment = "gateway"
	}
	if len(opts.Modes) == 0 {
		opts.Modes = DefaultModes
	}
	if opts.UnboundedConcurrency <= 0 {
		opts.UnboundedConcurrency = 1000
	}
	if opts.RolloutTimeout <= 0 {
		opts.RolloutTimeout = 3 * time.Minute
	}
	if opts.StaticConcurrency <= 0 {
		conc, err := readDeploymentEnv(ctx, opts.Kubectl, opts.Namespace, opts.Deployment, "GATEWAY_TENANT_CONCURRENCY")
		if err != nil {
			return nil, fmt.Errorf("read the deployment's concurrency cap (or pass -static-concurrency): %w", err)
		}
		n, err := strconv.Atoi(strings.TrimSpace(conc))
		if err != nil || n < 1 {
			return nil, fmt.Errorf("deployment reports GATEWAY_TENANT_CONCURRENCY=%q, not a positive integer", conc)
		}
		opts.StaticConcurrency = n
	}
	if opts.RateLimitRPS <= 0 && opts.Calibration != nil && opts.Calibration.SustainableQPS > 0 && len(opts.Scenario.Tenants) > 0 {
		opts.RateLimitRPS = opts.Calibration.SustainableQPS / float64(len(opts.Scenario.Tenants))
	}

	original, err := readDeploymentEnvs(ctx, opts.Kubectl, opts.Namespace, opts.Deployment,
		"GATEWAY_TENANT_CONCURRENCY", "GATEWAY_AIMD_ENABLED", "GATEWAY_RATE_LIMIT_RPS")
	if err != nil {
		return nil, fmt.Errorf("snapshot gateway environment: %w", err)
	}
	restore := func() error {
		return applyGatewayEnv(ctx, opts, restoreEnv(original))
	}
	// Whatever happens, put the gateway back the way it was found.
	defer func() { _ = restore() }()

	scenario := opts.Scenario
	if opts.Calibration != nil {
		scenario.EnsureHarnessCapacity(opts.Calibration.MeasuredE2EP95Seconds)
	}

	summary := &MatrixSummary{
		StartedAt:       time.Now().UTC(),
		Scenario:        scenario.Name,
		LatencyTargetMS: scenario.LatencyTargetMS,
		QPSScale:        r.QPSScale,
	}
	if opts.Calibration != nil {
		summary.SustainableQPS = opts.Calibration.SustainableQPS
		summary.SaturationQPS = opts.Calibration.SuggestedSaturationQPS
	}

	for _, mode := range opts.Modes {
		settings, err := ModeSettingsFor(mode, opts.StaticConcurrency, opts.UnboundedConcurrency, opts.RateLimitRPS)
		if err != nil {
			return nil, err
		}
		if err := applyGatewayEnv(ctx, opts, settings.Env()); err != nil {
			return nil, fmt.Errorf("switch gateway to %s mode: %w", mode, err)
		}

		var before vllmSnapshot
		haveVLLM := opts.VLLMTarget != ""
		if haveVLLM {
			if before, err = scrapeVLLM(ctx, opts.VLLMTarget); err != nil {
				haveVLLM = false // diagnostics are best-effort
			}
		}

		report, err := r.Run(ctx, scenario)
		if err != nil {
			return nil, fmt.Errorf("run %s mode: %w", mode, err)
		}

		result := ModeResult{Mode: mode, Settings: settings, Report: report}
		if haveVLLM {
			if after, err := scrapeVLLM(ctx, opts.VLLMTarget); err == nil {
				result.VLLM = after.delta(before)
			}
		}
		if summary.Model == "" {
			summary.Model = report.Model
		}
		summary.Modes = append(summary.Modes, result)

		if opts.OutDir != "" {
			if err := writeJSON(filepath.Join(opts.OutDir, string(mode)+".json"), result); err != nil {
				return nil, err
			}
		}
	}

	if err := restore(); err != nil {
		return nil, fmt.Errorf("restore gateway environment: %w", err)
	}

	if opts.OutDir != "" {
		if err := writeMatrixArtifacts(opts.OutDir, summary); err != nil {
			return nil, err
		}
	}
	return summary, nil
}

// restoreEnv turns the captured environment back into set-env assignments,
// using kubectl's KEY- syntax to remove variables the deployment did not
// carry before the matrix ran.
func restoreEnv(original map[string]string) []string {
	order := []string{"GATEWAY_TENANT_CONCURRENCY", "GATEWAY_AIMD_ENABLED", "GATEWAY_RATE_LIMIT_RPS"}
	out := make([]string, 0, len(order))
	for _, key := range order {
		if value, ok := original[key]; ok {
			out = append(out, key+"="+value)
		} else {
			out = append(out, key+"-")
		}
	}
	return out
}

func applyGatewayEnv(ctx context.Context, opts MatrixOptions, assignments []string) error {
	args := []string{"-n", opts.Namespace, "set", "env", "deployment/" + opts.Deployment}
	args = append(args, assignments...)
	if out, err := runCmd(ctx, opts.Kubectl, args...); err != nil {
		return fmt.Errorf("kubectl set env: %s: %w", strings.TrimSpace(out), err)
	}
	args = []string{"-n", opts.Namespace, "rollout", "status", "deployment/" + opts.Deployment,
		"--timeout=" + opts.RolloutTimeout.String()}
	if out, err := runCmd(ctx, opts.Kubectl, args...); err != nil {
		return fmt.Errorf("gateway rollout: %s: %w", strings.TrimSpace(out), err)
	}
	return nil
}

func readDeploymentEnv(ctx context.Context, kubectl, namespace, deployment, key string) (string, error) {
	jsonpath := fmt.Sprintf(`{.spec.template.spec.containers[0].env[?(@.name=="%s")].value}`, key)
	args := []string{"-n", namespace, "get", "deployment", deployment, "-o", "jsonpath=" + jsonpath}
	out, err := runCmd(ctx, kubectl, args...)
	return strings.TrimSpace(out), err
}

func readDeploymentEnvs(ctx context.Context, kubectl, namespace, deployment string, keys ...string) (map[string]string, error) {
	out := make(map[string]string, len(keys))
	for _, key := range keys {
		value, err := readDeploymentEnv(ctx, kubectl, namespace, deployment, key)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", key, err)
		}
		if value != "" {
			out[key] = value
		}
	}
	return out, nil
}

func runCmd(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func writeJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

// writeMatrixArtifacts writes summary.json, a human-readable summary.md and
// the two comparison charts.
func writeMatrixArtifacts(outDir string, summary *MatrixSummary) error {
	if err := writeJSON(filepath.Join(outDir, "summary.json"), summary); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, "summary.md"), []byte(summary.Markdown()), 0o644); err != nil {
		return err
	}

	goodput := make([]Bar, 0, len(summary.Modes))
	ttft := make([]Bar, 0, len(summary.Modes))
	for _, m := range summary.Modes {
		total := m.Report.Totals()
		goodput = append(goodput, Bar{
			Label: string(m.Mode),
			Value: total.GoodputQualifiedRPS,
			Sub:   fmt.Sprintf("%.0f tok/s", total.GoodputTokensPerSec),
		})
		ttft = append(ttft, Bar{
			Label: string(m.Mode),
			Value: total.TTFTP95Ms,
			Sub:   fmt.Sprintf("%.0f reqs", float64(total.Requests)),
		})
	}
	if f, err := os.Create(filepath.Join(outDir, "goodput-by-mode.svg")); err == nil {
		err = WriteBarChartSVG(f, "Latency-qualified goodput by control mode", "qualified requests / second", goodput)
		f.Close()
		if err != nil {
			return err
		}
	} else {
		return err
	}
	if f, err := os.Create(filepath.Join(outDir, "ttft-p95-by-mode.svg")); err == nil {
		err = WriteBarChartSVG(f, "p95 time to first token by control mode", "milliseconds", ttft)
		f.Close()
		if err != nil {
			return err
		}
	} else {
		return err
	}
	return nil
}

// Markdown renders the comparison as a table for the writeup.
func (s *MatrixSummary) Markdown() string {
	var sb strings.Builder
	sb.WriteString("# Control-mode comparison: ")
	sb.WriteString(s.Scenario)
	sb.WriteString("\n\n")
	fmt.Fprintf(&sb, "- model: `%s`\n", s.Model)
	if s.LatencyTargetMS > 0 {
		fmt.Fprintf(&sb, "- latency target: first token within %.0f ms (streaming) / whole response (non-streaming)\n", s.LatencyTargetMS)
	}
	if s.SustainableQPS > 0 {
		fmt.Fprintf(&sb, "- measured sustainable capacity: %.1f req/s; saturation run offered %.1f req/s (%.1fx)\n",
			s.SustainableQPS, s.SaturationQPS, s.SaturationQPS/s.SustainableQPS)
	}
	fmt.Fprintf(&sb, "- load scale vs scenario file: %.2fx\n\n", s.QPSScale)
	sb.WriteString("| mode | offered | requests | 2xx | qualified | qualified/s | tok/s | TTFT p95 | 429s | server TTFT mean |\n")
	sb.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, m := range s.Modes {
		t := m.Report.Totals()
		fmt.Fprintf(&sb, "| %s | %d | %d | %d | %d | %.2f | %.1f | %.0f ms | %d | %.0f ms |\n",
			m.Mode, t.Offered, t.Requests, t.Successes, t.QualifiedSuccesses,
			t.GoodputQualifiedRPS, t.GoodputTokensPerSec, t.TTFTP95Ms, t.RateLimited,
			m.VLLM.MeanTTFTSeconds*1000)
	}
	sb.WriteString("\nEvery run used the same workload file and the same gateway; only the\ncontrol policy differed (gateway environment variables, applied and\nrestored by the harness).\n")
	return sb.String()
}

// vllmSnapshot is one scrape of the model server's /metrics endpoint.
type vllmSnapshot struct {
	ttftCount float64
	ttftSum   float64
	kvCache   float64
	queue     float64
}

// Candidate metric names, mirroring the aimd scraper's version tolerance.
var (
	vllmTTFTNames    = []string{"vllm:time_to_first_token_seconds", "time_to_first_token_seconds"}
	vllmKVCacheNames = []string{"vllm:kv_cache_usage_perc", "vllm:gpu_cache_usage_perc", "vllm:kv_cache_usage_ratio"}
	vllmQueueNames   = []string{"vllm:num_requests_waiting", "num_requests_waiting"}
)

func scrapeVLLM(ctx context.Context, target string) (vllmSnapshot, error) {
	var snap vllmSnapshot
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, strings.TrimRight(target, "/")+"/metrics", nil)
	if err != nil {
		return snap, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return snap, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return snap, fmt.Errorf("vllm metrics: status %d", resp.StatusCode)
	}

	counters := make(map[string]float64)
	gauges := make(map[string]float64)
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		name := fields[0]
		if i := strings.IndexByte(name, '{'); i >= 0 {
			name = name[:i]
		}
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}
		if strings.HasSuffix(name, "_count") || strings.HasSuffix(name, "_sum") ||
			strings.HasSuffix(name, "_total") || strings.HasSuffix(name, "_bucket") {
			counters[name] = value
		} else {
			gauges[name] = value
		}
	}
	if err := scanner.Err(); err != nil {
		return snap, err
	}

	for _, base := range vllmTTFTNames {
		if v, ok := counters[base+"_count"]; ok {
			snap.ttftCount = v
			snap.ttftSum = counters[base+"_sum"]
			break
		}
	}
	for _, name := range vllmKVCacheNames {
		if v, ok := gauges[name]; ok {
			snap.kvCache = v
			break
		}
	}
	for _, name := range vllmQueueNames {
		if v, ok := gauges[name]; ok {
			snap.queue = v
			break
		}
	}
	return snap, nil
}

// delta converts two snapshots into the per-run server-side view. Counters
// only ever grow, so a negative delta means the counters were reset and the
// values are reported from the after-snapshot alone.
func (after vllmSnapshot) delta(before vllmSnapshot) VLLMDelta {
	d := VLLMDelta{
		TTFTCount:      int64(after.ttftCount - before.ttftCount),
		TTFTSumSeconds: after.ttftSum - before.ttftSum,
		KVCacheUsage:   after.kvCache,
		QueueDepth:     after.queue,
	}
	if d.TTFTCount < 0 || d.TTFTSumSeconds < 0 {
		d.TTFTCount, d.TTFTSumSeconds = int64(after.ttftCount), after.ttftSum
	}
	if d.TTFTCount > 0 {
		d.MeanTTFTSeconds = d.TTFTSumSeconds / float64(d.TTFTCount)
	}
	return d
}

// PortForward runs `kubectl port-forward` in the background and waits until
// the forwarded endpoint answers, so benchmark runs and scrapes never need
// a manually opened terminal.
type PortForward struct {
	cmd    *exec.Cmd
	cancel context.CancelFunc
	local  int
}

// StartPortForward forwards local:remote on resource (e.g. "deploy/gateway")
// and blocks until probePath answers with any HTTP status.
func StartPortForward(ctx context.Context, kubectl, namespace, resource string, local, remote int, probePath string) (*PortForward, error) {
	fwdCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(fwdCtx, kubectl, "-n", namespace, "port-forward", resource,
		fmt.Sprintf("127.0.0.1:%d:%d", local, remote))
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start kubectl port-forward: %w", err)
	}

	pf := &PortForward{cmd: cmd, cancel: cancel, local: local}
	probeURL := fmt.Sprintf("http://127.0.0.1:%d%s", local, probePath)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			pf.Stop()
			return nil, ctx.Err()
		default:
		}
		reqCtx, reqCancel := context.WithTimeout(context.Background(), 2*time.Second)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, probeURL, nil)
		if err == nil {
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				resp.Body.Close()
				reqCancel()
				return pf, nil // any HTTP answer means the tunnel is up
			}
		}
		reqCancel()
		time.Sleep(500 * time.Millisecond)
	}
	pf.Stop()
	return nil, fmt.Errorf("port-forward to %s did not become ready within 30s", resource)
}

// Stop tears the tunnel down.
func (p *PortForward) Stop() {
	if p == nil {
		return
	}
	p.cancel()
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	_, _ = p.cmd.Process.Wait()
}
