// Command bench measures the gateway under load.
//
// Single scenario (against an already-forwarded gateway):
//
//      go run ./cmd/bench -scenario bench/scenarios/01-baseline.json -target http://localhost:8080
//
// Capacity calibration (ramps offered load, reports sustainable req/s):
//
//      go run ./cmd/bench -scenario bench/scenarios/02-saturation.json -calibrate
//
// Full four-mode comparison (manages its own port-forwards, reconfigures
// the gateway via kubectl, restores it afterwards, writes JSON + charts):
//
//      go run ./cmd/bench -scenario bench/scenarios/02-saturation.json -matrix
//
// API keys come from the environment (scenarios reference env:VAR names),
// so no secret ever lives in the repository.
package main

import (
        "context"
        "encoding/json"
        "flag"
        "fmt"
        "os"
        "os/signal"
        "path/filepath"
        "strings"
        "syscall"
        "time"

        "github.com/nynrathod/inference-guard/internal/bench"
)

func main() {
        var (
                scenarioPath = flag.String("scenario", "", "path to the scenario JSON file (required)")
                target       = flag.String("target", envOr("BENCH_TARGET", "http://127.0.0.1:8080"), "gateway base URL")
                model        = flag.String("model", "", "override the served-model name (default: scenario value or /v1/models discovery)")
                seed         = flag.Int64("seed", 1, "random seed for disconnect decisions")
                output       = flag.String("output", "", "single-run report path (default bench/results/<scenario>-<timestamp>.json)")
                outDir       = flag.String("out-dir", filepath.Join("bench", "results"), "directory for calibration and matrix artifacts")

                qpsScale        = flag.Float64("qps-scale", 1, "multiply every tenant's offered rate (shifts the whole profile)")
                latencyTargetMS = flag.Float64("latency-target-ms", 0, "SLO deadline qualifying successes for goodput (0 = scenario value)")

                calibrate       = flag.Bool("calibrate", false, "measure sustainable capacity with a stepped load ramp")
                calFrom         = flag.Float64("cal-from", 0, "calibration start rate (default: scenario total qps)")
                calStep         = flag.Float64("cal-step", 0, "calibration step increment (default: scenario total qps)")
                calMax          = flag.Float64("cal-max", 0, "calibration max rate (default: 4x scenario total qps)")
                calSeconds      = flag.Int("cal-seconds", 20, "seconds per calibration step (minimum 5)")
                calSuccessRate  = flag.Float64("cal-success-rate", 0.95, "success rate a sustainable step must meet")

                matrix          = flag.Bool("matrix", false, "run the scenario under every control mode and compare")
                matrixCalibrate = flag.Bool("matrix-calibrate", true, "calibrate capacity first and run the matrix at 2x it")
                modes           = flag.String("modes", "none,static,ratelimit,adaptive", "comma-separated control modes for -matrix")
                namespace       = flag.String("namespace", "inference-guard", "gateway namespace for -matrix")
                deployment      = flag.String("deployment", "gateway", "gateway deployment name for -matrix")
                kubectl         = flag.String("kubectl", "kubectl", "kubectl executable for -matrix")
                staticConc      = flag.Int("static-concurrency", 0, "concurrency cap for static/adaptive modes (0 = read from the deployment)")
                unboundedConc   = flag.Int("unbounded-concurrency", 1000, "concurrency used where concurrency control must stay out of the way")
                rateRPS         = flag.Float64("rate-rps", 0, "per-tenant rate for ratelimit mode (0 = calibrated capacity / tenants)")
                vllmTarget      = flag.String("vllm-target", envOr("BENCH_VLLM_TARGET", "http://127.0.0.1:18000"), "vLLM base URL for server-side TTFT diagnostics (empty disables)")
                gwLocalPort     = flag.Int("gateway-local-port", 18081, "local port the matrix opens for its gateway port-forward")
                vllmLocalPort   = flag.Int("vllm-local-port", 18000, "local port the matrix opens for its vLLM port-forward")
        )
        flag.Parse()

        if *scenarioPath == "" {
                flag.Usage()
                os.Exit(2)
        }
        scenario, err := bench.LoadScenario(*scenarioPath)
        if err != nil {
                fatal(err)
        }
        if *latencyTargetMS > 0 {
                scenario.LatencyTargetMS = *latencyTargetMS
        }

        ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
        defer stop()

        // Calibration on its own or as the matrix's first phase.
        var calibration *bench.CalibrationResult
        if *calibrate || (*matrix && *matrixCalibrate) {
                runner := bench.NewRunner(*target)
                runner.Model = *model
                cal, err := runner.Calibrate(ctx, scenario, bench.CalibrateOptions{
                        FromQPS:           *calFrom,
                        StepQPS:           *calStep,
                        MaxQPS:            *calMax,
                        StepSeconds:       *calSeconds,
                        LatencyTargetMS:   scenario.LatencyTargetMS,
                        AcceptSuccessRate: *calSuccessRate,
                })
                if err != nil {
                        fatal(err)
                }
                calibration = &cal
                printCalibration(cal)

                stamp := cal.StartedAt.Format("20060102T150405Z")
                if err := writeJSON(filepath.Join(*outDir, fmt.Sprintf("calibration-%s.json", stamp)), cal); err != nil {
                        fatal(err)
                }
        }

        switch {
        case *matrix:
                runMatrix(ctx, matrixConfig{
                        scenario:   scenario,
                        model:      *model,
                        seed:       *seed,
                        qpsScale:   *qpsScale,
                        modes:      *modes,
                        namespace:  *namespace,
                        deployment: *deployment,
                        kubectl:    *kubectl,
                        staticConc: *staticConc,
                        unbound:    *unboundedConc,
                        rateRPS:    *rateRPS,
                        vllm:       *vllmTarget,
                        outDir:     *outDir,
                        gwPort:     *gwLocalPort,
                        vllmPort:   *vllmLocalPort,
                        calibration: calibration,
                        matrixCalibrate: *matrixCalibrate,
                })

        case *calibrate:
                // Already ran above; nothing more to do.

        default:
                runner := bench.NewRunner(*target)
                runner.Model = *model
                runner.Seed = *seed
                runner.QPSScale = *qpsScale

                fmt.Printf("scenario %q against %s: %d tenant(s), %s sending window\n",
                        scenario.Name, runner.Target, len(scenario.Tenants), scenario.Duration())
                report, err := runner.Run(ctx, scenario)
                if err != nil {
                        fatal(err)
                }

                outPath := *output
                if outPath == "" {
                        outPath = filepath.Join(*outDir, fmt.Sprintf("%s-%s.json",
                                scenario.Name, report.StartedAt.Format("20060102T150405Z")))
                }
                if err := writeReport(outPath, report); err != nil {
                        fatal(err)
                }
                printSummary(report)
                warnHarnessDrops(report)
                fmt.Printf("\nreport written to %s\n", outPath)
        }
}

// matrixConfig carries the flag values into runMatrix.
type matrixConfig struct {
        scenario   bench.Scenario
        model      string
        seed       int64
        qpsScale   float64
        modes      string
        namespace  string
        deployment string
        kubectl    string
        staticConc int
        unbound    int
        rateRPS    float64
        vllm       string
        outDir     string
        gwPort     int
        vllmPort   int
        calibration      *bench.CalibrationResult
        matrixCalibrate  bool
}

func runMatrix(ctx context.Context, cfg matrixConfig) {
        // The matrix manages its own port-forwards so it never collides with a
        // tunnel the user happens to have open.
        gwForward, err := bench.StartPortForward(ctx, cfg.kubectl, cfg.namespace, "deploy/"+cfg.deployment, cfg.gwPort, 8080, "/healthz")
        if err != nil {
                fatal(err)
        }
        defer gwForward.Stop()

        var vllmForward *bench.PortForward
        vllmTarget := cfg.vllm
        if vllmTarget != "" {
                if vllmForward, err = bench.StartPortForward(ctx, cfg.kubectl, cfg.namespace, "deploy/vllm", cfg.vllmPort, 8000, "/health"); err != nil {
                        fmt.Fprintf(os.Stderr, "bench: vLLM port-forward failed (%v); continuing without server-side diagnostics\n", err)
                        vllmTarget = ""
                } else {
                        defer vllmForward.Stop()
                }
        }

        runner := bench.NewRunner(fmt.Sprintf("http://127.0.0.1:%d", cfg.gwPort))
        runner.Model = cfg.model
        runner.Seed = cfg.seed

        // Calibrated runs scale the scenario so the total offered load is the
        // measured saturation point (2x sustainable capacity), unless the user
        // pinned a scale themselves.
        if cfg.calibration != nil && cfg.calibration.SustainableQPS > 0 && cfg.qpsScale == 1 {
                total := cfg.scenario.TotalQPS()
                if total > 0 {
                        runner.QPSScale = cfg.calibration.SuggestedSaturationQPS / total
                }
        } else {
                runner.QPSScale = cfg.qpsScale
        }

        opts := bench.MatrixOptions{
                Kubectl:              cfg.kubectl,
                Namespace:            cfg.namespace,
                Deployment:           cfg.deployment,
                Scenario:             cfg.scenario,
                QPSScale:             runner.QPSScale,
                StaticConcurrency:    cfg.staticConc,
                UnboundedConcurrency: cfg.unbound,
                RateLimitRPS:         cfg.rateRPS,
                Calibration:          cfg.calibration,
                VLLMTarget:           vllmTarget,
                OutDir:               filepath.Join(cfg.outDir, fmt.Sprintf("matrix-%s", time.Now().UTC().Format("20060102T150405Z"))),
        }
        for _, m := range splitAndTrim(cfg.modes) {
                opts.Modes = append(opts.Modes, bench.ControlMode(m))
        }

        fmt.Printf("matrix: %d mode(s) against %s, load scaled %.2fx (total offered %.1f req/s)\n",
                len(opts.Modes), runner.Target, runner.QPSScale, cfg.scenario.TotalQPS()*runner.QPSScale)

        summary, err := bench.RunMatrix(ctx, runner, opts)
        if err != nil {
                fatal(err)
        }

        fmt.Print(summary.Markdown())
        for _, m := range summary.Modes {
                warnHarnessDrops(m.Report)
        }
        fmt.Printf("\nmatrix artifacts written to %s\n", opts.OutDir)
}

func splitAndTrim(csv string) []string {
        var out []string
        for _, part := range strings.Split(csv, ",") {
                if part = strings.TrimSpace(part); part != "" {
                        out = append(out, part)
                }
        }
        return out
}

func printCalibration(cal bench.CalibrationResult) {
        fmt.Printf("\ncalibration (%s, model %s):\n", cal.Target, cal.Model)
        fmt.Printf("%10s %9s %9s %11s %12s\n", "offered", "requests", "success", "ttft p95", "sustainable")
        for _, s := range cal.Steps {
                fmt.Printf("%9.0fq %9d %8.0f%% %9.0fms %12v\n",
                        s.OfferedQPS, s.Requests, s.SuccessRate*100, s.TTFTP95Ms, s.Sustainable)
        }
        fmt.Printf("sustainable capacity: %.1f req/s -> saturation scenario should offer %.1f req/s (2x)\n\n",
                cal.SustainableQPS, cal.SuggestedSaturationQPS)
}

// writeReport stores the report as indented JSON, creating the directory.
func writeReport(path string, report bench.RunReport) error {
        return writeJSON(path, report)
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

// printSummary shows the headline numbers per tenant.
func printSummary(r bench.RunReport) {
        fmt.Printf("\n%-12s %7s %6s %6s %6s %6s %10s %10s %10s %9s %9s\n",
                "tenant", "offered", "reqs", "ok", "qual", "429", "ttft p50", "ttft p95", "e2e p95", "qual/s", "tok/s")
        for _, t := range r.Tenants {
                fmt.Printf("%-12s %7d %6d %6d %6d %6d %9.0fms %9.0fms %9.0fms %9.2f %8.1f\n",
                        t.Name, t.Offered, t.Requests, t.Successes, t.QualifiedSuccesses, t.RateLimited,
                        t.TTFTP50Ms, t.TTFTP95Ms, t.E2EP95Ms, t.GoodputQualifiedRPS, t.GoodputTokensPerSec)
        }
        if r.LatencyTargetMS > 0 {
                fmt.Printf("\nqual = 2xx with first token (streaming) or whole response (non-streaming) within %.0f ms\n",
                        r.LatencyTargetMS)
        }
}

// warnHarnessDrops flags runs whose offered load was understated by the
// harness itself.
func warnHarnessDrops(r bench.RunReport) {
        var dropped, offered int
        for _, t := range r.Tenants {
                dropped += t.HarnessDropped
                offered += t.Offered
        }
        if dropped > 0 {
                fmt.Fprintf(os.Stderr,
                        "WARNING: the harness dropped %d of %d scheduled sends (max_in_flight too low) - results understate the offered load; raise max_in_flight or run via -matrix, which sizes it from measured latency\n",
                        dropped, offered)
        }
}

func fatal(err error) {
        fmt.Fprintf(os.Stderr, "bench: %v\n", err)
        os.Exit(1)
}

func envOr(name, fallback string) string {
        if value := os.Getenv(name); value != "" {
                return value
        }
        return fallback
}
