// Command bench runs one benchmark scenario against a gateway endpoint,
// prints a per-tenant summary, and writes the full report as JSON under
// bench/results for later analysis.
//
// Typical use against a local port-forward:
//
// go run ./cmd/bench -scenario bench/scenarios/01-baseline.json -target http://localhost:8080
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
	"path/filepath"

	"github.com/nynrathod/inference-guard/internal/bench"
)

func main() {
	var (
		scenarioPath = flag.String("scenario", "", "path to the scenario JSON file (required)")
		target       = flag.String("target", envOr("BENCH_TARGET", "http://127.0.0.1:8080"), "gateway base URL")
		model        = flag.String("model", "", "override the scenario's served-model name")
		seed         = flag.Int64("seed", 1, "random seed for disconnect decisions")
		output       = flag.String("output", "", "report path (default bench/results/<scenario>-<timestamp>.json)")
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

	runner := bench.NewRunner(*target)
	runner.Model = *model
	runner.Seed = *seed

	fmt.Printf("scenario %q against %s: %d tenant(s), %s sending window\n",
		scenario.Name, runner.Target, len(scenario.Tenants), scenario.Duration())

	report, err := runner.Run(context.Background(), scenario)
	if err != nil {
		fatal(err)
	}

	outPath := *output
	if outPath == "" {
		outPath = filepath.Join("bench", "results", fmt.Sprintf("%s-%s.json",
			scenario.Name, report.StartedAt.Format("20060102T150405Z")))
	}
	if err := writeReport(outPath, report); err != nil {
		fatal(err)
	}

	printSummary(report)
	fmt.Printf("\nreport written to %s\n", outPath)
}

// writeReport stores the report as indented JSON, creating the directory.
func writeReport(path string, report bench.RunReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

// printSummary shows the headline numbers per tenant.
func printSummary(r bench.RunReport) {
	fmt.Printf("\n%-12s %6s %6s %6s %6s %10s %10s %10s %8s\n",
		"tenant", "reqs", "ok", "429", "abort", "ttft p50", "ttft p95", "e2e p95", "tok/s")
	for _, t := range r.Tenants {
		fmt.Printf("%-12s %6d %6d %6d %6d %9.0fms %9.0fms %9.0fms %8.1f\n",
			t.Name, t.Requests, t.Successes, t.RateLimited, t.ClientAborted,
			t.TTFTP50Ms, t.TTFTP95Ms, t.E2EP95Ms, t.GoodputTokensPerSec)
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
