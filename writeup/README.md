# Writeup: adaptive admission control for multi-tenant LLM inference

This directory holds the methodology, the committed evidence and the
limitations of the benchmark claims. `bench/results/` is gitignored because
it collects every throwaway run; the curated runs that back a claim get
copied into `writeup/results/` with the exact reproduction command in the
file name or header.

## Method

1. **Capacity calibration.** The harness ramps a single-tenant load derived
   from the saturation scenario's request shape through increasing offered
   rates (default: start at the scenario's total QPS, step by the same,
   stop at 4x). A step is *sustainable* when its 2xx rate is at least 95%
   (configurable) and its p95 time to first token stays inside the
   scenario's `latency_target_ms`. The highest sustainable step is the
   measured capacity; the comparison runs at **2x that number**, so the
   overload factor is measured, not asserted.

2. **Open-loop load.** Requests fire at their scheduled arrival times and
   never wait for earlier responses. The harness's per-tenant slot ceiling
   (`max_in_flight`) is sized from the calibration's measured latency, and
   any send the harness itself cannot make is counted and reported as
   `harness_dropped` — the offered rate is never silently reduced.

3. **Honest latency signal.** Time to first token means the first SSE frame
   carrying non-empty generated content, measured identically on both sides:
   the gateway's controller (`gateway_time_to_first_token_seconds`) and the
   harness. First-byte time is reported separately as a diagnostic, and
   rejected requests never enter the latency window. vLLM's own
   `time_to_first_token_seconds` histogram is scraped per mode as an
   independent, server-side cross-check.

4. **Latency-qualified goodput.** The headline metric is qualified requests
   per second: 2xx responses whose first token (streaming) or whole
   response (non-streaming) landed inside `latency_target_ms`. Raw 2xx/s
   and completion tokens/s are reported alongside.

5. **Four control modes, one workload.** `just bench-matrix` runs the same
   scenario under no control, a static concurrency cap, a per-tenant rate
   limit (set to the measured capacity split across tenants), and the
   adaptive AIMD controller — reconfiguring the gateway via env vars
   between runs and restoring its original configuration afterwards.

## Reproduce

```bash
just up                  # or: just local-up
just check               # vet + build + tests (incl. -race)
just bench-calibrate     # writes bench/results/calibration-*.json
just bench-matrix        # writes bench/results/matrix-<timestamp>/
cp -r bench/results/matrix-<timestamp> writeup/results/<date>-<model>
```

Each committed result directory contains `summary.md` (the comparison
table), `summary.json`, one JSON report per control mode, and the two SVG
charts. The header of `summary.md` records model, latency target, measured
capacity and the applied load scale.

## Limitations

- Single gateway replica: concurrency counters and rate buckets are
  in-process; results apply to one gateway pod.
- Reject-only overload behavior (429 + Retry-After): there is no queueing,
  so "goodput under overload" means "useful work admitted and served", not
  "requests buffered then served".
- Token budgets reserve an estimate (prompt bytes/4 + max_tokens) and
  settle to upstream-reported usage; transient under-reservation is
  possible when clients under-declare max_tokens.
- The default model is Qwen/Qwen2.5-0.5B-Instruct. Results are only valid
  for the model named in their header; re-run after swapping `--model`.
- AIMD thresholds are shipped defaults; tune them against your own
  calibration before drawing conclusions from the adaptive mode.

## Results index

| Directory | Model | Environment | Headline |
|---|---|---|---|
| _(empty — run `just bench-matrix` and commit the artifacts here)_ | | | |
