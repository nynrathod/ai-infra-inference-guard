# inference-guard

A multi-tenant gateway for LLM inference. API-key auth, per-tenant admission
control, usage metering and streaming, in front of vLLM. One command brings up
the whole stack on GKE (GPU) or local kind (CPU) — on macOS, Linux or Windows.

```
client ──bearer──▶ gateway (Go) ──▶ vLLM (OpenAI-compatible)
                     │
                     │ usage event per request
                     ▼
        Kafka: inference-usage ──▶ usage-consumer (Go) ──▶ Redis roll-ups

Prometheus scrapes gateway + vLLM ──▶ Grafana dashboards
```

## What this is — and what it is not

**It is adaptive admission control with load shedding.** When a tenant is over
its concurrency limit, or the model server is saturated, requests are
**rejected fast** with `429 + Retry-After` so the tenant can back off. It is
**not** a bounded waiting queue, a backpressure pipe, or a weighted fair
scheduler: nothing waits inside the gateway. Reject-and-retry is the whole
design, and the benchmarks measure exactly that.

**Overload policy in one line each** (all switched via env vars, compared by
`just bench-matrix`):

| Policy | What limits requests |
|---|---|
| none | nothing (baseline) |
| static | fixed per-tenant concurrency cap |
| ratelimit | per-tenant request-rate cap (token bucket) |
| adaptive | static cap + AIMD controller that tightens/relaxes it from live saturation signals (vLLM queue depth, KV-cache pressure, per-tenant p95 time to first token) |

## What's inside

| Piece | What it does |
|---|---|
| `gateway` (Go) | Bearer token → tenant mapping, admission control (concurrency / rate / AIMD), SSE streaming proxy, Prometheus metrics, per-tenant token budgets |
| `usage-consumer` (Go) | Consumes Kafka usage events, rolls up tokens per tenant into Redis; at-least-once with idempotent (offset-keyed) writes |
| `bench` (Go) | Open-loop load generator, capacity calibration, four-control-mode comparison, SVG charts — no external dependencies |
| Kafka (Strimzi 0.43) | `inference-usage` topic between gateway and consumer |
| Redis 7.2 | Per-tenant usage roll-ups + token budget windows |
| vLLM 0.33 | OpenAI-compatible backend. Default model: **Qwen/Qwen2.5-0.5B-Instruct** (fast, cheap, reproducible; swap to a bigger model with a one-line arg change in `deploy/base`) |
| kube-prometheus-stack | Prometheus + Grafana |
| Terraform | GKE cluster, Artifact Registry, monitoring Helm release |

## Repo layout

```
cmd/gateway, cmd/usage-consumer, cmd/bench   Go services + benchmark harness
internal/                                    admission, aimd, ratelimit, metering, bench, ...
bench/scenarios/                             workload definitions (no secrets, no model names)
bench/results/                               run artifacts (gitignored; curated results live in writeup/)
deploy/base                                  kustomize base (GPU-sized vLLM)
deploy/local                                 CPU overlay for kind (KV-cache and thread tuned)
deploy/gateway, deploy/kafka, deploy/metering, deploy/dashboards   per-component manifests
infra/gcp                                    Terraform: GKE, Artifact Registry, monitoring
writeup/                                     methodology, committed results, limitations
justfile                                     cross-platform task runner (mac / linux / windows)
```

## Prerequisites

- Docker Desktop, running
- kubectl, helm, Go >= 1.26 (for `just check` and the benchmark harness)
- [just](https://github.com/casey/just) — the task runner, one-time install (10 seconds):
  - Windows: `winget install Casey.Just`
  - macOS: `brew install just`
  - Linux: `sudo apt install just`
- Cloud only: `gcloud` CLI, Terraform >= 1.5, a GCP project with billing enabled

No extra setup beyond that: on Windows the recipes run in the built-in PowerShell,
on macOS/Linux in the built-in shell.

## Run on GKE

```bash
# 1) authenticate
gcloud auth login
gcloud auth application-default login

# 2) configure
cp infra/gcp/terraform.tfvars.example infra/gcp/terraform.tfvars
#    then set project_id (your project), region, zone

# 3) review, then build everything
just plan
just up              # ~10-15 min: cluster + images + Kafka + vLLM + monitoring

# 4) prove it
just gateway-smoke
just gateway-load    # ~1-2 min of traffic so the Grafana panels have data
just grafana         # prints admin password, forwards localhost:3000

# 5) stop paying
just down            # ends with a cost check: disks/repositories/clusters should all be empty
```

`just up` is idempotent: it creates what's missing and keeps what exists. It also
bootstraps the namespaces, builds and pushes the images with a timestamped tag,
pins the Strimzi operator's Kubernetes version (the 0.43 operator cannot decode
k8s >= 1.33 versions on its own), and waits for every rollout to finish — no
manual kubectl anywhere.

## Run locally (kind, CPU)

Docker Desktop running and a kind cluster up (k8s >= 1.30), then:

```bash
just local-up
```

That installs the Strimzi operator, applies the CPU-tuned stack, and creates the
gateway API keys secret. The gateway pod recovers on its own once the secret
exists. Then use the same `gateway-smoke` / `grafana` / `check` tasks as above.

## API keys

The gateway reads `key=tenant` pairs from the `gateway-api-keys` secret:

```
dev-key-a=tenant-a,dev-key-b=tenant-b
```

The bearer token is the part **before** the `=`:

```
Authorization: Bearer dev-key-a
```

Missing or unknown key returns 401 and increments `gateway_auth_rejections_total`.

## Try it

Zero typing, works on every OS — opens the tunnel, checks health, 401, model
discovery and streaming, prints metrics:

```bash
just gateway-smoke
```

Prefer curl by hand? Open the tunnel first and keep it running
(`kubectl -n inference-guard port-forward deploy/gateway 18080:8080`), then
copy **one line at a time** for your shell. Nothing hardcodes a model name.

PowerShell 7.3+ / Linux / macOS / Git Bash / WSL (the common case):

```bash
curl -s http://localhost:18080/v1/models -H "Authorization: Bearer dev-key-a"
# use the id the command above returned (Qwen/Qwen2.5-0.5B-Instruct is the default):
curl -s http://localhost:18080/v1/chat/completions -H "Authorization: Bearer dev-key-a" -H "Content-Type: application/json" -d '{"model":"Qwen/Qwen2.5-0.5B-Instruct","messages":[{"role":"user","content":"Say OK"}],"max_tokens":5}'
```

Why two spellings: PowerShell 5.1 and PowerShell 7.3+ hand quotes to native
executables in opposite ways — no single JSON spelling works in both. Your
shell told you which block to use: if `curl` alone works, use the first
block; if you see an `Invoke-WebRequest` error, you are on 5.1 — use the
second block.

One request proves the API works, but it is invisible in Grafana: Prometheus
scrapes every 30s and the `rate()`/`increase()` panels need several samples.
Generate sustained traffic instead:

```bash
just gateway-load        # 60 streaming requests, alternating tenant-a/tenant-b
just gateway-load 200    # or any count, for a longer run
```

Then watch it in Grafana (Explore → datasource **Prometheus**):

```promql
sum by (tenant) (gateway_requests_total)
sum(increase(gateway_requests_total[10m])) by (tenant)
histogram_quantile(0.95, sum(rate(gateway_time_to_first_token_seconds_bucket[5m])) by (le, tenant))
max by (tenant) (gateway_ttft_p95_seconds)
gateway_requests_in_flight
```

Main metrics: `gateway_requests_total{status,tenant}`,
`gateway_request_duration_seconds_*`,
`gateway_time_to_first_token_seconds_*` (first **content token** — the
controller's signal; rejections excluded),
`gateway_time_to_first_byte_seconds_*` (diagnostic),
`gateway_requests_in_flight`, `gateway_auth_rejections_total`,
`gateway_aimd_saturated`, `gateway_aimd_tenant_limit`,
`gateway_ttft_p95_seconds` (per tenant), `gateway_vllm_kv_cache_usage_ratio`,
`gateway_vllm_queue_depth`.

## Benchmarks

The thesis this repo argues: **under overload, adaptive admission control keeps
p95 time to first token inside a target while delivering higher
latency-qualified goodput than no control, a static cap, or a rate limit.**

The harness (`cmd/bench`) exists to measure that claim honestly:

- **Open-loop load**: requests fire at their scheduled arrival times; the
  harness never blocks on responses. If the harness's own slot ceiling is hit,
  the send is skipped and *counted* (`harness_dropped`), never silently.
- **True TTFT**: time to the first streamed **content token**, not the first
  byte. The gateway's controller uses the same definition.
- **Latency-qualified goodput**: successes whose first token (streaming) or
  whole response (non-streaming) landed inside the scenario's
  `latency_target_ms`, per second. Raw 2xx/s and tokens/s are reported too.
- **Calibrated overload**: `just bench-matrix` first ramps offered load to
  measure sustainable capacity (success rate + latency target hold), then runs
  the comparison at **2x that measured capacity** — the overload factor is
  never a hardcoded guess.
- **Server-side cross-check**: vLLM's own `time_to_first_token_seconds`
  histogram is captured per mode as an independent diagnostic.

```bash
# after `just up` (GKE) or `just local-up` (kind):
just check                 # go vet + build + tests (incl. -race)
just bench-calibrate       # measure sustainable capacity, print + save the ramp
just bench-matrix          # all four control modes, one workload, charts + JSON
```

Matrix artifacts land in `bench/results/matrix-<timestamp>/` (per-mode JSON,
`summary.json`, `summary.md`, `goodput-by-mode.svg`, `ttft-p95-by-mode.svg`).
Curated results, methodology and limitations are committed under
[`writeup/`](writeup/).

## Known limitations (read before citing numbers)

- **Single gateway replica.** Concurrency counters and rate buckets live in the
  gateway process; scaling out requires moving them to a shared store (Redis
  already carries token budgets). The deployment is pinned to `replicas: 1`.
- **Admission control, not queueing.** Overload is answered with 429 +
  `Retry-After`; there is no bounded wait queue and no priority tiers.
- **Token reservation is an estimate.** Budgets reserve `prompt ≈ bytes/4 +
  max_tokens` up front (configurable: `GATEWAY_TOKEN_ESTIMATOR=fixed|chars4`)
  and settle to the upstream-reported usage afterwards. Simultaneous requests
  that under-declare `max_tokens` can briefly under-reserve.
- **The benchmark model is 0.5B by default.** Results label the exact model
  they ran; swap `deploy/base`'s `--model` (e.g. `Qwen/Qwen3-8B`, fits an L4)
  and re-run before claiming numbers for a bigger model.
- **AIMD thresholds ship as defaults**, not tuned constants. Tune them against
  your own calibration run (`just bench-calibrate`).

## Tasks

| just | What it does |
|---|---|
| `just tf-check` | terraform init + validate |
| `just plan` | terraform plan — free, review before spending |
| `just up` | full stack: apply, build + push images, secrets, Kafka, deploy, wait for rollouts |
| `just gateway-smoke` | health check, 401 without key, model discovery, streamed 200 with tenant attribution |
| `just gateway-load` | sustained traffic for Grafana: N streaming requests (default 60), alternating tenant-a/tenant-b |
| `just grafana` | prints the Grafana password, forwards localhost:3000 |
| `just check` | go vet + build + test (with -race) |
| `just bench-calibrate` | measure the deployed stack's sustainable capacity |
| `just bench-matrix` | four-control-mode comparison with calibrated 2x overload |
| `just down` | purges AR images, terraform destroy — back to $0, with a cost check |
| `just local-up` | kind stack: Strimzi + CPU manifests + API keys |

## Configuration

- Gateway: `GATEWAY_UPSTREAM`, `GATEWAY_VLLM_METRICS_URL`,
  `GATEWAY_TENANT_CONCURRENCY`, `GATEWAY_API_KEYS`,
  `GATEWAY_RATE_LIMIT_RPS` / `GATEWAY_RATE_LIMIT_BURST` (0 = off),
  `GATEWAY_TOKEN_ESTIMATOR` (`chars4` default, `fixed` for flat reservations),
  `GATEWAY_AIMD_ENABLED` + `GATEWAY_AIMD_*` thresholds,
  `GATEWAY_REDIS_ADDR`, `GATEWAY_KAFKA_BROKERS`, `GATEWAY_USAGE_TOPIC`,
  `GATEWAY_TENANT_BUDGETS`, `GATEWAY_DEFAULT_TENANT_BUDGET`
- Consumer: `BROKERS`, `TOPIC`, `GROUP_ID`, `REDIS_ADDR`, `REPORT_INTERVAL`
- Bench: `BENCH_TARGET`, `BENCH_VLLM_TARGET`, `BENCH_TENANT_A_KEY`,
  `BENCH_TENANT_B_KEY` (scenarios reference `env:` names; dev keys work)
- GCP: `infra/gcp/terraform.tfvars` (`project_id`, `region`, `zone`)

Swap in a bigger model by editing the vLLM args in `deploy/base` — keep
`--max-model-len` within your GPU memory.

## Troubleshooting (the ones that actually happen)

- **`Google Compute Engine does not have enough resources available ... <zone>`**
  GCP ran out of that zone. Change `zone` in terraform.tfvars to another letter in
  the same region and re-run `just up`. Everything already created is kept.
- **`failed to connect to the docker API ... dockerDesktopLinuxEngine`**
  Docker Desktop is not running. Start it, wait for it, re-run `just up`.
- **`strimzi-cluster-operator exceeded its progress deadline`**
  The operator pod did not come up. `just up` re-pins the operator's Kubernetes
  version env, restarts it once, and waits again; only if that also fails does
  it print the full diagnosis (pods, deployment conditions, events, operator
  log root errors). Re-run `just up` — it resumes from where it stopped.
- **`timed out waiting for the condition on kafkas/usage-kafka`**
  The Kafka broker did not become Ready in 15 minutes. `just up` prints the pod
  states, events and operator log automatically when this happens. A re-run of
  `just up` resumes from where it stopped — everything is idempotent.
- **Grafana draws a "Random Walk" curve**
  You are on the built-in demo datasource. Switch the datasource to **Prometheus**.
- **A benchmark warns `harness dropped ... sends`**
  The scenario's `max_in_flight` is smaller than the offered rate times the
  response latency. Run via `just bench-matrix` (sizes harness capacity from
  measured latency) or raise `max_in_flight` in the scenario.

## Cost note

The GKE control plane and nodes bill per minute while the cluster exists.
`just up` → demo → `just down` on the same day keeps the bill near zero.
`just down` also purges images from Artifact Registry so `terraform destroy`
stays clean, then lists remaining disks / repositories / clusters — all three
should report zero items.

## License

MIT — see [LICENSE](LICENSE).
