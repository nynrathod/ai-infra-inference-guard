# inference-guard

A multi-tenant gateway for LLM inference. API-key auth, per-tenant rate limiting,
usage metering and streaming, in front of vLLM. One command brings up the whole
stack on GKE (GPU) or local kind (CPU) — on macOS, Linux or Windows.

```
client ──bearer──▶ gateway (Go) ──▶ vLLM (OpenAI-compatible)
                     │
                     │ usage event per request
                     ▼
        Kafka: inference-usage ──▶ usage-consumer (Go) ──▶ Redis roll-ups

Prometheus scrapes gateway + vLLM ──▶ Grafana dashboards
```

## What's inside

| Piece | What it does |
|---|---|
| `gateway` (Go) | Bearer token → tenant mapping, AIMD concurrency control, SSE streaming proxy, Prometheus metrics |
| `usage-consumer` (Go) | Consumes Kafka usage events, rolls up tokens per tenant into Redis |
| Kafka (Strimzi 0.43) | `inference-usage` topic between gateway and consumer |
| Redis 7.2 | Per-tenant usage roll-ups |
| vLLM 0.33 | OpenAI-compatible backend (small Qwen model for demos) |
| kube-prometheus-stack | Prometheus + Grafana |
| Terraform | GKE cluster, Artifact Registry, monitoring Helm release |

## Repo layout

```
cmd/gateway, cmd/usage-consumer    Go services
deploy/base                        kustomize base (GPU-sized vLLM)
deploy/local                       CPU overlay for kind (KV-cache and thread tuned)
deploy/gateway, deploy/kafka, deploy/metering   per-component manifests
infra/gcp                          Terraform: GKE, Artifact Registry, monitoring
justfile                           cross-platform task runner (mac / linux / windows)
make.ps1                           Windows-native runner, same tasks as the justfile
writeup/                           results, limitations, demo script
```

## Prerequisites

- Docker Desktop, running
- kubectl, helm
- [just](https://github.com/casey/just) — the task runner, one-time install (10 seconds):
  - Windows: `winget install Casey.Just`
  - macOS: `brew install just`
  - Linux: `sudo apt install just`
- Cloud only: `gcloud` CLI, Terraform >= 1.5, a GCP project with billing enabled

No extra setup beyond that: on Windows the recipes run in the built-in PowerShell,
on macOS/Linux in the built-in shell. `make.ps1` stays in the repo as a pure
PowerShell alternative with the same tasks.

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
just grafana         # prints admin password, forwards localhost:3000

# 5) stop paying
just down
```

`just up` is idempotent: it creates what's missing and keeps what exists. It also
bootstraps the namespaces, builds and pushes the images with a timestamped tag,
and waits for every rollout to finish — no manual kubectl anywhere.

## Run locally (kind, CPU)

Docker Desktop running and a kind cluster up (k8s >= 1.30), then:

```bash
just local-up
```

That installs the Strimzi operator, applies the CPU-tuned stack, and creates the
gateway API keys secret. The gateway pod recovers on its own once the secret
exists. Then use the same `gateway-smoke` / `grafana` tasks as above.

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

After `just gateway-smoke` (or any port-forward to the gateway):

```bash
curl -s http://localhost:18080/v1/chat/completions \
  -H "Authorization: Bearer dev-key-a" \
  -H "Content-Type: application/json" \
  -d '{"model":"Qwen/Qwen3-8B","messages":[{"role":"user","content":"Say OK"}],"max_tokens":5}'
```

Generate traffic for the dashboards (1 request per second):

```bash
for i in $(seq 1 60); do curl -s -o /dev/null http://localhost:18080/v1/chat/completions -H "Authorization: Bearer dev-key-a" -H "Content-Type: application/json" -d '{"model":"Qwen/Qwen3-8B","messages":[{"role":"user","content":"Say OK"}],"max_tokens":5}'; sleep 1; done
```

Watch it in Grafana (Explore → datasource **Prometheus**):

```promql
sum by (tenant) (gateway_requests_total)
sum(increase(gateway_requests_total[10m])) by (tenant)
histogram_quantile(0.95, sum(rate(gateway_request_duration_seconds_bucket[5m])) by (le, tenant))
gateway_requests_in_flight
```

Main metrics: `gateway_requests_total{status,tenant}`, `gateway_request_duration_seconds_*`,
`gateway_time_to_first_byte_seconds_*`, `gateway_requests_in_flight`,
`gateway_auth_rejections_total`, `gateway_aimd_saturated`,
`gateway_vllm_kv_cache_usage_ratio`, `gateway_vllm_queue_depth`.

Tip: `rate()` over a long window reads as ~0 with demo traffic. Use the raw
counter or `increase()` to see real numbers.

## Tasks

| just | make.ps1 (Windows) | What it does |
|---|---|---|
| `just tf-check` | `.\make.ps1 tf-check` | terraform init + validate |
| `just plan` | `.\make.ps1 plan` | terraform plan — free, review before spending |
| `just up` | `.\make.ps1 up` | full stack: apply, build + push images, secrets, Kafka, deploy, wait for rollouts |
| `just gateway-smoke` | `.\make.ps1 gateway-smoke` | health check, 401 without key, streamed 200 with tenant attribution |
| `just grafana` | `.\make.ps1 grafana` | prints the Grafana password, forwards localhost:3000 |
| `just down` | `.\make.ps1 down` | purges AR images, terraform destroy — back to $0 |
| `just local-up` | — | kind stack: Strimzi + CPU manifests + API keys |

## Configuration

- Gateway: `GATEWAY_UPSTREAM`, `GATEWAY_VLLM_METRICS_URL`, `GATEWAY_TENANT_CONCURRENCY`, `GATEWAY_API_KEYS`
- Consumer: `BROKERS`, `TOPIC`, `GROUP_ID`, `REDIS_ADDR`, `REPORT_INTERVAL`
- GCP: `infra/gcp/terraform.tfvars` (`project_id`, `region`, `zone`)

Swap in a bigger model by editing the vLLM args in `deploy/base` — keep
`--max-model-len` within your GPU memory.

## Troubleshooting (the ones that actually happen)

- **`Google Compute Engine does not have enough resources available ... <zone>`**
  GCP ran out of that zone. Change `zone` in terraform.tfvars to another letter in
  the same region and re-run `just up`. Everything already created is kept.
- **`failed to connect to the docker API ... dockerDesktopLinuxEngine`**
  Docker Desktop is not running. Start it, wait for it, re-run `just up`.
- **`Service gateway does not have a service port 80`**
  The service port is not 80. `just gateway-smoke` reads the real port
  automatically; if you forward manually, read it first:
  `kubectl -n inference-guard get svc gateway`
- **`strimzi-cluster-operator exceeded its progress deadline`**
  The operator pod did not come up. `just up` restarts it once automatically
  and waits again; only if that also fails does it print the full diagnosis
  (pods, deployment conditions, events, operator log). Re-run `just up` - it
  resumes from where it stopped.
- **`timed out waiting for the condition on kafkas/usage-kafka`**
  The Kafka broker did not become Ready in 15 minutes. `just up` prints the pod
  states, events and operator log automatically when this happens. A re-run of
  `just up` resumes from where it stopped - everything is idempotent.
- **Grafana draws a "Random Walk" curve**
  You are on the built-in demo datasource. Switch the datasource to **Prometheus**.
- **A port-forward stops when you close its window**
  That is how port-forwards work. Keep the window open while you use the browser.

## Cost note

The GKE control plane and nodes bill per minute while the cluster exists.
`just up` → demo → `just down` on the same day keeps the bill near zero.
`just down` also purges images from Artifact Registry so `terraform destroy`
stays clean.

## License

MIT — see [LICENSE](LICENSE).