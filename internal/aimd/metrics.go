package aimd

import "github.com/prometheus/client_golang/prometheus"

// metrics instruments the controller so saturation signals and limit
// decisions are visible on the gateway's scrape endpoint.
type metrics struct {
	queueDepth   prometheus.Gauge
	kvCacheUsage prometheus.Gauge
	stale        prometheus.Gauge
	saturated    prometheus.Gauge
	ttftP95      prometheus.Gauge
	tenantLimit  *prometheus.GaugeVec
}

// newMetrics registers the controller's collectors on reg. A nil reg
// disables exposition but keeps the collectors usable.
func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		queueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gateway_vllm_queue_depth",
			Help: "Requests waiting in the model server queue, as observed by the gateway.",
		}),
		kvCacheUsage: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gateway_vllm_kv_cache_usage_ratio",
			Help: "Model server KV cache usage (0-1), as observed by the gateway.",
		}),
		stale: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gateway_vllm_signals_stale",
			Help: "1 when the gateway cannot scrape model server signals and is holding limits.",
		}),
		saturated: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gateway_aimd_saturated",
			Help: "1 while the AIMD controller considers the model server saturated.",
		}),
		ttftP95: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gateway_ttft_p95_seconds",
			Help: "p95 time to first byte over the gateway's recent observations.",
		}),
		tenantLimit: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gateway_aimd_tenant_limit",
			Help: "Current per-tenant concurrency limit enforced by admission control.",
		}, []string{"tenant"}),
	}
	if reg != nil {
		reg.MustRegister(
			m.queueDepth,
			m.kvCacheUsage,
			m.stale,
			m.saturated,
			m.ttftP95,
			m.tenantLimit,
		)
	}
	return m
}
