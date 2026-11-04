// Package metrics instruments the gateway with Prometheus metrics and
// exposes them on a scrape endpoint.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is the gateway's Prometheus instrumentation. All instruments are
// registered on a private registry so the gateway never collides with
// default-process collectors and tests run in isolation.
type Metrics struct {
	reg              *prometheus.Registry
	requestsTotal    *prometheus.CounterVec
	requestsInFlight *prometheus.GaugeVec
	requestDuration  *prometheus.HistogramVec
	timeToFirstByte  *prometheus.HistogramVec
	authRejections   *prometheus.CounterVec
}

// New returns an initialized Metrics ready to be wired into the handler
// chain.
func New() *Metrics {
	m := &Metrics{
		reg: prometheus.NewRegistry(),
		requestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_requests_total",
			Help: "Completed gateway requests by tenant and response status.",
		}, []string{"tenant", "status"}),
		requestsInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gateway_requests_in_flight",
			Help: "Requests currently being served, by tenant.",
		}, []string{"tenant"}),
		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gateway_request_duration_seconds",
			Help:    "Total time requests spend in the gateway, by tenant.",
			Buckets: prometheus.DefBuckets,
		}, []string{"tenant"}),
		timeToFirstByte: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gateway_time_to_first_byte_seconds",
			Help:    "Time from request admission to the first response byte, by tenant.",
			Buckets: prometheus.DefBuckets,
		}, []string{"tenant"}),
		authRejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_auth_rejections_total",
			Help: "Requests rejected by authentication, by reason.",
		}, []string{"reason"}),
	}
	m.reg.MustRegister(
		m.requestsTotal,
		m.requestsInFlight,
		m.requestDuration,
		m.timeToFirstByte,
		m.authRejections,
	)
	return m
}

// Handler serves the Prometheus scrape endpoint.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// RecordRejection counts a request that failed authentication.
func (m *Metrics) RecordRejection(reason string) {
	m.authRejections.WithLabelValues(reason).Inc()
}
