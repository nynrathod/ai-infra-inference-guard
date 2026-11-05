// Package aimd implements an AIMD (additive increase / multiplicative
// decrease) controller that adapts per-tenant concurrency limits to model
// server saturation signals: request queue depth, KV cache usage, and the
// gateway's own measured time to first byte.
package aimd

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Config bounds how the controller adapts limits.
type Config struct {
	// BaseLimit is the per-tenant concurrency limit before any adaptation.
	BaseLimit int
	// MinLimit is the floor a tenant limit never drops below.
	MinLimit int
	// DecreaseFactor multiplies limits on each saturated tick (within (0,1]).
	DecreaseFactor float64
	// AdditiveStep raises limits by this many slots per recovery tick.
	AdditiveStep int
	// RecoveryTicks is the number of consecutive healthy ticks required
	// before limits start growing again.
	RecoveryTicks int
	// QueueDepthHigh is the waiting-queue depth treated as saturation.
	QueueDepthHigh float64
	// KVCacheHigh is the KV cache usage (0..1) treated as saturation.
	KVCacheHigh float64
	// TTFTHigh is the measured p95 time to first byte treated as saturation.
	TTFTHigh time.Duration
	// DominantShare is the fraction of global in-flight requests above
	// which a tenant is considered a noisy neighbor and cut harder.
	DominantShare float64
	// Interval between control ticks.
	Interval time.Duration
	// StaleAfter is the number of consecutive failed scrapes after which
	// signals are reported stale and limits are held.
	StaleAfter int
}

// SignalSource yields the current model server saturation signals.
type SignalSource interface {
	Scrape(ctx context.Context) (Signals, error)
}

// Limiter is the runtime-adjustable admission surface the controller drives.
type Limiter interface {
	SetLimit(tenant string, limit int)
	Snapshot() map[string]int
}

// Controller watches saturation signals and adapts per-tenant limits with an
// AIMD policy: multiplicative decrease while the model server is saturated,
// additive increase once it has been healthy for several ticks. Tenants
// holding a dominant share of in-flight work during saturation are cut one
// extra round, containing noisy neighbors.
type Controller struct {
	cfg     Config
	source  SignalSource
	limiter Limiter
	window  *TTFTWindow
	metrics *metrics
	log     *slog.Logger

	mu          sync.Mutex
	limits      map[string]int
	healthy     int
	saturated   bool
	failures    int
	staleLogged bool
}

// New returns a Controller. reg may be nil to disable instrumentation.
func New(cfg Config, source SignalSource, limiter Limiter, reg prometheus.Registerer, log *slog.Logger) *Controller {
	if cfg.StaleAfter < 1 {
		cfg.StaleAfter = 3
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 2 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	return &Controller{
		cfg:     cfg,
		source:  source,
		limiter: limiter,
		window:  NewTTFTWindow(512),
		metrics: newMetrics(reg),
		log:     log,
		limits:  make(map[string]int),
	}
}

// ObserveTTFT records one gateway-measured time to first byte, attributed to
// its tenant. It is safe for concurrent use and intended to be wired as the
// metrics package's TTFT observer.
func (c *Controller) ObserveTTFT(tenant string, d time.Duration) {
	c.window.Observe(d)
}

// Run executes the control loop until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) {
	c.log.Info("aimd controller started",
		"interval", c.cfg.Interval.String(),
		"base_limit", c.cfg.BaseLimit,
		"min_limit", c.cfg.MinLimit)
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.log.Info("aimd controller stopped")
			return
		case <-ticker.C:
			c.tick(ctx)
		}
	}
}

// tick performs one control decision.
func (c *Controller) tick(ctx context.Context) {
	sig, err := c.source.Scrape(ctx)
	if err != nil {
		c.handleScrapeFailure(err)
		return
	}
	c.failures = 0
	c.staleLogged = false
	c.metrics.stale.Set(0)

	if sig.HasQueueDepth {
		c.metrics.queueDepth.Set(sig.QueueDepth)
	}
	if sig.HasKVCache {
		c.metrics.kvCacheUsage.Set(sig.KVCacheUsage)
	}
	ttftP95, hasTTFT := c.window.Percentile(0.95)
	if hasTTFT {
		c.metrics.ttftP95.Set(ttftP95)
	}

	saturated, reason := evaluate(sig, ttftP95, hasTTFT, c.cfg)

	c.mu.Lock()
	defer c.mu.Unlock()
	if saturated {
		if c.healthy > 0 && c.healthy < c.cfg.RecoveryTicks {
			// Saturation returned before recovery was confirmed: the previous
			// decrease is still draining from the model server, so restart the
			// healthy streak instead of cutting a second time.
			c.healthy = 0
		} else {
			c.healthy = 0
			c.decrease(c.limiter.Snapshot())
		}
	} else {
		c.healthy++
		if c.healthy >= c.cfg.RecoveryTicks {
			c.increase()
		}
	}
	if saturated != c.saturated {
		c.saturated = saturated
		c.metrics.saturated.Set(boolFloat(saturated))
		if saturated {
			c.log.Warn("model server saturated, tightening limits", "reason", reason)
		} else {
			c.log.Info("model server healthy, relaxing limits")
		}
	}
	c.publishLimits()
}

// handleScrapeFailure counts consecutive failures and reports stale signals
// once the configured threshold is crossed, without touching any limits.
func (c *Controller) handleScrapeFailure(err error) {
	c.failures++
	if c.failures < c.cfg.StaleAfter {
		return
	}
	c.metrics.stale.Set(1)
	if !c.staleLogged {
		c.log.Warn("model server signals unavailable, holding limits", "error", err)
		c.staleLogged = true
	}
}

// decrease applies one round of multiplicative decrease to every tenant with
// in-flight work. Tenants holding at least DominantShare of global in-flight
// work are cut a second round. Caller must hold c.mu.
func (c *Controller) decrease(snapshot map[string]int) {
	total := 0
	for _, n := range snapshot {
		total += n
	}
	for tenant, inFlight := range snapshot {
		current := c.effectiveLimit(tenant)
		next := c.scaleDown(current)
		if total > 1 && inFlight > 0 &&
			float64(inFlight) >= c.cfg.DominantShare*float64(total) {
			next = c.scaleDown(next)
		}
		if next < current {
			c.limits[tenant] = next
			c.limiter.SetLimit(tenant, next)
			c.log.Info("tenant limit decreased",
				"tenant", tenant, "from", current, "to", next, "in_flight", inFlight)
		}
	}
}

// increase raises every reduced tenant limit one additive step toward the
// base limit. Caller must hold c.mu.
func (c *Controller) increase() {
	for tenant, limit := range c.limits {
		if limit >= c.cfg.BaseLimit {
			continue
		}
		next := limit + c.cfg.AdditiveStep
		if next > c.cfg.BaseLimit {
			next = c.cfg.BaseLimit
		}
		c.limits[tenant] = next
		c.limiter.SetLimit(tenant, next)
		if next == c.cfg.BaseLimit {
			c.log.Info("tenant limit restored", "tenant", tenant, "limit", next)
		}
	}
}

// scaleDown applies the decrease factor with the configured floor.
func (c *Controller) scaleDown(limit int) int {
	next := int(math.Ceil(float64(limit) * c.cfg.DecreaseFactor))
	if next < c.cfg.MinLimit {
		next = c.cfg.MinLimit
	}
	if next > limit {
		next = limit
	}
	if next < 1 {
		next = 1
	}
	return next
}

// effectiveLimit is the tenant's current limit, defaulting to the base limit
// for tenants the controller has not adjusted yet.
func (c *Controller) effectiveLimit(tenant string) int {
	if limit, ok := c.limits[tenant]; ok {
		return limit
	}
	return c.cfg.BaseLimit
}

// publishLimits mirrors the controller's view of tenant limits into metrics.
// Caller must hold c.mu.
func (c *Controller) publishLimits() {
	for tenant, limit := range c.limits {
		c.metrics.tenantLimit.WithLabelValues(tenant).Set(float64(limit))
	}
}

// evaluate reports whether the model server is saturated and why. Signal
// precedence: queue depth, then KV cache pressure, then TTFT. Signals the
// server did not expose are never treated as saturation.
func evaluate(sig Signals, ttftP95 float64, hasTTFT bool, cfg Config) (bool, string) {
	if sig.HasQueueDepth && sig.QueueDepth >= cfg.QueueDepthHigh {
		return true, "queue_depth"
	}
	if sig.HasKVCache && sig.KVCacheUsage >= cfg.KVCacheHigh {
		return true, "kv_cache"
	}
	if hasTTFT && ttftP95 >= cfg.TTFTHigh.Seconds() {
		return true, "ttft_p95"
	}
	return false, ""
}

func boolFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
