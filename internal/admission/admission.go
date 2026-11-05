// Package admission enforces per-tenant limits on concurrent in-flight
// requests, protecting the upstream model server from being monopolized by
// any single tenant.
package admission

import (
	"net/http"
	"sync"

	"github.com/nynrathod/inference-guard/internal/apierror"
	"github.com/nynrathod/inference-guard/internal/auth"
)

// Controller tracks in-flight requests per tenant and admits a request only
// while its tenant is below its concurrency limit. Limits are adjustable at
// runtime so an overload policy can tighten or relax them without a restart.
type Controller struct {
	mu            sync.Mutex
	defaultLimit  int
	limits        map[string]int
	inFlightCount map[string]int
}

// New returns a Controller admitting at most defaultLimit concurrent
// requests per tenant.
func New(defaultLimit int) *Controller {
	return &Controller{
		defaultLimit:  defaultLimit,
		limits:        make(map[string]int),
		inFlightCount: make(map[string]int),
	}
}

// Middleware admits requests whose tenant is below its concurrency limit,
// responding 429 otherwise, and releases the slot when the request finishes.
func (c *Controller) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant, ok := auth.TenantFromContext(r.Context())
		if !ok {
			apierror.Write(w, http.StatusInternalServerError, "api_error", "tenant_unresolved",
				"tenant could not be resolved")
			return
		}
		if !c.Acquire(tenant) {
			w.Header().Set("Retry-After", "1")
			apierror.Write(w, http.StatusTooManyRequests, "rate_limit_error", "tenant_concurrency_limit",
				"tenant concurrency limit reached, retry shortly")
			return
		}
		defer c.Release(tenant)
		next.ServeHTTP(w, r)
	})
}

// Acquire claims one in-flight slot for the tenant, reporting false when the
// tenant is already at its limit.
func (c *Controller) Acquire(tenant string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inFlightCount[tenant] >= c.limitLocked(tenant) {
		return false
	}
	c.inFlightCount[tenant]++
	return true
}

// Release returns one in-flight slot for the tenant.
func (c *Controller) Release(tenant string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inFlightCount[tenant] > 0 {
		c.inFlightCount[tenant]--
	}
}

// InFlight reports the number of requests currently admitted for the tenant.
func (c *Controller) InFlight(tenant string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inFlightCount[tenant]
}

// Snapshot returns a copy of the current in-flight counts per tenant.
func (c *Controller) Snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.inFlightCount))
	for tenant, n := range c.inFlightCount {
		out[tenant] = n
	}
	return out
}

// Limit reports the current concurrency limit for the tenant.
func (c *Controller) Limit(tenant string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.limitLocked(tenant)
}

// SetLimit overrides the concurrency limit for one tenant.
func (c *Controller) SetLimit(tenant string, limit int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.limits[tenant] = limit
}

func (c *Controller) limitLocked(tenant string) int {
	if limit, ok := c.limits[tenant]; ok {
		return limit
	}
	return c.defaultLimit
}
