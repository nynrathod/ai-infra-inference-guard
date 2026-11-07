// Package ratelimit enforces a per-tenant request-rate limit (token bucket)
// in front of the model server. Where admission control caps concurrent
// in-flight requests, this caps the request *arrival rate* per tenant — an
// alternative overload policy that can be selected with a single env var,
// GATEWAY_RATE_LIMIT_RPS (0 disables the middleware entirely).
package ratelimit

import (
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/nynrathod/inference-guard/internal/apierror"
	"github.com/nynrathod/inference-guard/internal/auth"
)

// Limiter is a per-tenant token bucket refilling at rps tokens per second up
// to burst tokens. Each request consumes one token; a tenant with an empty
// bucket gets a 429 with Retry-After. It is safe for concurrent use.
type Limiter struct {
	mu      sync.Mutex
	rps     float64
	burst   float64
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

// New returns a Limiter refilling at rps requests per second. A burst below
// one is raised to one so a bucket always admits at least a single request
// between refills; rps must be positive.
func New(rps float64, burst int) *Limiter {
	if burst < 1 {
		burst = 1
	}
	return &Limiter{
		rps:     rps,
		burst:   float64(burst),
		buckets: make(map[string]*bucket),
	}
}

// Allow reports whether the tenant may send one more request right now.
func (l *Limiter) Allow(tenant string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[tenant]
	if !ok {
		// A new bucket starts full: an idle tenant is never penalized for
		// the time it was away. Opening the bucket consumes one token like
		// any other request, so a burst of N admits exactly N back-to-back
		// requests; the N+1th must wait for a refill.
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[tenant] = b
	} else {
		elapsed := now.Sub(b.last).Seconds()
		if elapsed > 0 {
			b.tokens += elapsed * l.rps
			if b.tokens > l.burst {
				b.tokens = l.burst
			}
			b.last = now
		}
	}
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// retryAfter estimates how long the tenant must wait for one token.
func (l *Limiter) retryAfter(tenant string) time.Duration {
	l.mu.Lock()
	b, ok := l.buckets[tenant]
	l.mu.Unlock()
	if !ok || l.rps <= 0 {
		return time.Second
	}
	deficit := 1 - b.tokens
	if deficit <= 0 {
		return 0
	}
	return time.Duration(math.Ceil(deficit / l.rps * float64(time.Second)))
}

// Middleware admits requests whose tenant still has rate budget, responding
// 429 with Retry-After otherwise. It must sit below auth (it needs the
// tenant) and above metering (rejected requests must not reserve tokens).
func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant, ok := auth.TenantFromContext(r.Context())
		if !ok {
			apierror.Write(w, http.StatusInternalServerError, "api_error", "tenant_unresolved",
				"tenant could not be resolved")
			return
		}
		if !l.Allow(tenant, time.Now()) {
			w.Header().Set("Retry-After", strconv.Itoa(int(math.Max(1, l.retryAfter(tenant).Seconds()))))
			apierror.Write(w, http.StatusTooManyRequests, "rate_limit_error", "tenant_rate_limit",
				"tenant request rate limit reached, retry shortly")
			return
		}
		next.ServeHTTP(w, r)
	})
}
