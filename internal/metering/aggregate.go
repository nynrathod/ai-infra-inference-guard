package metering

import (
	"sync"
	"time"
)

// TenantUsage accumulates one tenant's counters for a single day.
type TenantUsage struct {
	Requests         int64
	PromptTokens     int64
	CompletionTokens int64
}

// DayKey returns the UTC day an event belongs to ("2026-11-04").
func DayKey(t time.Time) string {
	return t.UTC().Format("2006-01-02")
}

// Aggregator is a concurrency-safe, in-memory per-day, per-tenant usage
// roll-up used by the usage-event consumer for periodic reporting.
type Aggregator struct {
	mu      sync.Mutex
	tenants map[string]map[string]*TenantUsage // day -> tenant -> totals
}

// NewAggregator returns an empty Aggregator.
func NewAggregator() *Aggregator {
	return &Aggregator{tenants: make(map[string]map[string]*TenantUsage)}
}

// Add folds one usage event into the roll-up.
func (a *Aggregator) Add(u Usage) {
	a.mu.Lock()
	defer a.mu.Unlock()
	day := DayKey(u.RecordedAt)
	byTenant, ok := a.tenants[day]
	if !ok {
		byTenant = make(map[string]*TenantUsage)
		a.tenants[day] = byTenant
	}
	t, ok := byTenant[u.TenantID]
	if !ok {
		t = &TenantUsage{}
		byTenant[u.TenantID] = t
	}
	t.Requests++
	t.PromptTokens += u.PromptTokens
	t.CompletionTokens += u.CompletionTokens
}

// Snapshot returns a copy of one day's per-tenant totals.
func (a *Aggregator) Snapshot(day string) map[string]TenantUsage {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]TenantUsage, len(a.tenants[day]))
	for tenant, t := range a.tenants[day] {
		out[tenant] = *t
	}
	return out
}

// Prune drops days older than keep, bounding memory.
func (a *Aggregator) Prune(now time.Time, keep time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for day := range a.tenants {
		if dayStart, err := time.Parse("2006-01-02", day); err == nil {
			if now.Sub(dayStart) > keep {
				delete(a.tenants, day)
			}
		}
	}
}
