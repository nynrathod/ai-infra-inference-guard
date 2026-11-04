package metering

import (
	"context"
	"sync"
	"time"
)

// nowFunc is swappable so tests can roll budget windows deterministically.
var nowFunc = time.Now

// windowBucket returns the window bucket index a point in time falls into.
func windowBucket(now time.Time, window time.Duration) int64 {
	if window <= 0 {
		window = 24 * time.Hour
	}
	return now.Unix() / int64(window.Seconds())
}

// MemoryBudgetStore is the in-process BudgetStore used when no Redis is
// configured: budgets are enforced per gateway replica. Windows roll over
// lazily on first access after the boundary.
type MemoryBudgetStore struct {
	mu      sync.Mutex
	window  time.Duration
	entries map[string]*memoryEntry
}

type memoryEntry struct {
	bucket int64
	used   int64
}

// NewMemoryBudgetStore returns an in-memory store with the given window.
func NewMemoryBudgetStore(window time.Duration) *MemoryBudgetStore {
	if window <= 0 {
		window = 24 * time.Hour
	}
	return &MemoryBudgetStore{window: window, entries: make(map[string]*memoryEntry)}
}

// Reserve charges tokens to the tenant's current window atomically.
func (s *MemoryBudgetStore) Reserve(ctx context.Context, tenant string, tokens, limit int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.entryLocked(tenant)
	if limit > 0 && e.used+tokens > limit {
		return ErrBudgetExhausted
	}
	e.used += tokens
	return nil
}

// Commit adjusts the tenant's current window by (actual - reserved).
func (s *MemoryBudgetStore) Commit(ctx context.Context, tenant string, reserved, actual int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.entryLocked(tenant)
	e.used += actual - reserved
	if e.used < 0 {
		e.used = 0
	}
	return nil
}

// Close is a no-op; MemoryBudgetStore holds no resources.
func (s *MemoryBudgetStore) Close() error { return nil }

// used reports the tokens charged in the tenant's current window (tests).
func (s *MemoryBudgetStore) used(tenant string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entryLocked(tenant).used
}

func (s *MemoryBudgetStore) entryLocked(tenant string) *memoryEntry {
	bucket := windowBucket(nowFunc(), s.window)
	e, ok := s.entries[tenant]
	if !ok || e.bucket != bucket {
		e = &memoryEntry{bucket: bucket}
		s.entries[tenant] = e
	}
	return e
}
