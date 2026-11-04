package metering

import (
	"context"
	"testing"
	"time"
)

// restoreNow pins the package clock; used across the package's tests.
func restoreNow(t *testing.T, now time.Time) {
	t.Helper()
	prev := nowFunc
	nowFunc = func() time.Time { return now }
	t.Cleanup(func() { nowFunc = prev })
}

func TestMemoryStoreReserveCommit(t *testing.T) {
	restoreNow(t, time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC))
	s := NewMemoryBudgetStore(time.Hour)
	ctx := context.Background()

	if err := s.Reserve(ctx, "tenant-a", 10, 100); err != nil {
		t.Fatal(err)
	}
	if err := s.Reserve(ctx, "tenant-a", 10, 100); err != nil {
		t.Fatal(err)
	}
	if got := s.used("tenant-a"); got != 20 {
		t.Fatalf("used = %d, want 20", got)
	}
	if err := s.Commit(ctx, "tenant-a", 10, 4); err != nil {
		t.Fatal(err)
	}
	if got := s.used("tenant-a"); got != 14 {
		t.Fatalf("after commit used = %d, want 14", got)
	}
	if err := s.Reserve(ctx, "tenant-a", 1000, 100); err != ErrBudgetExhausted {
		t.Fatalf("err = %v, want ErrBudgetExhausted", err)
	}
	// limit 0 = unlimited, but usage is still recorded
	if err := s.Reserve(ctx, "tenant-b", 5000, 0); err != nil {
		t.Fatal(err)
	}
	if got := s.used("tenant-b"); got != 5000 {
		t.Fatalf("tenant-b used = %d, want 5000", got)
	}
}

func TestMemoryStoreWindowRollover(t *testing.T) {
	now := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	restoreNow(t, now)
	s := NewMemoryBudgetStore(time.Hour)

	if err := s.Reserve(context.Background(), "tenant-a", 50, 0); err != nil {
		t.Fatal(err)
	}
	restoreNow(t, now.Add(2*time.Hour)) // next window: budget resets
	if got := s.used("tenant-a"); got != 0 {
		t.Fatalf("used after rollover = %d, want 0", got)
	}
}
