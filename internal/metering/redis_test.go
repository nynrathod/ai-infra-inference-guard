package metering

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestRedisStoreReserveCommit(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)

	restoreNow(t, time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC))
	s := NewRedisBudgetStore(mr.Addr(), "", time.Hour)
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	if err := s.Reserve(ctx, "tenant-a", 10, 100); err != nil {
		t.Fatal(err)
	}
	if err := s.Reserve(ctx, "tenant-a", 10, 100); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, "tenant-a", 10, 4); err != nil {
		t.Fatal(err)
	}
	if err := s.Reserve(ctx, "tenant-a", 1000, 100); err != ErrBudgetExhausted {
		t.Fatalf("err = %v, want ErrBudgetExhausted", err)
	}
	// limit 0 = unlimited, but usage is still recorded
	if err := s.Reserve(ctx, "tenant-b", 5000, 0); err != nil {
		t.Fatal(err)
	}
}

func TestRedisStoreWindowRollover(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)

	now := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	restoreNow(t, now)
	s := NewRedisBudgetStore(mr.Addr(), "", time.Hour)
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Reserve(context.Background(), "tenant-a", 50, 0); err != nil {
		t.Fatal(err)
	}
	// next hour bucket: a fresh key, budget resets
	restoreNow(t, now.Add(2*time.Hour))
	if err := s.Reserve(context.Background(), "tenant-a", 50, 50); err != nil {
		t.Fatalf("reserve in the new window failed: %v", err)
	}
}
