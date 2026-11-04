package metering

import (
	"testing"
	"time"
)

func TestAggregatorAddSnapshot(t *testing.T) {
	a := NewAggregator()
	base := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

	a.Add(Usage{TenantID: "a", RecordedAt: base, PromptTokens: 10, CompletionTokens: 5})
	a.Add(Usage{TenantID: "a", RecordedAt: base.Add(time.Second), PromptTokens: 1, CompletionTokens: 1})
	a.Add(Usage{TenantID: "b", RecordedAt: base, PromptTokens: 3, CompletionTokens: 0})
	a.Add(Usage{TenantID: "a", RecordedAt: base.Add(48 * time.Hour)}) // another day

	snap := a.Snapshot(DayKey(base))
	if len(snap) != 2 {
		t.Fatalf("snapshot has %d tenants, want 2", len(snap))
	}
	if snap["a"].PromptTokens != 11 || snap["a"].CompletionTokens != 6 || snap["a"].Requests != 2 {
		t.Errorf("tenant a = %+v", snap["a"])
	}
	if snap["b"].Requests != 1 || snap["b"].PromptTokens != 3 {
		t.Errorf("tenant b = %+v", snap["b"])
	}
	if n := len(a.Snapshot(DayKey(base.Add(48 * time.Hour)))); n != 1 {
		t.Errorf("next-day snapshot has %d tenants, want 1", n)
	}
}

func TestAggregatorPrune(t *testing.T) {
	a := NewAggregator()
	old := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	recent := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
	a.Add(Usage{TenantID: "a", RecordedAt: old})
	a.Add(Usage{TenantID: "a", RecordedAt: recent})

	a.Prune(time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC), 48*time.Hour)

	if n := len(a.Snapshot(DayKey(old))); n != 0 {
		t.Error("old day was not pruned")
	}
	if n := len(a.Snapshot(DayKey(recent))); n != 1 {
		t.Error("recent day was pruned")
	}
}
