package aimd

import (
	"math"
	"testing"
	"time"
)

func TestTTFTWindowEmpty(t *testing.T) {
	w := NewTTFTWindow(4)
	if _, ok := w.Percentile(0.95); ok {
		t.Fatal("empty window reported a percentile")
	}
}

func TestTTFTWindowPercentile(t *testing.T) {
	w := NewTTFTWindow(4)
	// The window retains only the 4 most recent observations.
	for _, ms := range []int64{100, 200, 300, 400, 500} {
		w.Observe(time.Duration(ms) * time.Millisecond)
	}

	assertNear := func(p, want float64) {
		t.Helper()
		got, ok := w.Percentile(p)
		if !ok {
			t.Fatalf("Percentile(%v) reported empty", p)
		}
		if math.Abs(got-want) > 1e-9 {
			t.Errorf("Percentile(%v) = %v, want %v", p, got, want)
		}
	}

	assertNear(0.0, 0.2) // retained values: 200, 300, 400, 500 ms
	assertNear(0.5, 0.35)
	assertNear(1.0, 0.5)
	assertNear(0.95, 0.485)
}
