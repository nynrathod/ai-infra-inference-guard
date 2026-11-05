package aimd

import (
	"sort"
	"sync"
	"time"
)

// TTFTWindow is a fixed-size sliding window of recent time-to-first-byte
// observations, used to derive a latency signal that is independent of the
// model server's self-reported metrics.
type TTFTWindow struct {
	mu    sync.Mutex
	buf   []float64
	head  int
	count int
}

// NewTTFTWindow returns a window retaining the size most recent observations.
func NewTTFTWindow(size int) *TTFTWindow {
	if size < 1 {
		size = 1
	}
	return &TTFTWindow{buf: make([]float64, size)}
}

// Observe records one time-to-first-byte duration.
func (w *TTFTWindow) Observe(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf[w.head] = d.Seconds()
	w.head = (w.head + 1) % len(w.buf)
	if w.count < len(w.buf) {
		w.count++
	}
}

// Percentile returns the p-quantile (0..1) of the retained observations using
// linear interpolation, mirroring Prometheus histogram_quantile semantics. It
// reports false when the window is empty.
func (w *TTFTWindow) Percentile(p float64) (float64, bool) {
	w.mu.Lock()
	if w.count == 0 {
		w.mu.Unlock()
		return 0, false
	}
	values := make([]float64, w.count)
	copy(values, w.buf[:w.count])
	w.mu.Unlock()

	sort.Float64s(values)
	if p <= 0 {
		return values[0], true
	}
	if p >= 1 {
		return values[len(values)-1], true
	}
	pos := p * float64(len(values)-1)
	lo := int(pos)
	hi := min(lo+1, len(values)-1)
	frac := pos - float64(lo)
	return values[lo] + (values[hi]-values[lo])*frac, true
}
