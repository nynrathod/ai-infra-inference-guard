package aimd

import (
        "sort"
        "sync"
        "time"
)

// TTFTWindow is a fixed-size sliding window of recent time-to-first-token
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

// Observe records one time-to-first-token duration.
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

// tenantWindows keeps one TTFT window per tenant so one tenant's latency
// cannot mask another's: the controller reacts to the worst-affected tenant,
// not to an average diluted across healthy tenants.
type tenantWindows struct {
        mu      sync.Mutex
        size    int
        windows map[string]*TTFTWindow
}

// newTenantWindows returns per-tenant windows each retaining size observations.
func newTenantWindows(size int) *tenantWindows {
        return &tenantWindows{size: size, windows: make(map[string]*TTFTWindow)}
}

// observe records one duration against the tenant's window.
func (t *tenantWindows) observe(tenant string, d time.Duration) {
        t.mu.Lock()
        w, ok := t.windows[tenant]
        if !ok {
                w = NewTTFTWindow(t.size)
                t.windows[tenant] = w
        }
        t.mu.Unlock()
        w.Observe(d)
}

// percentile reports the p-quantile for one tenant; false when it has no
// observations.
func (t *tenantWindows) percentile(tenant string, p float64) (float64, bool) {
        t.mu.Lock()
        w, ok := t.windows[tenant]
        t.mu.Unlock()
        if !ok {
                return 0, false
        }
        return w.Percentile(p)
}

// worstPercentile reports the highest p-quantile across all tenants together
// with the tenant that produced it; false when no tenant has observations.
func (t *tenantWindows) worstPercentile(p float64) (value float64, tenant string, ok bool) {
        t.mu.Lock()
        names := make([]string, 0, len(t.windows))
        for name := range t.windows {
                names = append(names, name)
        }
        t.mu.Unlock()
        for _, name := range names {
                v, has := t.percentile(name, p)
                if !has {
                        continue
                }
                if !ok || v > value {
                        value, tenant, ok = v, name, true
                }
        }
        return value, tenant, ok
}
