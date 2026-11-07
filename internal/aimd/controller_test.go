package aimd

import (
        "context"
        "errors"
        "io"
        "log/slog"
        "testing"
        "time"

        "github.com/prometheus/client_golang/prometheus/testutil"
)

type fakeSource struct {
        sig Signals
        err error
}

func (f *fakeSource) Scrape(context.Context) (Signals, error) {
        return f.sig, f.err
}

type fakeLimiter struct {
        snapshot map[string]int
        limits   map[string]int
}

func (f *fakeLimiter) SetLimit(tenant string, limit int) {
        if f.limits == nil {
                f.limits = make(map[string]int)
        }
        f.limits[tenant] = limit
}

func (f *fakeLimiter) Snapshot() map[string]int {
        out := make(map[string]int, len(f.snapshot))
        for tenant, n := range f.snapshot {
                out[tenant] = n
        }
        return out
}

func testConfig() Config {
        return Config{
                BaseLimit:      8,
                MinLimit:       2,
                DecreaseFactor: 0.5,
                AdditiveStep:   1,
                RecoveryTicks:  2,
                QueueDepthHigh: 12,
                KVCacheHigh:    0.9,
                TTFTHigh:       2500 * time.Millisecond,
                DominantShare:  0.6,
                Interval:       time.Hour,
                StaleAfter:     2,
        }
}

func newTestController(cfg Config, src SignalSource, lim *fakeLimiter) *Controller {
        log := slog.New(slog.NewTextHandler(io.Discard, nil))
        return New(cfg, src, lim, nil, log)
}

func saturatedSignals() Signals {
        return Signals{QueueDepth: 20, HasQueueDepth: true, KVCacheUsage: 0.4, HasKVCache: true}
}

func healthySignals() Signals {
        return Signals{QueueDepth: 0, HasQueueDepth: true, KVCacheUsage: 0.3, HasKVCache: true}
}

func TestDecreaseOnSaturation(t *testing.T) {
        cfg := testConfig()
        lim := &fakeLimiter{snapshot: map[string]int{"tenant-a": 6, "tenant-b": 2}}
        c := newTestController(cfg, &fakeSource{sig: saturatedSignals()}, lim)

        c.tick(context.Background())

        // Both tenants are cut multiplicatively (8 -> 4). tenant-a holds 6/8 of
        // in-flight work, above the dominant share, and is cut one more round.
        if got := lim.limits["tenant-b"]; got != 4 {
                t.Errorf("tenant-b limit = %d, want 4", got)
        }
        if got := lim.limits["tenant-a"]; got != 2 {
                t.Errorf("tenant-a limit = %d, want 2", got)
        }
}

func TestFloorNeverBreached(t *testing.T) {
        cfg := testConfig()
        lim := &fakeLimiter{snapshot: map[string]int{"tenant-a": 1}}
        c := newTestController(cfg, &fakeSource{sig: saturatedSignals()}, lim)

        for i := 0; i < 6; i++ {
                c.tick(context.Background())
        }

        if got := lim.limits["tenant-a"]; got != cfg.MinLimit {
                t.Errorf("limit = %d, want floor %d", got, cfg.MinLimit)
        }
}

func TestAdditiveRecovery(t *testing.T) {
        cfg := testConfig()
        lim := &fakeLimiter{snapshot: map[string]int{"tenant-a": 1, "tenant-b": 1}}
        src := &fakeSource{}
        c := newTestController(cfg, src, lim)

        src.sig = saturatedSignals()
        c.tick(context.Background())
        if got := lim.limits["tenant-a"]; got != 4 {
                t.Fatalf("after decrease = %d, want 4", got)
        }

        src.sig = healthySignals()
        c.tick(context.Background()) // healthy tick 1: below recovery threshold
        if got := lim.limits["tenant-a"]; got != 4 {
                t.Fatalf("limit grew before %d healthy ticks: %d", cfg.RecoveryTicks, got)
        }

        c.tick(context.Background()) // healthy tick 2: +1
        if got := lim.limits["tenant-a"]; got != 5 {
                t.Fatalf("after first recovery tick = %d, want 5", got)
        }

        for i := 0; i < 10; i++ {
                c.tick(context.Background())
        }
        if got := lim.limits["tenant-a"]; got != cfg.BaseLimit {
                t.Fatalf("tenant-a after recovery = %d, want %d", got, cfg.BaseLimit)
        }
        if got := lim.limits["tenant-b"]; got != cfg.BaseLimit {
                t.Fatalf("tenant-b after recovery = %d, want %d", got, cfg.BaseLimit)
        }
}

func TestSaturationResetsRecovery(t *testing.T) {
        cfg := testConfig()
        lim := &fakeLimiter{snapshot: map[string]int{"tenant-a": 1}}
        src := &fakeSource{}
        c := newTestController(cfg, src, lim)

        src.sig = saturatedSignals()
        c.tick(context.Background()) // 8 -> 4

        src.sig = healthySignals()
        c.tick(context.Background()) // healthy tick 1

        src.sig = saturatedSignals()
        c.tick(context.Background()) // recovery counter reset, still 4

        src.sig = healthySignals()
        c.tick(context.Background()) // healthy tick 1 again: no growth yet
        if got := lim.limits["tenant-a"]; got != 4 {
                t.Fatalf("limit grew after interrupted recovery: %d", got)
        }
        c.tick(context.Background()) // healthy tick 2: growth resumes
        if got := lim.limits["tenant-a"]; got != 5 {
                t.Fatalf("recovery did not resume: %d", got)
        }
}

func TestHoldsLimitsWhenSignalsUnavailable(t *testing.T) {
        cfg := testConfig()
        lim := &fakeLimiter{snapshot: map[string]int{"tenant-a": 1}}
        c := newTestController(cfg, &fakeSource{err: errors.New("connection refused")}, lim)

        for i := 0; i < 5; i++ {
                c.tick(context.Background())
        }

        if len(lim.limits) != 0 {
                t.Fatalf("limits changed while signals unavailable: %v", lim.limits)
        }
        if got := testutil.ToFloat64(c.metrics.stale); got != 1 {
                t.Errorf("stale = %v, want 1", got)
        }
}

func TestPerTenantTTFTIsolation(t *testing.T) {
        cfg := testConfig()
        lim := &fakeLimiter{snapshot: map[string]int{"tenant-a": 2, "tenant-b": 2}}
        // Model server signals look healthy, but tenant-a's latency is far over
        // the threshold while tenant-b is fast: the controller must still react.
        c := newTestController(cfg, &fakeSource{sig: healthySignals()}, lim)

        for i := 0; i < 10; i++ {
                c.ObserveTTFT("tenant-a", 4*time.Second)
                c.ObserveTTFT("tenant-b", 10*time.Millisecond)
        }

        c.tick(context.Background())

        if got := lim.limits["tenant-a"]; got != 4 {
                t.Errorf("tenant-a limit = %d, want 4 (cut on worst-tenant TTFT)", got)
        }
        if got := lim.limits["tenant-b"]; got != 4 {
                t.Errorf("tenant-b limit = %d, want 4 (also cut: saturation is global)", got)
        }
}

func TestHealthyTenantsDoNotTriggerSaturation(t *testing.T) {
        cfg := testConfig()
        lim := &fakeLimiter{snapshot: map[string]int{"tenant-a": 2}}
        c := newTestController(cfg, &fakeSource{sig: healthySignals()}, lim)

        for i := 0; i < 10; i++ {
                c.ObserveTTFT("tenant-a", 10*time.Millisecond)
        }

        c.tick(context.Background())

        if len(lim.limits) != 0 {
                t.Errorf("limits changed without saturation: %v", lim.limits)
        }
}

func TestEvaluate(t *testing.T) {
        cfg := testConfig()
        cases := []struct {
                name    string
                sig     Signals
                ttftP95 float64
                hasTTFT bool
                wantSat bool
                wantWhy string
        }{
                {
                        name:    "queue depth over threshold",
                        sig:     Signals{QueueDepth: cfg.QueueDepthHigh, HasQueueDepth: true},
                        wantSat: true,
                        wantWhy: "queue_depth",
                },
                {
                        name:    "kv cache over threshold",
                        sig:     Signals{KVCacheUsage: cfg.KVCacheHigh, HasKVCache: true},
                        wantSat: true,
                        wantWhy: "kv_cache",
                },
                {
                        name:    "ttft p95 over threshold",
                        sig:     Signals{QueueDepth: 0, HasQueueDepth: true, KVCacheUsage: 0.1, HasKVCache: true},
                        ttftP95: cfg.TTFTHigh.Seconds(),
                        hasTTFT: true,
                        wantSat: true,
                        wantWhy: "ttft_p95",
                },
                {
                        name:    "healthy",
                        sig:     Signals{QueueDepth: 1, HasQueueDepth: true, KVCacheUsage: 0.2, HasKVCache: true},
                        ttftP95: 0.1,
                        hasTTFT: true,
                },
                {
                        name:    "unexposed signals are ignored, ttft still catches",
                        sig:     Signals{},
                        ttftP95: cfg.TTFTHigh.Seconds() * 2,
                        hasTTFT: true,
                        wantSat: true,
                        wantWhy: "ttft_p95",
                },
        }

        for _, tc := range cases {
                t.Run(tc.name, func(t *testing.T) {
                        sat, why := evaluate(tc.sig, tc.ttftP95, tc.hasTTFT, cfg)
                        if sat != tc.wantSat || why != tc.wantWhy {
                                t.Fatalf("evaluate() = (%v, %q), want (%v, %q)",
                                        sat, why, tc.wantSat, tc.wantWhy)
                        }
                })
        }
}
