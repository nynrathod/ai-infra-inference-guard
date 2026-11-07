// Command gateway runs the inference-guard API gateway: a multi-tenant,
// OpenAI-compatible entry point in front of a model inference server.
package main

import (
        "context"
        "log/slog"
        "os"
        "os/signal"
        "syscall"
        "time"

        "github.com/nynrathod/inference-guard/internal/admission"
        "github.com/nynrathod/inference-guard/internal/aimd"
        "github.com/nynrathod/inference-guard/internal/auth"
        "github.com/nynrathod/inference-guard/internal/config"
        "github.com/nynrathod/inference-guard/internal/metering"
        "github.com/nynrathod/inference-guard/internal/metrics"
        "github.com/nynrathod/inference-guard/internal/proxy"
        "github.com/nynrathod/inference-guard/internal/ratelimit"
        "github.com/nynrathod/inference-guard/internal/router"
        "github.com/nynrathod/inference-guard/internal/server"
)

func main() {
        log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
        slog.SetDefault(log)

        cfg, err := config.Load()
        if err != nil {
                log.Error("invalid configuration", "error", err)
                os.Exit(1)
        }

        m := metrics.New()
        admissionCtl := admission.New(cfg.TenantConcurrency)

        var rateLimiter *ratelimit.Limiter
        if cfg.RateLimitRPS > 0 {
                rateLimiter = ratelimit.New(cfg.RateLimitRPS, cfg.RateLimitBurst)
                log.Info("per-tenant request rate limit enabled",
                        "rps", cfg.RateLimitRPS, "burst", cfg.RateLimitBurst)
        }

        meter := metering.New(metering.Options{
                Window:        cfg.BudgetWindow,
                ReserveTokens: cfg.BudgetReserveTokens,
                TokenEstimator: cfg.TokenEstimator,
                Budgets:       cfg.TenantBudgets,
                DefaultBudget: cfg.DefaultTenantBudget,
        }, budgetStore(cfg, log), usageSink(cfg, log), log)
        defer meter.Close()

        if cfg.AIMDEnabled {
                ctl := aimd.New(
                        aimd.Config{
                                BaseLimit:      cfg.TenantConcurrency,
                                MinLimit:       cfg.AIMDMinTenantConcurrency,
                                DecreaseFactor: cfg.AIMDDecreaseFactor,
                                AdditiveStep:   cfg.AIMDAdditiveStep,
                                RecoveryTicks:  cfg.AIMDRecoveryTicks,
                                QueueDepthHigh: cfg.AIMDQueueDepthHigh,
                                KVCacheHigh:    cfg.AIMDKVCacheHigh,
                                TTFTHigh:       cfg.AIMDTTFTHigh,
                                DominantShare:  cfg.AIMDDominantShare,
                                Interval:       cfg.AIMDInterval,
                        },
                        aimd.NewSignalScraper(cfg.VLLMMetricsURL, cfg.VLLMMetricsTimeout),
                        admissionCtl,
                        m.Registry(),
                        log,
                )
                m.SetTTFTObserver(ctl.ObserveTTFT)

                ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
                defer stop()
                go ctl.Run(ctx)
        }

        srv := server.New(
                server.Config{
                        Addr:              cfg.Addr,
                        ReadHeaderTimeout: cfg.ReadHeaderTimeout,
                        ShutdownTimeout:   cfg.ShutdownTimeout,
                },
                router.New(
                        log,
                        auth.New(cfg.APIKeys, log, m),
                        admissionCtl,
                        rateLimiter,
                        m,
                        meter,
                        proxy.New(cfg.UpstreamURL, log).Handler(),
                ),
                log,
        )

        if err := srv.Run(); err != nil {
                log.Error("gateway terminated", "error", err)
                os.Exit(1)
        }
}

// budgetStore returns the shared token budget store: Redis when an address is
// configured, so budgets hold across gateway replicas, otherwise an
// in-process store for single-replica deployments.
func budgetStore(cfg *config.Config, log *slog.Logger) metering.BudgetStore {
        if cfg.RedisAddr == "" {
                return metering.NewMemoryBudgetStore(cfg.BudgetWindow)
        }
        store := metering.NewRedisBudgetStore(cfg.RedisAddr, cfg.RedisPassword, cfg.BudgetWindow)
        ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
        defer cancel()
        if err := store.Ping(ctx); err != nil {
                log.Error("redis budget store unreachable", "addr", cfg.RedisAddr, "error", err)
                os.Exit(1)
        }
        return store
}

// usageSink returns the usage event sink: a Kafka producer when brokers are
// configured, otherwise nil, which makes metering discard events.
func usageSink(cfg *config.Config, log *slog.Logger) metering.EventSink {
        if len(cfg.KafkaBrokers) == 0 {
                return nil
        }
        return metering.NewKafkaSink(cfg.KafkaBrokers, cfg.UsageTopic, log)
}
