// Command gateway runs the inference-guard API gateway: a multi-tenant,
// OpenAI-compatible entry point in front of a model inference server.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/nynrathod/inference-guard/internal/admission"
	"github.com/nynrathod/inference-guard/internal/auth"
	"github.com/nynrathod/inference-guard/internal/config"
	"github.com/nynrathod/inference-guard/internal/metering"
	"github.com/nynrathod/inference-guard/internal/metrics"
	"github.com/nynrathod/inference-guard/internal/proxy"
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

	// Token budgets: shared Redis when configured, in-process otherwise.
	var budgets metering.BudgetStore
	if cfg.RedisAddr != "" {
		redisStore := metering.NewRedisBudgetStore(cfg.RedisAddr, cfg.RedisPassword, cfg.BudgetWindow)
		pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := redisStore.Ping(pingCtx); err != nil {
			cancel()
			log.Error("cannot reach Redis (GATEWAY_REDIS_ADDR)", "addr", cfg.RedisAddr, "error", err)
			os.Exit(1)
		}
		cancel()
		budgets = redisStore
		log.Info("token budgets backed by Redis", "addr", cfg.RedisAddr, "window", cfg.BudgetWindow.String())
	} else {
		budgets = metering.NewMemoryBudgetStore(cfg.BudgetWindow)
		log.Info("token budgets in-process (set GATEWAY_REDIS_ADDR to share them across replicas)")
	}

	// Usage events: shipped to Kafka when brokers are configured.
	var sink metering.EventSink = metering.NoopSink{}
	if len(cfg.KafkaBrokers) > 0 {
		sink = metering.NewKafkaSink(cfg.KafkaBrokers, cfg.UsageTopic, log)
		log.Info("usage events shipping to Kafka", "brokers", cfg.KafkaBrokers, "topic", cfg.UsageTopic)
	} else {
		log.Info("usage events discarded (set GATEWAY_KAFKA_BROKERS to ship them)")
	}

	meter := metering.New(
		metering.Options{
			Window:        cfg.BudgetWindow,
			ReserveTokens: cfg.BudgetReserveTokens,
			Budgets:       cfg.TenantBudgets,
			DefaultBudget: cfg.DefaultTenantBudget,
		},
		budgets,
		sink,
		log,
	)

	m := metrics.New()

	srv := server.New(
		server.Config{
			Addr:              cfg.Addr,
			ReadHeaderTimeout: cfg.ReadHeaderTimeout,
			ShutdownTimeout:   cfg.ShutdownTimeout,
		},
		router.New(
			log,
			auth.New(cfg.APIKeys, log, m),
			admission.New(cfg.TenantConcurrency),
			m,
			meter,
			proxy.New(cfg.UpstreamURL, log).Handler(),
		),
		log,
	)

	runErr := srv.Run()
	if closeErr := meter.Close(); closeErr != nil {
		log.Warn("metering shutdown", "error", closeErr)
	}
	if runErr != nil {
		log.Error("gateway terminated", "error", runErr)
		os.Exit(1)
	}
}
