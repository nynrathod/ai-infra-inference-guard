// Command usage-consumer reads usage events from the inference-usage Kafka
// topic, persists per-tenant token accounting in Redis and logs a periodic
// per-tenant roll-up.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/nynrathod/inference-guard/internal/metering"
)

type config struct {
	Brokers        []string
	Topic          string
	GroupID        string
	RedisAddr      string
	RedisPassword  string
	ReportInterval time.Duration
}

func loadConfig() (config, error) {
	var errs []string
	var cfg config

	brokersRaw := strings.TrimSpace(os.Getenv("BROKERS"))
	if brokersRaw == "" {
		errs = append(errs, "BROKERS is required (e.g. usage-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092)")
	}
	for _, b := range strings.Split(brokersRaw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			cfg.Brokers = append(cfg.Brokers, b)
		}
	}

	cfg.Topic = envOr("TOPIC", "inference-usage")
	cfg.GroupID = envOr("GROUP_ID", "usage-consumer")
	cfg.RedisAddr = strings.TrimSpace(os.Getenv("REDIS_ADDR"))
	if cfg.RedisAddr == "" {
		errs = append(errs, "REDIS_ADDR is required (e.g. redis:6379)")
	}
	cfg.RedisPassword = os.Getenv("REDIS_PASSWORD")

	interval, err := time.ParseDuration(envOr("REPORT_INTERVAL", "60s"))
	if err != nil || interval <= 0 {
		errs = append(errs, "REPORT_INTERVAL must be a positive duration (e.g. 60s)")
	}
	cfg.ReportInterval = interval

	if len(errs) > 0 {
		return cfg, fmt.Errorf("invalid configuration: %s", strings.Join(errs, "; "))
	}
	return cfg, nil
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	cfg, err := loadConfig()
	if err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword})
	defer func() { _ = rdb.Close() }()
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		cancel()
		log.Error("cannot reach Redis", "addr", cfg.RedisAddr, "error", err)
		os.Exit(1)
	}
	cancel()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		GroupID:  cfg.GroupID,
		Topic:    cfg.Topic,
		MinBytes: 1,
		MaxBytes: 1 << 20,
		MaxWait:  500 * time.Millisecond,
	})
	defer func() { _ = reader.Close() }()

	agg := metering.NewAggregator()
	go reportLoop(ctx, log, agg, cfg.ReportInterval)

	log.Info("usage consumer started",
		"brokers", cfg.Brokers, "topic", cfg.Topic, "group", cfg.GroupID, "redis", cfg.RedisAddr)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break // shutdown
			}
			log.Warn("fetch failed", "error", err)
			time.Sleep(time.Second)
			continue
		}

		var u metering.Usage
		if err := json.Unmarshal(msg.Value, &u); err != nil {
			log.Warn("discarding malformed usage event", "error", err)
		} else if err := persist(ctx, rdb, u); err != nil {
			log.Warn("redis write failed", "tenant", u.TenantID, "error", err)
		} else {
			agg.Add(u)
		}

		if err := reader.CommitMessages(ctx, msg); err != nil && ctx.Err() == nil {
			log.Warn("commit failed", "error", err)
		}
	}

	log.Info("usage consumer stopped")
}

// persist records one event's counters under usage:<tenant>:<day>:<field>,
// expiring after 30 days.
func persist(ctx context.Context, rdb *redis.Client, u metering.Usage) error {
	day := metering.DayKey(u.RecordedAt)
	prompt := fmt.Sprintf("usage:%s:%s:prompt", u.TenantID, day)
	completion := fmt.Sprintf("usage:%s:%s:completion", u.TenantID, day)
	requests := fmt.Sprintf("usage:%s:%s:requests", u.TenantID, day)
	ttl := 30 * 24 * time.Hour

	pipe := rdb.Pipeline()
	pipe.IncrBy(ctx, prompt, u.PromptTokens)
	pipe.IncrBy(ctx, completion, u.CompletionTokens)
	pipe.IncrBy(ctx, requests, 1)
	pipe.Expire(ctx, prompt, ttl)
	pipe.Expire(ctx, completion, ttl)
	pipe.Expire(ctx, requests, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// reportLoop logs a per-tenant roll-up of the current day.
func reportLoop(ctx context.Context, log *slog.Logger, agg *metering.Aggregator, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			day := metering.DayKey(time.Now())
			snapshot := agg.Snapshot(day)
			if len(snapshot) == 0 {
				continue
			}
			tenants := make([]string, 0, len(snapshot))
			for tenant := range snapshot {
				tenants = append(tenants, tenant)
			}
			sort.Slice(tenants, func(i, j int) bool {
				return snapshot[tenants[i]].CompletionTokens > snapshot[tenants[j]].CompletionTokens
			})
			log.Info("usage roll-up", "day", day, "tenants", tenants, "totals", snapshot)
		}
	}
}
