package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func unsetEnv(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		old, had := os.LookupEnv(key)
		os.Unsetenv(key)
		t.Cleanup(func() {
			if had {
				os.Setenv(key, old)
			} else {
				os.Unsetenv(key)
			}
		})
	}
}

var meteringEnvKeys = []string{
	"GATEWAY_REDIS_ADDR", "GATEWAY_REDIS_PASSWORD", "GATEWAY_BUDGET_WINDOW",
	"GATEWAY_BUDGET_RESERVE_TOKENS", "GATEWAY_TENANT_BUDGETS",
	"GATEWAY_DEFAULT_TENANT_BUDGET", "GATEWAY_KAFKA_BROKERS", "GATEWAY_USAGE_TOPIC",
}

func TestLoadValid(t *testing.T) {
	t.Setenv("GATEWAY_UPSTREAM", "http://localhost:8000")
	t.Setenv("GATEWAY_API_KEYS", "sk-a=tenant-a, sk-b=tenant-b")
	t.Setenv("GATEWAY_TENANT_CONCURRENCY", "16")
	t.Setenv("GATEWAY_ADDR", ":9090")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Addr != ":9090" {
		t.Errorf("Addr = %q, want :9090", cfg.Addr)
	}
	if cfg.UpstreamURL.String() != "http://localhost:8000" {
		t.Errorf("UpstreamURL = %q, want http://localhost:8000", cfg.UpstreamURL.String())
	}
	if cfg.TenantConcurrency != 16 {
		t.Errorf("TenantConcurrency = %d, want 16", cfg.TenantConcurrency)
	}

	want := map[string]string{"sk-a": "tenant-a", "sk-b": "tenant-b"}
	if len(cfg.APIKeys) != len(want) {
		t.Fatalf("len(APIKeys) = %d, want %d", len(cfg.APIKeys), len(want))
	}
	for key, tenant := range want {
		if cfg.APIKeys[key] != tenant {
			t.Errorf("APIKeys[%q] = %q, want %q", key, cfg.APIKeys[key], tenant)
		}
	}
}

func TestLoadDefaults(t *testing.T) {
	unsetEnv(t, append([]string{
		"GATEWAY_ADDR", "GATEWAY_TENANT_CONCURRENCY",
		"GATEWAY_READ_HEADER_TIMEOUT", "GATEWAY_SHUTDOWN_TIMEOUT",
	}, meteringEnvKeys...)...)
	t.Setenv("GATEWAY_UPSTREAM", "http://localhost:8000")
	t.Setenv("GATEWAY_API_KEYS", "sk-a=tenant-a")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Addr != ":8080" {
		t.Errorf("Addr = %q, want :8080", cfg.Addr)
	}
	if cfg.TenantConcurrency != 8 {
		t.Errorf("TenantConcurrency = %d, want 8", cfg.TenantConcurrency)
	}
	if cfg.ReadHeaderTimeout != 10*time.Second {
		t.Errorf("ReadHeaderTimeout = %v, want 10s", cfg.ReadHeaderTimeout)
	}
	if cfg.ShutdownTimeout != 15*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 15s", cfg.ShutdownTimeout)
	}
}

func TestLoadMeteringDefaults(t *testing.T) {
	unsetEnv(t, meteringEnvKeys...)
	t.Setenv("GATEWAY_UPSTREAM", "http://localhost:8000")
	t.Setenv("GATEWAY_API_KEYS", "sk-a=tenant-a")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.RedisAddr != "" {
		t.Errorf("RedisAddr = %q, want empty", cfg.RedisAddr)
	}
	if len(cfg.KafkaBrokers) != 0 {
		t.Errorf("KafkaBrokers = %v, want empty", cfg.KafkaBrokers)
	}
	if cfg.BudgetWindow != 24*time.Hour {
		t.Errorf("BudgetWindow = %v, want 24h", cfg.BudgetWindow)
	}
	if cfg.BudgetReserveTokens != 1024 {
		t.Errorf("BudgetReserveTokens = %d, want 1024", cfg.BudgetReserveTokens)
	}
	if cfg.TenantBudgets != nil {
		t.Errorf("TenantBudgets = %v, want nil", cfg.TenantBudgets)
	}
	if cfg.DefaultTenantBudget != 0 {
		t.Errorf("DefaultTenantBudget = %d, want 0", cfg.DefaultTenantBudget)
	}
	if cfg.UsageTopic != "inference-usage" {
		t.Errorf("UsageTopic = %q, want inference-usage", cfg.UsageTopic)
	}
}

func TestLoadMeteringConfig(t *testing.T) {
	unsetEnv(t, meteringEnvKeys...)
	t.Setenv("GATEWAY_UPSTREAM", "http://localhost:8000")
	t.Setenv("GATEWAY_API_KEYS", "sk-a=tenant-a")
	t.Setenv("GATEWAY_REDIS_ADDR", "redis:6379")
	t.Setenv("GATEWAY_KAFKA_BROKERS", "kafka-0:9092, kafka-1:9092")
	t.Setenv("GATEWAY_TENANT_BUDGETS", "tenant-a:100000, tenant-b:250000")
	t.Setenv("GATEWAY_BUDGET_WINDOW", "1h")
	t.Setenv("GATEWAY_BUDGET_RESERVE_TOKENS", "512")
	t.Setenv("GATEWAY_DEFAULT_TENANT_BUDGET", "50000")
	t.Setenv("GATEWAY_USAGE_TOPIC", "usage-events")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.RedisAddr != "redis:6379" {
		t.Errorf("RedisAddr = %q", cfg.RedisAddr)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[0] != "kafka-0:9092" || cfg.KafkaBrokers[1] != "kafka-1:9092" {
		t.Errorf("KafkaBrokers = %v", cfg.KafkaBrokers)
	}
	if cfg.TenantBudgets["tenant-a"] != 100000 || cfg.TenantBudgets["tenant-b"] != 250000 {
		t.Errorf("TenantBudgets = %v", cfg.TenantBudgets)
	}
	if cfg.BudgetWindow != time.Hour {
		t.Errorf("BudgetWindow = %v, want 1h", cfg.BudgetWindow)
	}
	if cfg.BudgetReserveTokens != 512 {
		t.Errorf("BudgetReserveTokens = %d, want 512", cfg.BudgetReserveTokens)
	}
	if cfg.DefaultTenantBudget != 50000 {
		t.Errorf("DefaultTenantBudget = %d, want 50000", cfg.DefaultTenantBudget)
	}
	if cfg.UsageTopic != "usage-events" {
		t.Errorf("UsageTopic = %q", cfg.UsageTopic)
	}
}

func TestLoadInvalidTenantBudgets(t *testing.T) {
	unsetEnv(t, meteringEnvKeys...)
	t.Setenv("GATEWAY_UPSTREAM", "http://localhost:8000")
	t.Setenv("GATEWAY_API_KEYS", "sk-a=tenant-a")
	t.Setenv("GATEWAY_TENANT_BUDGETS", "tenant-a:not-a-number")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "GATEWAY_TENANT_BUDGETS") {
		t.Fatalf("err = %v, want GATEWAY_TENANT_BUDGETS error", err)
	}
}

func TestLoadInvalidBudgetWindow(t *testing.T) {
	unsetEnv(t, meteringEnvKeys...)
	t.Setenv("GATEWAY_UPSTREAM", "http://localhost:8000")
	t.Setenv("GATEWAY_API_KEYS", "sk-a=tenant-a")
	t.Setenv("GATEWAY_BUDGET_WINDOW", "nonsense")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "GATEWAY_BUDGET_WINDOW") {
		t.Fatalf("err = %v, want GATEWAY_BUDGET_WINDOW error", err)
	}
}

func TestLoadMissingRequired(t *testing.T) {
	unsetEnv(t, append([]string{"GATEWAY_UPSTREAM", "GATEWAY_API_KEYS"}, meteringEnvKeys...)...)

	_, err := Load()
	if err == nil {
		t.Fatal("Load() = nil error, want failure")
	}
	for _, want := range []string{"GATEWAY_UPSTREAM", "GATEWAY_API_KEYS"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err.Error(), want)
		}
	}
}

func TestLoadInvalidUpstream(t *testing.T) {
	t.Setenv("GATEWAY_UPSTREAM", "not-a-url")
	t.Setenv("GATEWAY_API_KEYS", "sk-a=tenant-a")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "GATEWAY_UPSTREAM") {
		t.Fatalf("err = %v, want GATEWAY_UPSTREAM error", err)
	}
}

func TestLoadInvalidAPIKeys(t *testing.T) {
	t.Setenv("GATEWAY_UPSTREAM", "http://localhost:8000")
	t.Setenv("GATEWAY_API_KEYS", "tenant-a-without-key")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "key=tenant") {
		t.Fatalf("err = %v, want key=tenant error", err)
	}
}

func TestLoadInvalidConcurrency(t *testing.T) {
	t.Setenv("GATEWAY_UPSTREAM", "http://localhost:8000")
	t.Setenv("GATEWAY_API_KEYS", "sk-a=tenant-a")
	t.Setenv("GATEWAY_TENANT_CONCURRENCY", "0")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "GATEWAY_TENANT_CONCURRENCY") {
		t.Fatalf("err = %v, want GATEWAY_TENANT_CONCURRENCY error", err)
	}
}

func TestLoadRateLimitAndEstimator(t *testing.T) {
	t.Setenv("GATEWAY_UPSTREAM", "http://localhost:8000")
	t.Setenv("GATEWAY_API_KEYS", "sk-a=tenant-a")
	t.Setenv("GATEWAY_RATE_LIMIT_RPS", "5.5")
	t.Setenv("GATEWAY_RATE_LIMIT_BURST", "10")
	t.Setenv("GATEWAY_TOKEN_ESTIMATOR", "Chars4")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.RateLimitRPS != 5.5 {
		t.Errorf("RateLimitRPS = %v, want 5.5", cfg.RateLimitRPS)
	}
	if cfg.RateLimitBurst != 10 {
		t.Errorf("RateLimitBurst = %d, want 10", cfg.RateLimitBurst)
	}
	if cfg.TokenEstimator != "chars4" {
		t.Errorf("TokenEstimator = %q, want normalized chars4", cfg.TokenEstimator)
	}
}

func TestLoadRejectsUnknownEstimator(t *testing.T) {
	t.Setenv("GATEWAY_UPSTREAM", "http://localhost:8000")
	t.Setenv("GATEWAY_API_KEYS", "sk-a=tenant-a")
	t.Setenv("GATEWAY_TOKEN_ESTIMATOR", "wild-guess")

	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted an unknown token estimator")
	}
}

func TestLoadRejectsNegativeRateLimit(t *testing.T) {
	t.Setenv("GATEWAY_UPSTREAM", "http://localhost:8000")
	t.Setenv("GATEWAY_API_KEYS", "sk-a=tenant-a")
	t.Setenv("GATEWAY_RATE_LIMIT_RPS", "-1")

	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted a negative rate limit")
	}
}
