package config

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config is the complete runtime configuration of the gateway, resolved once
// at startup from the environment.
type Config struct {
	Addr              string
	UpstreamURL       *url.URL
	APIKeys           map[string]string
	TenantConcurrency int
	ReadHeaderTimeout time.Duration
	ShutdownTimeout   time.Duration

	// Metering (Phase 4). RedisAddr empty = in-process budgets;
	// KafkaBrokers empty = usage events are discarded (NoopSink).
	RedisAddr           string
	RedisPassword       string
	BudgetWindow        time.Duration
	BudgetReserveTokens int64
	TenantBudgets       map[string]int64
	DefaultTenantBudget int64
	KafkaBrokers        []string
	UsageTopic          string
}

const (
	envPrefix = "GATEWAY"

	keyAddr                = "addr"
	keyUpstream            = "upstream"
	keyAPIKeys             = "api_keys"
	keyTenantConcurrency   = "tenant_concurrency"
	keyReadHeaderTimeout   = "read_header_timeout"
	keyShutdownTimeout     = "shutdown_timeout"
	keyRedisAddr           = "redis_addr"
	keyRedisPassword       = "redis_password"
	keyBudgetWindow        = "budget_window"
	keyBudgetReserveTokens = "budget_reserve_tokens"
	keyTenantBudgets       = "tenant_budgets"
	keyDefaultBudget       = "default_tenant_budget"
	keyKafkaBrokers        = "kafka_brokers"
	keyUsageTopic          = "usage_topic"

	defaultAddr                = ":8080"
	defaultTenantConcurrency   = 8
	defaultReadHeaderTimeout   = 10 * time.Second
	defaultShutdownTimeout     = 15 * time.Second
	defaultBudgetWindow        = 24 * time.Hour
	defaultBudgetReserveTokens = 1024
	defaultUsageTopic          = "inference-usage"
)

// Load reads all GATEWAY_* environment variables and fails fast on any
// invalid or missing required value.
func Load() (*Config, error) {
	v := viper.New()
	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	v.SetDefault(keyAddr, defaultAddr)
	v.SetDefault(keyTenantConcurrency, defaultTenantConcurrency)
	v.SetDefault(keyReadHeaderTimeout, defaultReadHeaderTimeout)
	v.SetDefault(keyShutdownTimeout, defaultShutdownTimeout)
	v.SetDefault(keyBudgetWindow, defaultBudgetWindow)
	v.SetDefault(keyBudgetReserveTokens, defaultBudgetReserveTokens)
	v.SetDefault(keyUsageTopic, defaultUsageTopic)

	var errs []error

	concurrency := v.GetInt(keyTenantConcurrency)
	if concurrency < 1 {
		errs = append(errs, fmt.Errorf("%s must be >= 1", envName(keyTenantConcurrency)))
	}

	upstream, err := url.Parse(v.GetString(keyUpstream))
	if err != nil || upstream.Scheme == "" || upstream.Host == "" {
		errs = append(errs, fmt.Errorf("%s must be an absolute URL (e.g. http://localhost:8000)", envName(keyUpstream)))
	}

	apiKeys, err := parseAPIKeys(v.GetString(keyAPIKeys))
	if err != nil {
		errs = append(errs, err)
	}

	budgetWindow := v.GetDuration(keyBudgetWindow)
	if budgetWindow <= 0 {
		errs = append(errs, fmt.Errorf("%s must be a positive duration (e.g. 24h)", envName(keyBudgetWindow)))
	}

	reserveTokens := v.GetInt64(keyBudgetReserveTokens)
	if reserveTokens < 0 {
		errs = append(errs, fmt.Errorf("%s must be >= 0", envName(keyBudgetReserveTokens)))
	}

	defaultBudget := v.GetInt64(keyDefaultBudget)
	if defaultBudget < 0 {
		errs = append(errs, fmt.Errorf("%s must be >= 0", envName(keyDefaultBudget)))
	}

	tenantBudgets, err := parseTenantBudgets(v.GetString(keyTenantBudgets))
	if err != nil {
		errs = append(errs, err)
	}

	usageTopic := strings.TrimSpace(v.GetString(keyUsageTopic))
	if usageTopic == "" {
		errs = append(errs, fmt.Errorf("%s must not be empty", envName(keyUsageTopic)))
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return &Config{
		Addr:                v.GetString(keyAddr),
		UpstreamURL:         upstream,
		APIKeys:             apiKeys,
		TenantConcurrency:   concurrency,
		ReadHeaderTimeout:   v.GetDuration(keyReadHeaderTimeout),
		ShutdownTimeout:     v.GetDuration(keyShutdownTimeout),
		RedisAddr:           strings.TrimSpace(v.GetString(keyRedisAddr)),
		RedisPassword:       v.GetString(keyRedisPassword),
		BudgetWindow:        budgetWindow,
		BudgetReserveTokens: reserveTokens,
		TenantBudgets:       tenantBudgets,
		DefaultTenantBudget: defaultBudget,
		KafkaBrokers:        parseBrokers(v.GetString(keyKafkaBrokers)),
		UsageTopic:          usageTopic,
	}, nil
}

// parseAPIKeys turns "key=tenant,key=tenant" into the key-to-tenant map used
// for authentication and per-tenant accounting.
func parseAPIKeys(raw string) (map[string]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("%s is required (format: \"key=tenant,key=tenant\")", envName(keyAPIKeys))
	}
	keys := make(map[string]string)
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		key, tenant, ok := strings.Cut(pair, "=")
		if !ok || key == "" || tenant == "" {
			return nil, fmt.Errorf("%s contains invalid entry %q (expected \"key=tenant\")", envName(keyAPIKeys), pair)
		}
		keys[key] = tenant
	}
	return keys, nil
}

// parseTenantBudgets turns "tenant-a:100000,tenant-b:250000" into the
// per-tenant window budget map. An empty string means no tenant-specific
// budgets (everyone falls back to the default).
func parseTenantBudgets(raw string) (map[string]int64, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	budgets := make(map[string]int64)
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		tenant, limitRaw, ok := strings.Cut(pair, ":")
		if !ok || tenant == "" {
			return nil, fmt.Errorf("%s contains invalid entry %q (expected \"tenant:tokens\")", envName(keyTenantBudgets), pair)
		}
		limit, err := strconv.ParseInt(limitRaw, 10, 64)
		if err != nil || limit < 0 {
			return nil, fmt.Errorf("%s contains invalid token limit %q (expected a non-negative integer)", envName(keyTenantBudgets), limitRaw)
		}
		budgets[tenant] = limit
	}
	return budgets, nil
}

// parseBrokers splits "host:9092,host:9092" into a broker list.
func parseBrokers(raw string) []string {
	var brokers []string
	for _, broker := range strings.Split(raw, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

func envName(key string) string {
	return envPrefix + "_" + strings.ToUpper(key)
}
