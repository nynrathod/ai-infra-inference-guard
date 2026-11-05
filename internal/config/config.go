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

	AIMDEnabled              bool
	AIMDInterval             time.Duration
	AIMDMinTenantConcurrency int
	AIMDDecreaseFactor       float64
	AIMDAdditiveStep         int
	AIMDRecoveryTicks        int
	AIMDQueueDepthHigh       float64
	AIMDKVCacheHigh          float64
	AIMDTTFTHigh             time.Duration
	AIMDDominantShare        float64

	VLLMMetricsURL     string
	VLLMMetricsTimeout time.Duration

	RedisAddr           string
	RedisPassword       string
	KafkaBrokers        []string
	BudgetWindow        time.Duration
	BudgetReserveTokens int64
	TenantBudgets       map[string]int64
	DefaultTenantBudget int64
	UsageTopic          string
}

const (
	envPrefix = "GATEWAY"

	keyAddr              = "addr"
	keyUpstream          = "upstream"
	keyAPIKeys           = "api_keys"
	keyTenantConcurrency = "tenant_concurrency"
	keyReadHeaderTimeout = "read_header_timeout"
	keyShutdownTimeout   = "shutdown_timeout"

	keyAIMDEnabled         = "aimd_enabled"
	keyAIMDInterval        = "aimd_interval"
	keyAIMDMinTenantLimits = "aimd_min_tenant_concurrency"
	keyAIMDDecreaseFactor  = "aimd_decrease_factor"
	keyAIMDAdditiveStep    = "aimd_additive_step"
	keyAIMDRecoveryTicks   = "aimd_recovery_ticks"
	keyAIMDQueueDepthHigh  = "aimd_queue_depth_high"
	keyAIMDKVCacheHigh     = "aimd_kv_cache_high"
	keyAIMDTTFTHigh        = "aimd_ttft_high"
	keyAIMDDominantShare   = "aimd_dominant_share"

	keyVLLMMetricsURL     = "vllm_metrics_url"
	keyVLLMMetricsTimeout = "vllm_metrics_timeout"

	keyRedisAddr           = "redis_addr"
	keyRedisPassword       = "redis_password"
	keyKafkaBrokers        = "kafka_brokers"
	keyBudgetWindow        = "budget_window"
	keyBudgetReserveTokens = "budget_reserve_tokens"
	keyTenantBudgets       = "tenant_budgets"
	keyDefaultTenantBudget = "default_tenant_budget"
	keyUsageTopic          = "usage_topic"

	defaultAddr                = ":8080"
	defaultTenantConcurrency   = 8
	defaultReadHeaderTimeout   = 10 * time.Second
	defaultShutdownTimeout     = 15 * time.Second
	defaultAIMDInterval        = 2 * time.Second
	defaultAIMDMinTenantLimits = 2
	defaultAIMDDecreaseFactor  = 0.5
	defaultAIMDAdditiveStep    = 1
	defaultAIMDRecoveryTicks   = 3
	defaultAIMDQueueDepthHigh  = 12
	defaultAIMDKVCacheHigh     = 0.90
	defaultAIMDTTFTHigh        = 2500 * time.Millisecond
	defaultAIMDDominantShare   = 0.6
	defaultVLLMMetricsTimeout  = time.Second

	defaultBudgetWindow        = 24 * time.Hour
	defaultBudgetReserveTokens = int64(1024)
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
	v.SetDefault(keyAIMDEnabled, true)
	v.SetDefault(keyAIMDInterval, defaultAIMDInterval)
	v.SetDefault(keyAIMDMinTenantLimits, defaultAIMDMinTenantLimits)
	v.SetDefault(keyAIMDDecreaseFactor, defaultAIMDDecreaseFactor)
	v.SetDefault(keyAIMDAdditiveStep, defaultAIMDAdditiveStep)
	v.SetDefault(keyAIMDRecoveryTicks, defaultAIMDRecoveryTicks)
	v.SetDefault(keyAIMDQueueDepthHigh, defaultAIMDQueueDepthHigh)
	v.SetDefault(keyAIMDKVCacheHigh, defaultAIMDKVCacheHigh)
	v.SetDefault(keyAIMDTTFTHigh, defaultAIMDTTFTHigh)
	v.SetDefault(keyAIMDDominantShare, defaultAIMDDominantShare)
	v.SetDefault(keyVLLMMetricsTimeout, defaultVLLMMetricsTimeout)
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

	if minLimit := v.GetInt(keyAIMDMinTenantLimits); minLimit < 1 || minLimit > concurrency {
		errs = append(errs, fmt.Errorf("%s must be between 1 and %s",
			envName(keyAIMDMinTenantLimits), envName(keyTenantConcurrency)))
	}
	if factor := v.GetFloat64(keyAIMDDecreaseFactor); factor <= 0 || factor > 1 {
		errs = append(errs, fmt.Errorf("%s must be within (0, 1]", envName(keyAIMDDecreaseFactor)))
	}
	if step := v.GetInt(keyAIMDAdditiveStep); step < 1 {
		errs = append(errs, fmt.Errorf("%s must be >= 1", envName(keyAIMDAdditiveStep)))
	}
	if ticks := v.GetInt(keyAIMDRecoveryTicks); ticks < 1 {
		errs = append(errs, fmt.Errorf("%s must be >= 1", envName(keyAIMDRecoveryTicks)))
	}
	if depth := v.GetFloat64(keyAIMDQueueDepthHigh); depth <= 0 {
		errs = append(errs, fmt.Errorf("%s must be > 0", envName(keyAIMDQueueDepthHigh)))
	}
	if kv := v.GetFloat64(keyAIMDKVCacheHigh); kv <= 0 || kv > 1 {
		errs = append(errs, fmt.Errorf("%s must be within (0, 1]", envName(keyAIMDKVCacheHigh)))
	}
	if ttft := v.GetDuration(keyAIMDTTFTHigh); ttft <= 0 {
		errs = append(errs, fmt.Errorf("%s must be > 0", envName(keyAIMDTTFTHigh)))
	}
	if share := v.GetFloat64(keyAIMDDominantShare); share <= 0 || share > 1 {
		errs = append(errs, fmt.Errorf("%s must be within (0, 1]", envName(keyAIMDDominantShare)))
	}
	if interval := v.GetDuration(keyAIMDInterval); interval <= 0 {
		errs = append(errs, fmt.Errorf("%s must be > 0", envName(keyAIMDInterval)))
	}
	if timeout := v.GetDuration(keyVLLMMetricsTimeout); timeout <= 0 {
		errs = append(errs, fmt.Errorf("%s must be > 0", envName(keyVLLMMetricsTimeout)))
	}

	budgets, err := parseTenantBudgets(v.GetString(keyTenantBudgets))
	if err != nil {
		errs = append(errs, err)
	}
	brokers := parseBrokers(v.GetString(keyKafkaBrokers))
	if window := v.GetDuration(keyBudgetWindow); window <= 0 {
		errs = append(errs, fmt.Errorf("%s must be a positive duration (e.g. 24h)", envName(keyBudgetWindow)))
	}
	if reserve := v.GetInt64(keyBudgetReserveTokens); reserve < 1 {
		errs = append(errs, fmt.Errorf("%s must be >= 1", envName(keyBudgetReserveTokens)))
	}
	if def := v.GetInt64(keyDefaultTenantBudget); def < 0 {
		errs = append(errs, fmt.Errorf("%s must be >= 0", envName(keyDefaultTenantBudget)))
	}

	metricsURL := ""
	if raw := strings.TrimSpace(v.GetString(keyVLLMMetricsURL)); raw != "" {
		u, err := url.Parse(raw)
		if err != nil || u.Scheme == "" || u.Host == "" {
			errs = append(errs, fmt.Errorf("%s must be an absolute URL (e.g. http://vllm:8000/metrics)",
				envName(keyVLLMMetricsURL)))
		} else {
			metricsURL = raw
		}
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	if metricsURL == "" {
		clone := *upstream
		clone.Path = "/metrics"
		clone.RawQuery = ""
		metricsURL = clone.String()
	}

	usageTopic := strings.TrimSpace(v.GetString(keyUsageTopic))
	if usageTopic == "" {
		usageTopic = defaultUsageTopic
	}

	return &Config{
		Addr:              v.GetString(keyAddr),
		UpstreamURL:       upstream,
		APIKeys:           apiKeys,
		TenantConcurrency: concurrency,
		ReadHeaderTimeout: v.GetDuration(keyReadHeaderTimeout),
		ShutdownTimeout:   v.GetDuration(keyShutdownTimeout),

		AIMDEnabled:              v.GetBool(keyAIMDEnabled),
		AIMDInterval:             v.GetDuration(keyAIMDInterval),
		AIMDMinTenantConcurrency: v.GetInt(keyAIMDMinTenantLimits),
		AIMDDecreaseFactor:       v.GetFloat64(keyAIMDDecreaseFactor),
		AIMDAdditiveStep:         v.GetInt(keyAIMDAdditiveStep),
		AIMDRecoveryTicks:        v.GetInt(keyAIMDRecoveryTicks),
		AIMDQueueDepthHigh:       v.GetFloat64(keyAIMDQueueDepthHigh),
		AIMDKVCacheHigh:          v.GetFloat64(keyAIMDKVCacheHigh),
		AIMDTTFTHigh:             v.GetDuration(keyAIMDTTFTHigh),
		AIMDDominantShare:        v.GetFloat64(keyAIMDDominantShare),

		VLLMMetricsURL:     metricsURL,
		VLLMMetricsTimeout: v.GetDuration(keyVLLMMetricsTimeout),

		RedisAddr:           strings.TrimSpace(v.GetString(keyRedisAddr)),
		RedisPassword:       v.GetString(keyRedisPassword),
		KafkaBrokers:        brokers,
		BudgetWindow:        v.GetDuration(keyBudgetWindow),
		BudgetReserveTokens: v.GetInt64(keyBudgetReserveTokens),
		TenantBudgets:       budgets,
		DefaultTenantBudget: v.GetInt64(keyDefaultTenantBudget),
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

// parseBrokers splits a comma-separated broker list; empty input yields nil.
func parseBrokers(raw string) []string {
	var brokers []string
	for _, b := range strings.Split(raw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	return brokers
}

// parseTenantBudgets turns "tenant:tokens, tenant:tokens" into the per-tenant
// window budget map; empty input yields a nil map (no per-tenant caps).
func parseTenantBudgets(raw string) (map[string]int64, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	budgets := make(map[string]int64)
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		tenant, tokens, ok := strings.Cut(pair, ":")
		tenant = strings.TrimSpace(tenant)
		tokens = strings.TrimSpace(tokens)
		if !ok || tenant == "" {
			return nil, fmt.Errorf("%s contains invalid entry %q (expected \"tenant:tokens\")", envName(keyTenantBudgets), pair)
		}
		n, err := strconv.ParseInt(tokens, 10, 64)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("%s contains an invalid token count for tenant %q", envName(keyTenantBudgets), tenant)
		}
		budgets[tenant] = n
	}
	return budgets, nil
}

func envName(key string) string {
	return envPrefix + "_" + strings.ToUpper(key)
}
