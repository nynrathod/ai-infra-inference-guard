package config

import (
	"errors"
	"fmt"
	"net/url"
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
	ReadHeaderTimeout time.Duration
	ShutdownTimeout   time.Duration
}

const (
	envPrefix = "GATEWAY"

	keyAddr              = "addr"
	keyUpstream          = "upstream"
	keyAPIKeys           = "api_keys"
	keyReadHeaderTimeout = "read_header_timeout"
	keyShutdownTimeout   = "shutdown_timeout"

	defaultAddr              = ":8080"
	defaultReadHeaderTimeout = 10 * time.Second
	defaultShutdownTimeout   = 15 * time.Second
)

// Load reads all GATEWAY_* environment variables and fails fast on any
// invalid or missing required value.
func Load() (*Config, error) {
	v := viper.New()
	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	v.SetDefault(keyAddr, defaultAddr)
	v.SetDefault(keyReadHeaderTimeout, defaultReadHeaderTimeout)
	v.SetDefault(keyShutdownTimeout, defaultShutdownTimeout)

	var errs []error

	upstream, err := url.Parse(v.GetString(keyUpstream))
	if err != nil || upstream.Scheme == "" || upstream.Host == "" {
		errs = append(errs, fmt.Errorf("%s must be an absolute URL (e.g. http://localhost:8000)", envName(keyUpstream)))
	}

	apiKeys, err := parseAPIKeys(v.GetString(keyAPIKeys))
	if err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return &Config{
		Addr:              v.GetString(keyAddr),
		UpstreamURL:       upstream,
		APIKeys:           apiKeys,
		ReadHeaderTimeout: v.GetDuration(keyReadHeaderTimeout),
		ShutdownTimeout:   v.GetDuration(keyShutdownTimeout),
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

func envName(key string) string {
	return envPrefix + "_" + strings.ToUpper(key)
}
