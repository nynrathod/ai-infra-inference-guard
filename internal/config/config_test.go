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
	unsetEnv(t, "GATEWAY_ADDR", "GATEWAY_TENANT_CONCURRENCY", "GATEWAY_READ_HEADER_TIMEOUT", "GATEWAY_SHUTDOWN_TIMEOUT")
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

func TestLoadMissingRequired(t *testing.T) {
	unsetEnv(t, "GATEWAY_UPSTREAM", "GATEWAY_API_KEYS")

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
