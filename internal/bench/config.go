// Package bench implements the load-generation harness used to measure the
// gateway under sustained and adversarial traffic: offered load per tenant,
// streaming time to first token, goodput, admission rejections, and client
// disconnect behavior. It talks to the gateway as a plain HTTP client and
// adds no dependencies beyond the standard library.
package bench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// TenantConfig describes the offered load for one simulated tenant.
type TenantConfig struct {
	// Name labels the results; it only has to be unique within the scenario.
	Name string `json:"name"`
	// APIKey is either a literal key or "env:VAR_NAME", resolved at startup
	// so real keys never live in the repository.
	APIKey string `json:"api_key"`
	// QPS is the offered request rate.
	QPS float64 `json:"qps"`
	// MaxInFlight bounds this tenant's concurrent requests in the harness.
	MaxInFlight int `json:"max_in_flight"`
	// Stream selects SSE streaming (recommended: it measures time to first
	// token) over a single-shot JSON response.
	Stream       bool `json:"stream"`
	MaxTokens    int  `json:"max_tokens"`
	PromptTokens int  `json:"prompt_tokens"`
	// DisconnectProbability is the fraction of streams the client aborts
	// immediately after the first content token.
	DisconnectProbability float64 `json:"disconnect_after_first_byte"`
	// AbortAfterSeconds aborts a stream this many seconds after the request
	// started (0 = never).
	AbortAfterSeconds float64 `json:"abort_after_seconds"`
}

// Scenario is one benchmark configuration: a set of tenants with offered
// load profiles, run concurrently for a fixed sending window.
type Scenario struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	// Model is the served-model name forwarded to vLLM.
	Model string `json:"model"`
	// ChatTemplateKwargs is passed through verbatim, e.g. to disable Qwen3
	// thinking mode. May be nil.
	ChatTemplateKwargs json.RawMessage `json:"chat_template_kwargs"`
	DurationSeconds    int             `json:"duration_seconds"`
	CooldownSeconds    int             `json:"cooldown_seconds"`
	// RequestTimeoutSeconds bounds one request (0 = default 60s).
	RequestTimeoutSeconds int            `json:"request_timeout_seconds"`
	Tenants               []TenantConfig `json:"tenants"`
}

// LoadScenario reads and validates a scenario file. A UTF-8 BOM is
// tolerated so files saved by Windows editors parse too.
func LoadScenario(path string) (Scenario, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Scenario{}, fmt.Errorf("read scenario: %w", err)
	}
	raw = bytes.TrimPrefix(raw, []byte{0xEF, 0xBB, 0xBF})
	var s Scenario
	if err := json.Unmarshal(raw, &s); err != nil {
		return Scenario{}, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := s.Validate(); err != nil {
		return Scenario{}, fmt.Errorf("validate %s: %w", path, err)
	}
	return s, nil
}

// Validate reports whether the scenario is self-consistent and safe to run.
func (s Scenario) Validate() error {
	if strings.TrimSpace(s.Name) == "" {
		return fmt.Errorf("name is required")
	}
	if s.Model == "" {
		return fmt.Errorf("model is required")
	}
	if len(s.Tenants) == 0 {
		return fmt.Errorf("at least one tenant is required")
	}
	if s.DurationSeconds < 1 {
		return fmt.Errorf("duration_seconds must be >= 1")
	}
	if s.CooldownSeconds < 0 {
		return fmt.Errorf("cooldown_seconds must be >= 0")
	}
	if s.RequestTimeoutSeconds != 0 && s.RequestTimeoutSeconds < 1 {
		return fmt.Errorf("request_timeout_seconds must be >= 1 when set")
	}
	if len(s.ChatTemplateKwargs) > 0 {
		var probe any
		if err := json.Unmarshal(s.ChatTemplateKwargs, &probe); err != nil {
			return fmt.Errorf("chat_template_kwargs must be valid JSON: %w", err)
		}
	}
	seen := make(map[string]bool, len(s.Tenants))
	for i := range s.Tenants {
		t := &s.Tenants[i]
		if strings.TrimSpace(t.Name) == "" {
			return fmt.Errorf("tenants[%d].name is required", i)
		}
		if seen[t.Name] {
			return fmt.Errorf("tenant name %q is used twice", t.Name)
		}
		seen[t.Name] = true
		if t.APIKey == "" {
			return fmt.Errorf("tenant %q: api_key is required", t.Name)
		}
		if t.QPS <= 0 {
			return fmt.Errorf("tenant %q: qps must be > 0", t.Name)
		}
		if t.MaxInFlight < 1 {
			return fmt.Errorf("tenant %q: max_in_flight must be >= 1", t.Name)
		}
		if t.MaxTokens < 1 {
			return fmt.Errorf("tenant %q: max_tokens must be >= 1", t.Name)
		}
		if t.PromptTokens < 1 || t.PromptTokens > 8192 {
			return fmt.Errorf("tenant %q: prompt_tokens must be within [1, 8192]", t.Name)
		}
		if t.DisconnectProbability < 0 || t.DisconnectProbability > 1 {
			return fmt.Errorf("tenant %q: disconnect_after_first_byte must be within [0, 1]", t.Name)
		}
		if t.AbortAfterSeconds < 0 {
			return fmt.Errorf("tenant %q: abort_after_seconds must be >= 0", t.Name)
		}
	}
	return nil
}

// Duration is the window over which new requests are issued.
func (s *Scenario) Duration() time.Duration {
	return time.Duration(s.DurationSeconds) * time.Second
}

// Cooldown is the extra window in-flight requests get to finish after
// sending stops.
func (s *Scenario) Cooldown() time.Duration {
	return time.Duration(s.CooldownSeconds) * time.Second
}

// Timeout is the per-request budget, defaulting to 60 seconds.
func (s *Scenario) Timeout() time.Duration {
	if s.RequestTimeoutSeconds > 0 {
		return time.Duration(s.RequestTimeoutSeconds) * time.Second
	}
	return 60 * time.Second
}

// ResolveAPIKeys turns "env:NAME" references into literal keys. All missing
// variables are reported at once so a run fails fast, before any load is
// sent.
func (s *Scenario) ResolveAPIKeys() (map[string]string, error) {
	keys := make(map[string]string, len(s.Tenants))
	var missing []string
	for _, t := range s.Tenants {
		value := t.APIKey
		if name, ok := strings.CutPrefix(t.APIKey, "env:"); ok {
			value = os.Getenv(name)
			if value == "" {
				missing = append(missing, name)
			}
		}
		keys[t.Name] = value
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("unset environment variables: %s", strings.Join(missing, ", "))
	}
	return keys, nil
}
