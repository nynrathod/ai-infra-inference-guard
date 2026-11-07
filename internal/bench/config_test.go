package bench

import (
        "encoding/json"
        "path/filepath"
        "strings"
        "testing"
)

func validScenario() Scenario {
        return Scenario{
                Name:            "probe",
                Model:           "test-model",
                DurationSeconds: 1,
                CooldownSeconds: 1,
                Tenants: []TenantConfig{{
                        Name: "a", APIKey: "k", QPS: 1, MaxInFlight: 1,
                        Stream: true, MaxTokens: 4, PromptTokens: 4,
                }},
        }
}

func TestScenarioValidateRejectsNonsense(t *testing.T) {
        cases := []struct {
                name   string
                mutate func(*Scenario)
        }{
                {"no name", func(s *Scenario) { s.Name = "" }},
                {"whitespace name", func(s *Scenario) { s.Name = "   " }},
                {"no tenants", func(s *Scenario) { s.Tenants = nil }},
                {"zero duration", func(s *Scenario) { s.DurationSeconds = 0 }},
                {"negative cooldown", func(s *Scenario) { s.CooldownSeconds = -1 }},
                {"bad request timeout", func(s *Scenario) { s.RequestTimeoutSeconds = -2 }},
                {"bad kwargs json", func(s *Scenario) { s.ChatTemplateKwargs = json.RawMessage(`{oops`) }},
                {"negative latency target", func(s *Scenario) { s.LatencyTargetMS = -1 }},
                {"blank tenant name", func(s *Scenario) { s.Tenants[0].Name = " " }},
                {"duplicate tenant", func(s *Scenario) { s.Tenants = append(s.Tenants, s.Tenants[0]) }},
                {"no api key", func(s *Scenario) { s.Tenants[0].APIKey = "" }},
                {"zero qps", func(s *Scenario) { s.Tenants[0].QPS = 0 }},
                {"zero in-flight", func(s *Scenario) { s.Tenants[0].MaxInFlight = 0 }},
                {"zero max tokens", func(s *Scenario) { s.Tenants[0].MaxTokens = 0 }},
                {"zero prompt tokens", func(s *Scenario) { s.Tenants[0].PromptTokens = 0 }},
                {"huge prompt tokens", func(s *Scenario) { s.Tenants[0].PromptTokens = 9999 }},
                {"disconnect over 1", func(s *Scenario) { s.Tenants[0].DisconnectProbability = 1.5 }},
                {"negative abort", func(s *Scenario) { s.Tenants[0].AbortAfterSeconds = -1 }},
        }
        for _, tc := range cases {
                t.Run(tc.name, func(t *testing.T) {
                        s := validScenario()
                        tc.mutate(&s)
                        if err := s.Validate(); err == nil {
                                t.Fatalf("expected a validation error")
                        }
                })
        }
}

func TestScenarioValidateAcceptsGoodInput(t *testing.T) {
        if err := validScenario().Validate(); err != nil {
                t.Fatalf("unexpected error: %v", err)
        }
        // A missing model is valid: the runner discovers it via /v1/models.
        s := validScenario()
        s.Model = ""
        if err := s.Validate(); err != nil {
                t.Fatalf("scenario without model rejected: %v", err)
        }
}

func TestResolveAPIKeys(t *testing.T) {
        t.Setenv("BENCH_KEY_PRESENT", "secret-value")
        s := Scenario{Tenants: []TenantConfig{
                {Name: "lit", APIKey: "literal"},
                {Name: "env", APIKey: "env:BENCH_KEY_PRESENT"},
                {Name: "missing", APIKey: "env:BENCH_KEY_ABSENT"},
        }}
        keys, err := s.ResolveAPIKeys()
        if err == nil {
                t.Fatalf("expected an error for the unset variable")
        }
        if keys != nil {
                t.Fatalf("expected no keys on error")
        }
        if !strings.Contains(err.Error(), "BENCH_KEY_ABSENT") {
                t.Fatalf("error should name the missing variable, got: %v", err)
        }
}

// TestShippedScenariosValidate guards the scenario files that ship with the
// repository: each parses, validates, and carries no literal API key.
func TestShippedScenariosValidate(t *testing.T) {
        paths, err := filepath.Glob(filepath.Join("..", "..", "bench", "scenarios", "*.json"))
        if err != nil || len(paths) == 0 {
                t.Fatalf("no scenario files found under bench/scenarios: %v", err)
        }
        names := make(map[string]bool)
        for _, p := range paths {
                s, err := LoadScenario(p)
                if err != nil {
                        t.Fatalf("%s: %v", p, err)
                }
                if names[s.Name] {
                        t.Fatalf("scenario name %q is used by more than one file", s.Name)
                }
                names[s.Name] = true
                for _, tenant := range s.Tenants {
                        if !strings.HasPrefix(tenant.APIKey, "env:") {
                                t.Errorf("%s: tenant %q ships a literal api key", p, tenant.Name)
                        }
                }
        }
}
