package bench

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCalibrateDerivesDefaultsFromScenario(t *testing.T) {
	s := Scenario{
		Name:            "cal",
		Model:           "test-model",
		DurationSeconds: 1,
		Tenants: []TenantConfig{{
			Name: "a", APIKey: "k", QPS: 4, MaxInFlight: 4,
			Stream: true, MaxTokens: 8, PromptTokens: 8,
		}},
	}

	// Defaults are derived inside Calibrate; verify them by running the
	// cheapest possible ramp (one step) against a live endpoint.
	srv := streamHandlerDelayed()
	defer srv.Close()

	runner := NewRunner(srv.URL)
	res, err := runner.Calibrate(context.Background(), s, CalibrateOptions{
		FromQPS:     4,
		StepQPS:     4,
		MaxQPS:      4,
		StepSeconds: 5,
	})
	if err != nil {
		t.Fatalf("calibrate: %v", err)
	}
	if len(res.Steps) != 1 {
		t.Fatalf("steps = %d, want 1", len(res.Steps))
	}
	step := res.Steps[0]
	if step.OfferedQPS != 4 {
		t.Fatalf("offered = %v, want 4", step.OfferedQPS)
	}
	if step.SuccessRate < 0.99 {
		t.Fatalf("success rate = %v, want all requests served", step.SuccessRate)
	}
	if !step.Sustainable || res.SustainableQPS != 4 {
		t.Fatalf("healthy endpoint should be sustainable at its own rate: %+v", step)
	}
	if res.SuggestedSaturationQPS != 8 {
		t.Fatalf("suggested saturation = %v, want 8 (2x sustainable)", res.SuggestedSaturationQPS)
	}
	if res.MeasuredE2EP95Seconds <= 0 {
		t.Fatalf("measured e2e p95 = %v, want > 0", res.MeasuredE2EP95Seconds)
	}
}

func TestCalibrateRejectsEmptyScenario(t *testing.T) {
	runner := NewRunner("http://127.0.0.1:1")
	if _, err := runner.Calibrate(context.Background(), Scenario{Name: "empty"}, CalibrateOptions{}); err == nil {
		t.Fatal("expected an error for a scenario with no tenants")
	}
}

func TestCalibrateLatencyTargetMarksUnsustainable(t *testing.T) {
	// First token takes ~250ms; a 100ms deadline makes every step
	// unsustainable even though all requests succeed.
	srv := streamHandlerDelayed()
	defer srv.Close()

	s := Scenario{
		Name:            "cal-tight",
		Model:           "test-model",
		DurationSeconds: 1,
		Tenants: []TenantConfig{{
			Name: "a", APIKey: "stream-key", QPS: 4, MaxInFlight: 8,
			Stream: true, MaxTokens: 8, PromptTokens: 8,
		}},
	}
	runner := NewRunner(srv.URL)
	res, err := runner.Calibrate(context.Background(), s, CalibrateOptions{
		FromQPS:         4,
		StepQPS:         4,
		MaxQPS:          4,
		StepSeconds:     5,
		LatencyTargetMS: 100,
	})
	if err != nil {
		t.Fatalf("calibrate: %v", err)
	}
	if len(res.Steps) != 1 || res.Steps[0].Sustainable {
		t.Fatalf("tight deadline should mark the step unsustainable: %+v", res.Steps)
	}
	if res.SustainableQPS != 0 {
		t.Fatalf("sustainable = %v, want 0", res.SustainableQPS)
	}
}

// streamHandlerDelayed serves a deterministic SSE stream whose first token
// arrives after ~250ms, on its own test server.
func streamHandlerDelayed() *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		time.Sleep(250 * time.Millisecond)
		w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"tok\"}}]}\n\n"))
		w.Write([]byte("data: {\"choices\":[{\"delta\":{}}],\"usage\":{\"completion_tokens\":1}}\n\n"))
		w.Write([]byte("data: [DONE]\n\n"))
	}))
	return srv
}
