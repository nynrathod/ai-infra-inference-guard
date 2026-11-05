package bench

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Runner executes scenarios against a gateway endpoint.
type Runner struct {
	// Target is the gateway base URL, e.g. http://localhost:8080.
	Target string
	// Model overrides the scenario's model when non-empty.
	Model string
	// Seed makes disconnect decisions reproducible across runs.
	Seed int64

	client *http.Client
}

// NewRunner returns a runner with a connection pool sized for benchmarking.
func NewRunner(target string) *Runner {
	return &Runner{
		Target: strings.TrimRight(target, "/"),
		Seed:   1,
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:          256,
				MaxIdleConnsPerHost:   256,
				IdleConnTimeout:       30 * time.Second,
				ResponseHeaderTimeout: 30 * time.Second,
			},
		},
	}
}

// Run executes the scenario and returns its report. New requests are issued
// for the scenario's duration; in-flight requests then get the cooldown
// window to finish before the run ends.
func (r *Runner) Run(ctx context.Context, s Scenario) (RunReport, error) {
	keys, err := s.ResolveAPIKeys()
	if err != nil {
		return RunReport{}, err
	}
	model := s.Model
	if r.Model != "" {
		model = r.Model
	}
	if model == "" {
		return RunReport{}, fmt.Errorf("no model configured: set it in the scenario or on the runner")
	}

	runCtx, cancelRun := context.WithTimeout(ctx, s.Duration()+s.Cooldown()+5*time.Second)
	defer cancelRun()
	paceCtx, cancelPace := context.WithTimeout(runCtx, s.Duration())
	defer cancelPace()

	started := time.Now()
	resCh := make(chan RequestResult, 1024)
	var results []RequestResult
	var collect sync.WaitGroup
	collect.Add(1)
	go func() {
		defer collect.Done()
		for res := range resCh {
			results = append(results, res)
		}
	}()

	var peakMu sync.Mutex
	peaks := make(map[string]int)
	var tenants sync.WaitGroup
	for _, t := range s.Tenants {
		tenants.Add(1)
		go func(t TenantConfig) {
			defer tenants.Done()
			r.runTenant(paceCtx, runCtx, t, keys[t.Name], model, s, resCh, &peakMu, peaks)
		}(t)
	}
	tenants.Wait()
	close(resCh)
	collect.Wait()

	active := time.Since(started).Seconds()
	if active > s.Duration().Seconds() {
		active = s.Duration().Seconds()
	}
	stats := Summarize(results, peaks, active)

	// Every scenario tenant appears in the report even if it produced no
	// recorded results (all requests dropped at the end of the window).
	present := make(map[string]bool, len(stats))
	for _, st := range stats {
		present[st.Name] = true
	}
	for _, t := range s.Tenants {
		if !present[t.Name] {
			stats = append(stats, TenantStats{Name: t.Name, MaxInFlightObserved: peaks[t.Name]})
		}
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].Name < stats[j].Name })

	return RunReport{
		Scenario:      s.Name,
		Description:   s.Description,
		Target:        r.Target,
		Model:         model,
		StartedAt:     started.UTC(),
		WallSeconds:   time.Since(started).Seconds(),
		ActiveSeconds: active,
		Tenants:       stats,
	}, nil
}

// runTenant issues requests at the tenant's offered rate until paceCtx
// closes, never exceeding MaxInFlight concurrent requests. Ticks that find
// no free slot are dropped, so the offered rate never exceeds the tenant's
// in-flight bound and any 429s measured come from the gateway's admission
// control, not from the harness queuing against itself.
func (r *Runner) runTenant(paceCtx, runCtx context.Context, t TenantConfig, key, model string, s Scenario, resCh chan<- RequestResult, peakMu *sync.Mutex, peaks map[string]int) {
	var wgp sync.WaitGroup
	defer wgp.Wait()
	interval := time.Duration(float64(time.Second) / t.QPS)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slots := make(chan struct{}, t.MaxInFlight)
	rng := rand.New(rand.NewSource(r.seedFor(t.Name)))
	gauge := &tenantGauge{mu: peakMu, peaks: peaks, name: t.Name}

	for {
		select {
		case <-paceCtx.Done():
			return
		case <-ticker.C:
		}
		select {
		case slots <- struct{}{}:
		case <-paceCtx.Done():
			return
		}
		disconnect := t.DisconnectProbability > 0 && rng.Float64() < t.DisconnectProbability
		wgp.Add(1)
		go func() {
			defer wgp.Done()
			defer func() { <-slots }()
			gauge.enter()
			defer gauge.exit()
			r.doRequest(runCtx, t, key, model, s, disconnect, resCh)
		}()
	}
}

// tenantGauge tracks concurrent in-flight requests per tenant and keeps the
// peak observed, which is reported with the run.
type tenantGauge struct {
	mu    *sync.Mutex
	peaks map[string]int
	name  string
	cur   int
}

func (g *tenantGauge) enter() {
	g.mu.Lock()
	g.cur++
	if g.cur > g.peaks[g.name] {
		g.peaks[g.name] = g.cur
	}
	g.mu.Unlock()
}

func (g *tenantGauge) exit() {
	g.mu.Lock()
	g.cur--
	g.mu.Unlock()
}

// doRequest performs one chat completion and records its outcome. Requests
// still in flight when the run window closes are dropped rather than
// counted: they say nothing about the gateway.
func (r *Runner) doRequest(ctx context.Context, t TenantConfig, key, model string, s Scenario, disconnect bool, resCh chan<- RequestResult) {
	res := RequestResult{Tenant: t.Name, Outcome: OutcomeTransport, TTFTMs: -1, TotalMs: -1}

	body, err := buildRequest(model, t, s)
	if err != nil {
		resCh <- res
		return
	}
	reqCtx, cancel := context.WithTimeout(ctx, s.Timeout())
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
		r.Target+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		resCh <- res
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)

	start := time.Now()
	resp, err := r.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return // the run window closed under us; drop
		}
		if errors.Is(reqCtx.Err(), context.DeadlineExceeded) {
			res.Outcome = OutcomeTimeout
		}
		res.TotalMs = msSince(start)
		resCh <- res
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		res.TotalMs = msSince(start)
		switch {
		case resp.StatusCode == http.StatusTooManyRequests:
			res.Outcome = OutcomeRateLimited
			res.RetryAfterSeconds = retryAfterSeconds(resp.Header.Get("Retry-After"))
		case resp.StatusCode >= 500:
			res.Outcome = OutcomeServerError
		default:
			res.Outcome = OutcomeHTTPErr
		}
		resCh <- res
		return
	}

	if t.Stream {
		if !r.readStream(reqCtx, resp.Body, &res, start, disconnect, t.AbortAfterSeconds) {
			return
		}
	} else {
		if !r.readNonStream(reqCtx, resp.Body, &res, start) {
			return
		}
	}
	resCh <- res
}

// streamChunk is one SSE data frame of a chat completion stream.
type streamChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
	Usage *struct {
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

// readStream consumes an SSE stream, capturing time to first token,
// completion tokens, and intentional client aborts. It returns false when
// the run window closed mid-stream, in which case the result is dropped.
func (r *Runner) readStream(reqCtx context.Context, body io.Reader, res *RequestResult, start time.Time, disconnect bool, abortAfterSeconds float64) bool {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024)
	abortAfter := time.Duration(abortAfterSeconds * float64(time.Second))

	seenToken := false
	deltaTokens := 0
	usageTokens := 0
	setTokens := func() {
		if usageTokens > 0 {
			res.Tokens = usageTokens
		} else {
			res.Tokens = deltaTokens
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "[DONE]" {
			setTokens()
			res.Outcome = OutcomeSuccess
			res.TotalMs = msSince(start)
			return true
		}
		if payload == "" {
			continue
		}
		var chunk streamChunk
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			continue // frames we do not understand are skipped
		}
		content := ""
		if len(chunk.Choices) > 0 {
			content = chunk.Choices[0].Delta.Content
		}
		if content != "" {
			if !seenToken {
				seenToken = true
				res.TTFTMs = msSince(start)
			}
			deltaTokens++
		}
		if chunk.Usage != nil && chunk.Usage.CompletionTokens > 0 {
			usageTokens = chunk.Usage.CompletionTokens
		}
		if disconnect && seenToken {
			setTokens()
			res.Outcome = OutcomeClientAborted
			res.TotalMs = msSince(start)
			return true
		}
		if abortAfter > 0 && time.Since(start) >= abortAfter {
			setTokens()
			res.Outcome = OutcomeClientAborted
			res.TotalMs = msSince(start)
			return true
		}
	}

	if err := scanner.Err(); err != nil {
		switch {
		case reqCtx.Err() == context.DeadlineExceeded:
			res.Outcome = OutcomeTimeout
			setTokens()
			res.TotalMs = msSince(start)
			return true
		case reqCtx.Err() == context.Canceled:
			return false // run window closed mid-stream; drop
		default:
			res.Outcome = OutcomeTransport
			setTokens()
			res.TotalMs = msSince(start)
			return true
		}
	}
	// Server closed the stream without [DONE]; count what arrived.
	setTokens()
	res.Outcome = OutcomeSuccess
	res.TotalMs = msSince(start)
	return true
}

// readNonStream consumes a single-shot JSON response. It returns false when
// the run window closed mid-read, in which case the result is dropped.
func (r *Runner) readNonStream(reqCtx context.Context, body io.Reader, res *RequestResult, start time.Time) bool {
	raw, err := io.ReadAll(body)
	res.TotalMs = msSince(start)
	if err != nil {
		switch {
		case reqCtx.Err() == context.DeadlineExceeded:
			res.Outcome = OutcomeTimeout
		case reqCtx.Err() == context.Canceled:
			return false
		default:
			res.Outcome = OutcomeTransport
		}
		return true
	}
	var payload struct {
		Usage *struct {
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		res.Outcome = OutcomeServerError
		return true
	}
	if payload.Usage != nil {
		res.Tokens = payload.Usage.CompletionTokens
	}
	res.Outcome = OutcomeSuccess
	return true
}

// buildRequest renders one chat-completion request body. The prompt
// approximates prompt_tokens with one common word per token; served-model
// aliasing keeps tokenization differences out of the comparison.
func buildRequest(model string, t TenantConfig, s Scenario) (string, error) {
	req := chatRequest{
		Model:              model,
		Messages:           []chatMessage{{Role: "user", Content: strings.Repeat("benchmark ", t.PromptTokens)}},
		MaxTokens:          t.MaxTokens,
		Stream:             t.Stream,
		ChatTemplateKwargs: s.ChatTemplateKwargs,
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

type chatRequest struct {
	Model              string          `json:"model"`
	Messages           []chatMessage   `json:"messages"`
	MaxTokens          int             `json:"max_tokens"`
	Stream             bool            `json:"stream"`
	ChatTemplateKwargs json.RawMessage `json:"chat_template_kwargs,omitempty"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// retryAfterSeconds parses a Retry-After header in either the
// delay-seconds or the HTTP-date form.
func retryAfterSeconds(header string) float64 {
	if header == "" {
		return 0
	}
	if seconds, err := strconv.ParseFloat(header, 64); err == nil && seconds >= 0 {
		return seconds
	}
	if at, err := http.ParseTime(header); err == nil {
		return math.Max(0, time.Until(at).Seconds())
	}
	return 0
}

// seedFor derives a stable per-tenant seed so disconnect decisions are
// reproducible run to run.
func (r *Runner) seedFor(tenant string) int64 {
	h := fnv.New64a()
	h.Write([]byte(tenant))
	return r.Seed ^ int64(h.Sum64())
}

func msSince(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000
}
