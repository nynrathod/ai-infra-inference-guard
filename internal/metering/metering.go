// Package metering implements per-tenant token budgets and usage metering:
// every admitted request reserves tokens against a rolling budget window,
// the real usage is settled from the upstream response, and one Usage event
// per request is handed to an EventSink (e.g. a Kafka producer) without ever
// blocking the request path.
package metering

import (
        "bytes"
        "context"
        "crypto/rand"
        "encoding/hex"
        "encoding/json"
        "errors"
        "fmt"
        "io"
        "log/slog"
        "net/http"
        "strconv"
        "strings"
        "time"

        "github.com/nynrathod/inference-guard/internal/apierror"
        "github.com/nynrathod/inference-guard/internal/auth"
)

// Usage is the metering record for one completed (or rejected) request.
type Usage struct {
        TenantID         string    `json:"tenant_id"`
        RequestID        string    `json:"request_id"`
        Model            string    `json:"model,omitempty"`
        PromptTokens     int64     `json:"prompt_tokens"`
        CompletionTokens int64     `json:"completion_tokens"`
        TotalTokens      int64     `json:"total_tokens"`
        Streamed         bool      `json:"streamed"`
        Status           int       `json:"status"`
        TTFTMS           int64     `json:"ttft_ms"`
        DurationMS       int64     `json:"duration_ms"`
        Reason           string    `json:"reason,omitempty"`
        RecordedAt       time.Time `json:"recorded_at"`
}

// HasUsage reports whether real token counts were parsed from the response.
func (u Usage) HasUsage() bool {
        return u.PromptTokens > 0 || u.CompletionTokens > 0 || u.TotalTokens > 0
}

// ErrBudgetExhausted is returned by BudgetStore.Reserve when the tenant has
// no tokens left in the current window.
var ErrBudgetExhausted = errors.New("token budget exhausted")

// BudgetStore reserves tokens against per-tenant budget windows and settles
// the reservation once real usage is known. Implementations must be safe
// for concurrent use.
type BudgetStore interface {
        // Reserve atomically charges tokens to the tenant's current window,
        // returning ErrBudgetExhausted when that would exceed limit. A limit
        // of 0 or less disables the cap (accounting only).
        Reserve(ctx context.Context, tenant string, tokens, limit int64) error
        // Commit adjusts the window by (actual - reserved): over-reservations
        // are refunded, under-reservations debited.
        Commit(ctx context.Context, tenant string, reserved, actual int64) error
        // Close releases the store's resources.
        Close() error
}

// EventSink receives usage events. Emit must never block the request path:
// implementations buffer or drop.
type EventSink interface {
        Emit(Usage)
        // Close flushes and releases the sink.
        Close() error
}

// NoopSink discards events; the default when no broker is configured.
type NoopSink struct{}

// Emit discards the event.
func (NoopSink) Emit(Usage) {}

// Close is a no-op.
func (NoopSink) Close() error { return nil }

// Options configures the metering middleware.
type Options struct {
        // Window is the budget window length; budgets reset at each boundary.
        Window time.Duration
        // ReserveTokens is charged per request before real usage is known and
        // adjusted to actual usage once the response completes. It is also
        // the fallback output component when the request declares no
        // max_tokens.
        ReserveTokens int64
        // TokenEstimator selects how the up-front reservation is computed:
        // "" or "fixed" reserve ReserveTokens for every request; "chars4"
        // estimates the prompt from message length (bytes/4, a documented
        // approximation of BPE tokenization for English text) and adds the
        // requested max_tokens, so long prompts reserve more than short
        // ones. The real usage is settled from the response either way.
        TokenEstimator string
        // Budgets maps tenant -> window token limit.
        Budgets map[string]int64
        // DefaultBudget applies to tenants absent from Budgets; 0 = unlimited.
        DefaultBudget int64
}

// Token estimator names, mirroring the GATEWAY_TOKEN_ESTIMATOR values.
const (
        TokenEstimatorFixed  = "fixed"
        TokenEstimatorChars4 = "chars4"
)

// LimitFor returns the tenant's window budget; 0 means unlimited.
func (o Options) LimitFor(tenant string) int64 {
        if limit, ok := o.Budgets[tenant]; ok {
                return limit
        }
        return o.DefaultBudget
}

// Metering enforces per-tenant token budgets and emits usage events for
// admitted requests. It must sit below auth (it needs the tenant) and below
// admission (only admitted requests are metered).
type Metering struct {
        opts  Options
        store BudgetStore
        sink  EventSink
        log   *slog.Logger
}

// New returns a Metering backed by the given store and sink. Nil sink
// disables event shipping; options are normalized (window >= 1ns, reserve
// tokens >= 1, estimator defaults to fixed when unset).
func New(opts Options, store BudgetStore, sink EventSink, log *slog.Logger) *Metering {
        if opts.Window <= 0 {
                opts.Window = 24 * time.Hour
        }
        if opts.ReserveTokens <= 0 {
                opts.ReserveTokens = 1024
        }
        if opts.TokenEstimator == "" {
                opts.TokenEstimator = TokenEstimatorFixed
        }
        if sink == nil {
                sink = NoopSink{}
        }
        if log == nil {
                log = slog.Default()
        }
        return &Metering{opts: opts, store: store, sink: sink, log: log}
}

// Middleware meters admitted requests: reserve -> proxy -> settle + emit.
func (m *Metering) Middleware(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                tenant, ok := auth.TenantFromContext(r.Context())
                if !ok {
                        apierror.Write(w, http.StatusInternalServerError, "api_error", "tenant_unresolved",
                                "tenant could not be resolved")
                        return
                }

                start := time.Now()
                limit := m.opts.LimitFor(tenant)
                reserved := m.reserveFor(r)

                if err := m.store.Reserve(r.Context(), tenant, reserved, limit); err != nil {
                        if errors.Is(err, ErrBudgetExhausted) {
                                retry := m.opts.windowResetIn(start)
                                w.Header().Set("Retry-After",
                                        strconv.FormatInt(int64((retry+time.Second-1)/time.Second), 10))
                                apierror.Write(w, http.StatusTooManyRequests, "rate_limit_error", "tenant_budget_exhausted",
                                        fmt.Sprintf("token budget exhausted for the current window, retry in %s", retry.Truncate(time.Second)))
                                m.emit(Usage{
                                        TenantID:   tenant,
                                        RequestID:  requestID(r),
                                        Status:     http.StatusTooManyRequests,
                                        Reason:     "budget_exhausted",
                                        DurationMS: elapsedMS(start),
                                        RecordedAt: time.Now(),
                                })
                                return
                        }
                        // Fail open: an unavailable budget store must not take inference down.
                        m.log.Warn("budget store unavailable, failing open", "tenant", tenant, "error", err)
                        reserved = 0
                }

                // Ask the upstream to report token usage in the final stream chunk.
                r = withUsageReporting(r)

                rec := &usageRecorder{ResponseWriter: w, status: http.StatusOK}
                next.ServeHTTP(rec, r)

                u := rec.usage()
                event := Usage{
                        TenantID:         tenant,
                        RequestID:        requestID(r),
                        Model:            u.Model,
                        PromptTokens:     u.PromptTokens,
                        CompletionTokens: u.CompletionTokens,
                        TotalTokens:      u.TotalTokens,
                        Streamed:         rec.streamed(),
                        Status:           rec.status,
                        DurationMS:       elapsedMS(start),
                        RecordedAt:       time.Now(),
                }
                if !rec.firstWrite.IsZero() {
                        event.TTFTMS = rec.firstWrite.Sub(start).Milliseconds()
                }
                m.settle(tenant, reserved, event)
                m.emit(event)
        })
}

// reserveFor computes the up-front token reservation for one request. With
// the fixed estimator every request reserves ReserveTokens; with chars4 the
// reservation is estimated from the request itself (prompt bytes/4 plus the
// requested max_tokens), so simultaneous long requests can no longer
// under-reserve against a budget. Requests whose body cannot be read or
// parsed fall back to the fixed reservation, and the body is always
// restored for the proxy unchanged.
func (m *Metering) reserveFor(r *http.Request) int64 {
        reserved := m.opts.ReserveTokens
        if m.opts.TokenEstimator != TokenEstimatorChars4 || r.Body == nil {
                return reserved
        }
        if ct := r.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
                return reserved
        }
        body, err := io.ReadAll(io.LimitReader(r.Body, maxEstimateBody+1))
        restored := r.Clone(r.Context())
        restored.Body = io.NopCloser(bytes.NewReader(body))
        if len(body) > maxEstimateBody {
                *r = *restored
                return reserved
        }
        if err != nil {
                *r = *restored
                return reserved
        }
        var payload struct {
                Messages []struct {
                        Content string `json:"content"`
                } `json:"messages"`
                MaxTokens *int64 `json:"max_tokens"`
        }
        if json.Unmarshal(body, &payload) != nil || len(payload.Messages) == 0 {
                *r = *restored
                return reserved
        }
        *r = *restored

        var est int64
        for _, msg := range payload.Messages {
                est += int64(len(msg.Content)) / 4
        }
        if payload.MaxTokens != nil && *payload.MaxTokens > 0 {
                est += *payload.MaxTokens
        } else {
                // No declared output bound: fall back to the configured
                // default output component.
                est += m.opts.ReserveTokens
        }
        if est < 1 {
                est = 1
        }
        return est
}

const maxEstimateBody = 4 << 20 // 4 MiB cap on bodies inspected for estimation

// settle adjusts the reservation to the real usage. When no usage block was
// parsed (e.g. the client disconnected mid-stream) the reservation is kept:
// upstream tokens were likely still burned, and disconnect storms
// must not mint free capacity. Upstream errors (status >= 400) refund.
func (m *Metering) settle(tenant string, reserved int64, u Usage) {
        actual := reserved
        if u.HasUsage() {
                if u.TotalTokens > 0 {
                        actual = u.TotalTokens
                } else {
                        actual = u.PromptTokens + u.CompletionTokens
                }
        } else if u.Status >= http.StatusBadRequest {
                actual = 0
        }
        if actual == reserved {
                return
        }
        // The request context may already be cancelled (client disconnect), so
        // settle on a fresh context.
        ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
        defer cancel()
        if err := m.store.Commit(ctx, tenant, reserved, actual); err != nil {
                m.log.Warn("usage settle failed", "tenant", tenant, "error", err)
        }
}

func (m *Metering) emit(u Usage) {
        if m.sink != nil {
                m.sink.Emit(u)
        }
}

// Close releases the store and the sink.
func (m *Metering) Close() error {
        var errs []error
        if m.store != nil {
                if err := m.store.Close(); err != nil {
                        errs = append(errs, err)
                }
        }
        if m.sink != nil {
                if err := m.sink.Close(); err != nil {
                        errs = append(errs, err)
                }
        }
        return errors.Join(errs...)
}

// windowResetIn reports how long until the current window bucket rolls over.
func (o Options) windowResetIn(now time.Time) time.Duration {
        w := o.Window
        if w <= 0 {
                w = 24 * time.Hour
        }
        secs := int64(w.Seconds())
        return time.Duration(secs-now.Unix()%secs) * time.Second
}

// requestID mirrors the access logger's scheme so events and log lines can
// be joined when the client sends X-Request-ID.
func requestID(r *http.Request) string {
        if id := r.Header.Get("X-Request-ID"); id != "" {
                return id
        }
        buf := make([]byte, 8)
        if _, err := rand.Read(buf); err != nil {
                return strconv.FormatInt(time.Now().UnixNano(), 16)
        }
        return hex.EncodeToString(buf)
}

func elapsedMS(start time.Time) int64 {
        return time.Since(start).Milliseconds()
}
