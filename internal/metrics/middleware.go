package metrics

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/nynrathod/inference-guard/internal/auth"
)

// Middleware instruments authenticated requests: it counts completions by
// tenant and status, tracks in-flight requests, and measures total request
// duration, time to first response byte and — the signal the AIMD controller
// consumes — time to the first streamed *content token*. First byte is kept
// as a separate diagnostic: the first SSE frame can arrive before any
// generated text, and rejected requests (429 JSON bodies) produce a first
// byte but never a token, so token time is the honest latency signal.
// The middleware must sit below the auth middleware so every request is
// attributed to a tenant. Any TTFT observer registered via
// SetTTFTObserver is notified with the first-token duration only.
func (m *Metrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant, _ := auth.TenantFromContext(r.Context())
		m.requestsInFlight.WithLabelValues(tenant).Inc()

		rec := &firstTokenRecorder{ResponseWriter: w, status: http.StatusOK, start: time.Now(), detect: true}
		defer func() {
			m.requestsInFlight.WithLabelValues(tenant).Dec()
			m.requestsTotal.WithLabelValues(tenant, strconv.Itoa(rec.status)).Inc()
			m.requestDuration.WithLabelValues(tenant).Observe(time.Since(rec.start).Seconds())
			if !rec.firstWrite.IsZero() {
				m.timeToFirstByte.WithLabelValues(tenant).Observe(rec.firstWrite.Sub(rec.start).Seconds())
			}
			if !rec.firstToken.IsZero() {
				ttft := rec.firstToken.Sub(rec.start)
				m.timeToFirstToken.WithLabelValues(tenant).Observe(ttft.Seconds())
				m.notifyTTFT(tenant, ttft)
			}
		}()

		next.ServeHTTP(rec, r)
	})
}

const maxTokenScanLine = 64 << 10 // 64 KiB cap per buffered SSE line

// firstTokenRecorder wraps a ResponseWriter to capture the response status,
// the time of the first response byte (diagnostic) and the time of the first
// streamed content token (the controller's latency signal), while staying
// transparent to streaming (Flush) and response unwrapping.
//
// Token detection scans SSE "data:" frames for the first non-empty
// choices[0].delta.content (or reasoning_content for thinking models). The
// role-only chunk most servers emit first is skipped. Non-streaming JSON
// responses count as first-token at the first write of a 2xx body: the whole
// body arrives at once, so that instant is when the client can read the
// first token. Non-2xx responses never set firstToken, so rejections are
// excluded from the latency window by construction.
type firstTokenRecorder struct {
	http.ResponseWriter
	status     int
	start      time.Time
	firstWrite time.Time
	firstToken time.Time

	detect bool  // still deciding the response shape from Content-Type
	scan   bool  // still scanning for the first content token
	isSSE  bool  // response is an SSE stream
	line   []byte // partial SSE line across writes
}

func (r *firstTokenRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *firstTokenRecorder) Write(p []byte) (int, error) {
	if r.firstWrite.IsZero() {
		r.firstWrite = time.Now()
	}
	if r.firstToken.IsZero() {
		r.scanForToken(p)
	}
	return r.ResponseWriter.Write(p)
}

func (r *firstTokenRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *firstTokenRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

// scanForToken consumes p looking for the first content token. Once the
// token is found (or can never be found) scanning stops and further writes
// pass through untouched.
func (r *firstTokenRecorder) scanForToken(p []byte) {
	if r.detect {
		r.detect = false
		ct := r.Header().Get("Content-Type")
		switch {
		case bytes.Contains([]byte(ct), []byte("text/event-stream")):
			r.isSSE, r.scan = true, true
		case bytes.Contains([]byte(ct), []byte("application/json")):
			// Whole-body JSON: a 2xx body is readable content, anything
			// else (e.g. a 429 error object) is not a token.
			if r.status >= 200 && r.status < 300 {
				r.firstToken = r.firstWrite
			}
			r.scan = false
			return
		default:
			r.scan = false
			return
		}
	}
	if !r.scan || !r.isSSE {
		return
	}
	for _, b := range p {
		if b != '\n' {
			if len(r.line) < maxTokenScanLine {
				r.line = append(r.line, b)
			}
			continue
		}
		complete := r.line
		r.line = r.line[:0]
		if r.tokenInLine(complete) {
			r.firstToken = time.Now()
			r.scan = false
			return
		}
	}
}

// tokenInLine reports whether one SSE line carries the first content token.
func (r *firstTokenRecorder) tokenInLine(line []byte) bool {
	trimmed := bytes.TrimLeft(line, " \t")
	payload, ok := bytes.CutPrefix(trimmed, []byte("data:"))
	if !ok {
		return false
	}
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
		return false
	}
	var chunk struct {
		Choices []struct {
			Delta struct {
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(payload, &chunk); err != nil {
		return false // frames we do not understand cannot carry a token
	}
	if len(chunk.Choices) == 0 {
		return false
	}
	return chunk.Choices[0].Delta.Content != "" || chunk.Choices[0].Delta.ReasoningContent != ""
}
