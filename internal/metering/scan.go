package metering

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	maxRewriteBody = 16 << 20 // 16 MiB cap on rewritten request bodies
	maxScanLine    = 64 << 10 // 64 KiB cap per SSE line
	maxScanBody    = 4 << 20  // 4 MiB cap for buffered non-stream bodies
)

// usagePayload carries the token counts reported upstream for one request.
// The JSON tags match the nested "usage" object of the OpenAI wire format,
// which vLLM reports in the final SSE chunk (stream_options.include_usage
// injected by withUsageReporting) and in the body of non-streaming calls.
type usagePayload struct {
	Model            string `json:"model"`
	PromptTokens     int64  `json:"prompt_tokens"`
	CompletionTokens int64  `json:"completion_tokens"`
	TotalTokens      int64  `json:"total_tokens"`
}

func (p usagePayload) counted() bool {
	return p.PromptTokens > 0 || p.CompletionTokens > 0 || p.TotalTokens > 0
}

// parseUsage extracts the usage block from one chat-completion SSE chunk or
// the complete JSON body of a non-streaming call. Token counts live in the
// nested "usage" object and the model name at the top level; ok reports
// whether a counted usage block was present.
func parseUsage(data []byte) (usagePayload, bool) {
	var envelope struct {
		Model string        `json:"model"`
		Usage *usagePayload `json:"usage"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil || envelope.Usage == nil {
		return usagePayload{}, false
	}
	u := *envelope.Usage
	u.Model = envelope.Model
	if !u.counted() {
		return usagePayload{}, false
	}
	return u, true
}

// withUsageReporting rewrites streaming chat-completions requests so the
// upstream reports token usage in the final SSE chunk (OpenAI
// stream_options.include_usage): vLLM then appends a usage block the metering
// middleware parses to settle token budgets. Non-JSON, non-streaming,
// compressed and already-instrumented requests pass through untouched.
func withUsageReporting(r *http.Request) *http.Request {
	if r.Method != http.MethodPost || r.Body == nil {
		return r
	}
	if ct := r.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		return r
	}
	if r.Header.Get("Content-Encoding") != "" {
		return r
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRewriteBody+1))
	if err != nil {
		r.Body = io.NopCloser(bytes.NewReader(body))
		return r
	}
	restore := func() *http.Request {
		out := r.Clone(r.Context())
		out.Body = io.NopCloser(bytes.NewReader(body))
		return out
	}
	if len(body) > maxRewriteBody {
		return restore()
	}
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		return restore()
	}
	if stream, _ := payload["stream"].(bool); !stream {
		return restore()
	}
	opts, _ := payload["stream_options"].(map[string]any)
	if opts == nil {
		opts = make(map[string]any)
	}
	if include, _ := opts["include_usage"].(bool); include {
		return restore()
	}
	opts["include_usage"] = true
	payload["stream_options"] = opts
	rewritten, err := json.Marshal(payload)
	if err != nil {
		return restore()
	}
	out := restore()
	out.Body = io.NopCloser(bytes.NewReader(rewritten))
	out.ContentLength = int64(len(rewritten))
	out.Header.Del("Content-Length")
	return out
}

type scanMode int

const (
	scanDetect scanMode = iota // decide from Content-Type on first write
	scanSSE
	scanJSON
	scanNone
)

// usageRecorder wraps the ResponseWriter to capture the response status, the
// time of the first body byte (TTFT) and the usage block reported upstream,
// while staying transparent to streaming (Flush) and response unwrapping.
type usageRecorder struct {
	http.ResponseWriter
	status     int
	firstWrite time.Time

	mode scanMode
	line []byte // partial SSE line across writes
	last usagePayload
	body []byte // buffered JSON body (non-streaming)
}

func (r *usageRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *usageRecorder) Write(p []byte) (int, error) {
	if r.firstWrite.IsZero() {
		r.firstWrite = time.Now()
	}
	r.scan(p)
	return r.ResponseWriter.Write(p)
}

func (r *usageRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *usageRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

// usage returns the parsed usage block, if the upstream reported one.
func (r *usageRecorder) usage() usagePayload {
	if r.mode == scanJSON && len(r.body) > 0 {
		if up, ok := parseUsage(r.body); ok {
			r.last = up
		}
	}
	return r.last
}

// streamed reports whether the response was an SSE stream.
func (r *usageRecorder) streamed() bool { return r.mode == scanSSE }

func (r *usageRecorder) scan(p []byte) {
	if r.mode == scanDetect {
		r.mode = detectScanMode(r.Header().Get("Content-Type"))
	}
	switch r.mode {
	case scanSSE:
		for _, b := range p {
			if b == '\n' {
				r.handleLine(r.line)
				r.line = r.line[:0]
				continue
			}
			if len(r.line) < maxScanLine {
				r.line = append(r.line, b)
			}
		}
	case scanJSON:
		if room := maxScanBody - len(r.body); room > 0 {
			if len(p) > room {
				p = p[:room]
			}
			r.body = append(r.body, p...)
		}
	}
}

func detectScanMode(contentType string) scanMode {
	switch {
	case strings.Contains(contentType, "text/event-stream"):
		return scanSSE
	case strings.Contains(contentType, "application/json"):
		return scanJSON
	default:
		return scanNone
	}
}

func (r *usageRecorder) handleLine(line []byte) {
	data, ok := sseData(line)
	if !ok {
		return
	}
	if up, ok := parseUsage(data); ok {
		r.last = up
	}
}

// sseData extracts the payload of an SSE "data:" line, skipping empty lines
// and the [DONE] sentinel.
func sseData(line []byte) ([]byte, bool) {
	trimmed := bytes.TrimLeft(line, " \t")
	payload, ok := bytes.CutPrefix(trimmed, []byte("data:"))
	if !ok {
		return nil, false
	}
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
		return nil, false
	}
	return payload, true
}
