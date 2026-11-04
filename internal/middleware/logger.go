package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"time"

	"github.com/nynrathod/inference-guard/internal/auth"
)

// RequestLogger emits one structured log line per completed request and
// echoes the request ID to the client via the X-Request-ID response header.
func RequestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			id := requestID(r)
			w.Header().Set("X-Request-ID", id)
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(rec, r)

			attrs := []any{
				"request_id", id,
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"bytes", rec.bytes,
				"duration_ms", time.Since(start).Milliseconds(),
			}
			if tenant, ok := auth.TenantFromContext(r.Context()); ok {
				attrs = append(attrs, "tenant", tenant)
			}
			log.Info("request", attrs...)
		})
	}
}

// statusRecorder captures response status and byte count for logging while
// staying transparent to streaming (Flush) and response unwrapping.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(p []byte) (int, error) {
	n, err := r.ResponseWriter.Write(p)
	r.bytes += n
	return n, err
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func requestID(r *http.Request) string {
	if id := r.Header.Get("X-Request-ID"); id != "" {
		return id
	}
	buf := make([]byte, 8)
	rand.Read(buf)
	return hex.EncodeToString(buf)
}
