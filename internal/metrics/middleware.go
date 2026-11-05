package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/nynrathod/inference-guard/internal/auth"
)

// Middleware instruments authenticated requests: it counts completions by
// tenant and status, tracks in-flight requests, and measures total request
// duration and time to first response byte. It must sit below the auth
// middleware so every request is attributed to a tenant. Any TTFT observer
// registered via SetTTFTObserver is notified as well.
func (m *Metrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant, _ := auth.TenantFromContext(r.Context())
		m.requestsInFlight.WithLabelValues(tenant).Inc()

		rec := &firstByteRecorder{ResponseWriter: w, status: http.StatusOK, start: time.Now()}
		defer func() {
			m.requestsInFlight.WithLabelValues(tenant).Dec()
			m.requestsTotal.WithLabelValues(tenant, strconv.Itoa(rec.status)).Inc()
			m.requestDuration.WithLabelValues(tenant).Observe(time.Since(rec.start).Seconds())
			if !rec.firstWrite.IsZero() {
				ttft := rec.firstWrite.Sub(rec.start)
				m.timeToFirstByte.WithLabelValues(tenant).Observe(ttft.Seconds())
				m.notifyTTFT(tenant, ttft)
			}
		}()

		next.ServeHTTP(rec, r)
	})
}

// firstByteRecorder wraps a ResponseWriter to capture the response status and
// the time the first response byte was written, while staying transparent to
// streaming (Flush) and response unwrapping.
type firstByteRecorder struct {
	http.ResponseWriter
	status     int
	start      time.Time
	firstWrite time.Time
}

func (r *firstByteRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *firstByteRecorder) Write(p []byte) (int, error) {
	if r.firstWrite.IsZero() {
		r.firstWrite = time.Now()
	}
	return r.ResponseWriter.Write(p)
}

func (r *firstByteRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *firstByteRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}
