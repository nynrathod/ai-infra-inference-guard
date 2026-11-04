package middleware

import (
	"log/slog"
	"net/http"

	"github.com/nynrathod/inference-guard/internal/apierror"
)

// Recover converts panics in downstream handlers into 500 responses so a
// single bad request cannot terminate the process or leave a connection
// hanging without a response.
func Recover(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if v := recover(); v != nil {
					log.Error("panic recovered",
						"method", r.Method, "path", r.URL.Path, "panic", v)
					apierror.Write(w, http.StatusInternalServerError, "api_error", "internal_error",
						"internal server error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
