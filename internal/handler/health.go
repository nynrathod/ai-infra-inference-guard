package handler

import "net/http"

// Health reports liveness. It is unauthenticated by design so infrastructure
// probes (load balancers, Kubernetes) can reach it without credentials.
func Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
