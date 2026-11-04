package apierror

import (
	"encoding/json"
	"net/http"
)

// Body is the error envelope used across the gateway API. It matches the
// error format of OpenAI-compatible clients so SDKs surface gateway errors
// natively.
type Body struct {
	Error Detail `json:"error"`
}

// Detail carries the machine-readable parts of an API error.
type Detail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}

// Write responds with a JSON error in the gateway's canonical envelope.
func Write(w http.ResponseWriter, status int, errType, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(Body{
		Error: Detail{Message: message, Type: errType, Code: code},
	})
}
