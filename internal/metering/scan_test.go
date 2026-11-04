package metering

import (
	"net/http/httptest"
	"testing"
)

func TestUsageRecorderParsesSplitSSEUsage(t *testing.T) {
	rec := &usageRecorder{ResponseWriter: httptest.NewRecorder(), status: 200}
	rec.Header().Set("Content-Type", "text/event-stream")
	rec.WriteHeader(200)

	chunks := []string{
		"data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n",
		"data: {\"model\":\"m\",\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3,",
		"\"total_tokens\":10}}\n\n",
		"data: [DONE]\n\n",
	}
	for _, c := range chunks {
		rec.Write([]byte(c))
	}

	u := rec.usage()
	if u.PromptTokens != 7 || u.CompletionTokens != 3 || u.TotalTokens != 10 || u.Model != "m" {
		t.Fatalf("usage = %+v, want 7/3/10 model m", u)
	}
	if !rec.streamed() {
		t.Error("streamed() = false, want true")
	}
	if rec.status != 200 {
		t.Errorf("status = %d, want 200", rec.status)
	}
	if rec.firstWrite.IsZero() {
		t.Error("firstWrite not captured")
	}
}

func TestUsageRecorderParsesJSONBody(t *testing.T) {
	rec := &usageRecorder{ResponseWriter: httptest.NewRecorder(), status: 200}
	rec.Header().Set("Content-Type", "application/json")
	rec.Write([]byte(`{"id":"x","choices":[{"message":{"content":"hi"}}],`))
	rec.Write([]byte(`"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`))

	u := rec.usage()
	if u.PromptTokens != 1 || u.CompletionTokens != 2 || u.TotalTokens != 3 {
		t.Fatalf("usage = %+v, want 1/2/3", u)
	}
	if rec.streamed() {
		t.Error("streamed() = true for a JSON body")
	}
}

func TestUsageRecorderIgnoresUnknownContentType(t *testing.T) {
	rec := &usageRecorder{ResponseWriter: httptest.NewRecorder(), status: 200}
	rec.Header().Set("Content-Type", "text/plain")
	rec.Write([]byte("plain text"))
	if u := rec.usage(); u.counted() {
		t.Fatalf("usage = %+v, want none", u)
	}
}
