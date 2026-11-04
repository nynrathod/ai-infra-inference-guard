package proxy

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/nynrathod/inference-guard/internal/apierror"
)

// Proxy forwards OpenAI-compatible requests to the upstream model server and
// streams responses back to the client without buffering.
type Proxy struct {
	upstream *url.URL
	client   *http.Client
	log      *slog.Logger
}

// New returns a Proxy that targets the given upstream base URL.
func New(upstream *url.URL, log *slog.Logger) *Proxy {
	return &Proxy{
		upstream: upstream,
		log:      log,
		// No timeout: model responses, especially streams, are long-lived.
		client: &http.Client{},
	}
}

// Handler returns the http.Handler that forwards requests upstream.
func (p *Proxy) Handler() http.Handler {
	return http.HandlerFunc(p.serveHTTP)
}

func (p *Proxy) serveHTTP(w http.ResponseWriter, r *http.Request) {
	resp, err := p.client.Do(p.upstreamRequest(r))
	if err != nil {
		p.fail(w, r, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	copyResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	p.stream(w, r, resp.Body)
}

func (p *Proxy) upstreamRequest(r *http.Request) *http.Request {
	out := r.Clone(r.Context())
	out.URL.Scheme = p.upstream.Scheme
	out.URL.Host = p.upstream.Host
	out.URL.Path = joinPath(p.upstream.Path, r.URL.Path)
	out.URL.RawPath = ""
	out.RequestURI = ""
	out.Host = p.upstream.Host
	removeHopHeaders(out.Header)
	setForwardedFor(out, r.RemoteAddr)
	return out
}

func (p *Proxy) stream(w http.ResponseWriter, r *http.Request, body io.Reader) {
	if _, err := io.Copy(&flushWriter{w: w}, body); err != nil && r.Context().Err() == nil {
		p.log.Warn("upstream stream interrupted",
			"method", r.Method, "path", r.URL.Path, "error", err)
	}
}

func (p *Proxy) fail(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return
	}
	p.log.Error("upstream request failed",
		"method", r.Method, "path", r.URL.Path, "error", err)
	apierror.Write(w, http.StatusBadGateway, "api_error", "upstream_unavailable",
		"upstream model server unavailable")
}

// flushWriter flushes after every write so server-sent events reach the
// client as the model produces them.
type flushWriter struct {
	w http.ResponseWriter
}

func (fw *flushWriter) Write(p []byte) (int, error) {
	n, err := fw.w.Write(p)
	if f, ok := fw.w.(http.Flusher); ok {
		f.Flush()
	}
	return n, err
}

var hopHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

func removeHopHeaders(h http.Header) {
	for _, name := range hopHeaders {
		h.Del(name)
	}
}

func copyResponseHeaders(dst, src http.Header) {
	for name, values := range src {
		for _, value := range values {
			dst.Add(name, value)
		}
	}
	removeHopHeaders(dst)
}

func setForwardedFor(out *http.Request, remoteAddr string) {
	clientIP, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return
	}
	if prior := out.Header.Get("X-Forwarded-For"); prior != "" {
		clientIP = prior + ", " + clientIP
	}
	out.Header.Set("X-Forwarded-For", clientIP)
}

func joinPath(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	default:
		return a + b
	}
}
