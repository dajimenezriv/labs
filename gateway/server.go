package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
)

func NewHandler(cfg Config, log *slog.Logger) (http.Handler, error) {
	identityURL, err := url.Parse(cfg.IdentityURL)
	if err != nil {
		return nil, fmt.Errorf("parse identity url: %w", err)
	}

	ecommerceURL, err := url.Parse(cfg.EcommerceURL)
	if err != nil {
		return nil, fmt.Errorf("parse ecommerce url: %w", err)
	}

	// One transport, shared by both routes, because the connection pool lives on
	// the transport: a new one per request would open a new TCP connection per
	// request and pool nothing.
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			// Refuse quickly when an upstream is down rather than holding the
			// caller open. nginx's proxy_connect_timeout.
			Timeout: 2 * time.Second,
		}).DialContext,
		// The equivalent of nginx's `keepalive 32`: how many idle connections to
		// keep warm per upstream so a request need not pay for a handshake.
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		// Left to the client. Without this the transport adds its own
		// Accept-Encoding when the caller sent none and silently decompresses
		// the reply, which makes the gateway an editor of bodies rather than a
		// pipe for them.
		DisableCompression: true,
	}

	mux := http.NewServeMux()

	// Two zones, and which one a route gets is a routing decision, so it is
	// made here in the routing table rather than by a switch on paths inside
	// the limiter. Both are per-address; see ratelimit.go for what that is
	// worth and what it is not.
	//
	// The loose one is the general ceiling: a short spike is served
	// immediately and only sustained traffic above the rate is refused.
	loose := newZone(log, 20, 40, nodelay)

	// Credentials get their own, much tighter zone. A password guess costs an
	// argon2 hash — deliberately expensive — so the general limit is far too
	// loose to sit in front of one.
	tight := newZone(log, 2, 5, delay)

	identity := &proxy{identityURL, transport, log, maxBodyBytes}
	shop := &proxy{ecommerceURL, transport, log, maxBodyBytes}
	avatars := &proxy{identityURL, transport, log, maxAvatarBytes}

	for _, route := range []struct {
		pattern string
		zone    *zone
		to      *proxy
	}{
		{"POST /identity/users", tight, identity},
		{"POST /identity/login", tight, identity},
		{"POST /identity/refresh", tight, identity},
		{"POST /identity/logout", loose, identity},
		{"PUT /identity/me/avatar", tight, avatars},

		{"POST /ecommerce/orders", loose, shop},
		{"GET /ecommerce/orders", loose, shop},
		{"GET /ecommerce/orders/{uuid}", loose, shop},
		{"GET /ecommerce/orders/{uuid}/tracking", loose, shop},
		{"POST /ecommerce/files", loose, shop},
		{"POST /ecommerce/files/complete", loose, shop},

		{"GET /identity/docs", loose, identity},
		{"GET /identity/openapi.yaml", loose, identity},
		{"GET /ecommerce/docs", loose, shop},
		{"GET /ecommerce/openapi.yaml", loose, shop},
	} {
		mux.Handle(route.pattern, route.zone.wrap(route.to))
	}

	// Outermost first: the request id has to exist before the log line that reports it.
	return withRequestID(withAccessLog(mux, log)), nil
}

type contextKey struct{ name string }

var requestContextKey = &contextKey{"request-id"}

// withRequestID gives every request an id.
func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = uuid.NewString()
		}

		w.Header().Set("X-Request-ID", id)

		ctx := context.WithValue(r.Context(), requestContextKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func requestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestContextKey).(string)
	return id
}

// withAccessLog records how each request ended.
func withAccessLog(next http.Handler, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		log.InfoContext(r.Context(), "gateway request",
			"request_id", requestIDFrom(r.Context()),
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"bytes", rec.written,
			"remote", r.RemoteAddr,
			"duration", time.Since(start),
		)
	})
}

// statusRecorder remembers what was sent so the log line can report it. The
// ResponseWriter itself will not say afterwards.
type statusRecorder struct {
	http.ResponseWriter

	status  int
	written int64
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	n, err := r.ResponseWriter.Write(b)
	r.written += int64(n)
	return n, err
}

// Unwrap lets http.NewResponseController reach the real writer. Without it,
// wrapping here would quietly cost the handlers underneath the ability to
// flush or hijack.
func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

// requestTooLarge passes on what http.MaxBytesReader tells the writer when a
// body runs past its limit: the server stops expecting the rest of it and
// closes the connection instead of waiting for a body it has already refused.
//
// net/http finds this one by type assertion rather than through Unwrap, so a
// wrapper that does not forward it silently swallows the signal.
func (r *statusRecorder) requestTooLarge() {
	if w, ok := r.ResponseWriter.(interface{ requestTooLarge() }); ok {
		w.requestTooLarge()
	}
}
