package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// services counts what reached each upstream, so a test can ask not only
// whether a request was forwarded but which of the two it went to.
type services struct {
	identity atomic.Int64
	shop     atomic.Int64
}

// newTestGateway puts the real routing table in front of a stand-in for each
// service.
func newTestGateway(t *testing.T) (http.Handler, *services) {
	t.Helper()

	var seen services

	serve := func(calls *atomic.Int64) *httptest.Server {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(server.Close)

		return server
	}

	identity := serve(&seen.identity)
	shop := serve(&seen.shop)

	h, err := NewHandler(Config{IdentityURL: identity.URL, EcommerceURL: shop.URL}, testLogger())
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	return h, &seen
}

// TestUnlistedMethodsNeverReachAService is the other half of the allowlist:
// the table names a verb per path, so a path being published is not the same
// as every verb on it being published. Refusing here is what keeps a wrong
// method from costing an upstream hop.
func TestUnlistedMethodsNeverReachAService(t *testing.T) {
	h, seen := newTestGateway(t)

	for _, route := range []struct {
		method string
		path   string
	}{
		// Read verbs on the write-only endpoints.
		{http.MethodGet, "/identity/login"},
		{http.MethodGet, "/identity/users"},
		{http.MethodGet, "/ecommerce/files"},
		// Write verbs on the read-only ones.
		{http.MethodPost, "/ecommerce/orders/9f1c/tracking"},
		{http.MethodDelete, "/ecommerce/orders/9f1c"},
		{http.MethodPost, "/identity/openapi.yaml"},
		// The avatar upload is a PUT, and the near miss is the likelier
		// mistake than a verb nothing uses.
		{http.MethodPost, "/identity/me/avatar"},
		// TODO: add 404 checks
	} {
		r := httptest.NewRequest(route.method, route.path, nil)
		r.RemoteAddr = "10.0.0.1:1234"

		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: got %d, want 405", route.method, route.path, w.Code)
		}
	}

	if calls := seen.identity.Load() + seen.shop.Load(); calls != 0 {
		t.Errorf("a service was reached %d times by methods the gateway does not publish", calls)
	}
}

// TestEachPathGoesToItsOwnService is what the gateway is for: one origin in
// front of two services, so a client that knows this one address can log in
// and then shop without being told there is more than one process back here.
func TestEachPathGoesToItsOwnService(t *testing.T) {
	h, seen := newTestGateway(t)

	// The order of cmd/shopper's flow: register, log in, browse, order, then
	// ask where the parcel got to.
	const orderID = "0198f0c1-6f1e-7c1a-9a3e-6f0b2d4c8e51"

	routes := []struct {
		method string
		path   string
		want   *atomic.Int64
	}{
		{http.MethodPost, "/identity/users", &seen.identity},
		{http.MethodPost, "/identity/login", &seen.identity},
		{http.MethodPost, "/identity/refresh", &seen.identity},
		{http.MethodPost, "/ecommerce/orders", &seen.shop},
		{http.MethodGet, "/ecommerce/orders/" + orderID, &seen.shop},
		{http.MethodGet, "/ecommerce/orders/" + orderID + "/tracking", &seen.shop},
		{http.MethodPost, "/identity/logout", &seen.identity},
		// Not part of the flow, but the only other published surface either
		// service has, and the reason neither needs a port of its own. Both
		// pairs are here because they are the one place a path says which
		// service it belongs to and the routing table has to agree.
		{http.MethodGet, "/ecommerce/docs", &seen.shop},
		{http.MethodGet, "/ecommerce/openapi.yaml", &seen.shop},
		{http.MethodGet, "/identity/docs", &seen.identity},
		{http.MethodGet, "/identity/openapi.yaml", &seen.identity},
	}

	for _, route := range routes {
		before := route.want.Load()

		r := httptest.NewRequest(route.method, route.path, nil)
		r.RemoteAddr = "10.0.0.3:1234"

		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		if w.Code != http.StatusOK {
			t.Errorf("%s %s: got %d, want 200", route.method, route.path, w.Code)
		}

		if route.want.Load() != before+1 {
			t.Errorf("%s %s: did not reach the service it is routed to", route.method, route.path)
		}
	}

	// Every request landed on exactly one service, which the per-route check
	// above cannot tell from one that landed on both.
	if got := seen.identity.Load() + seen.shop.Load(); got != int64(len(routes)) {
		t.Errorf("%d calls for %d requests", got, len(routes))
	}
}

// TestCredentialPathsGetTheTightZone checks the routing table, not the
// limiter: the exact paths have to win over the /identity/ prefix, or login
// would quietly be sitting behind the loose zone.
func TestCredentialPathsGetTheTightZone(t *testing.T) {
	h, _ := newTestGateway(t)

	// The tight zone is 2/s with a burst of 5, so the sixth waits half a
	// second for its token rather than being refused. The loose zone's burst
	// is 40, so the same six go straight through.
	send := func(path string) time.Duration {
		start := time.Now()

		for range 6 {
			r := httptest.NewRequest(http.MethodPost, path, nil)
			r.RemoteAddr = "10.0.0.1:1234"

			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)

			if w.Code != http.StatusOK {
				t.Fatalf("%s: got %d, want 200", path, w.Code)
			}
		}

		return time.Since(start)
	}

	if elapsed := send("/identity/logout"); elapsed > 300*time.Millisecond {
		t.Errorf("/identity/logout took %v: it is behind the tight zone", elapsed)
	}

	if elapsed := send("/identity/login"); elapsed < 300*time.Millisecond {
		t.Errorf("/identity/login took %v: it is behind the loose zone", elapsed)
	}
}

// TestEveryResponseCarriesItsRequestID is the half of the request id that
// makes the other half worth having: an id only the logs can see is one the
// caller cannot quote back.
func TestEveryResponseCarriesItsRequestID(t *testing.T) {
	h, _ := newTestGateway(t)

	send := func(path, sent string) http.Header {
		r := httptest.NewRequest(http.MethodPost, path, nil)
		r.RemoteAddr = "10.0.0.4:1234"
		if sent != "" {
			r.Header.Set("X-Request-ID", sent)
		}

		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		return w.Header()
	}

	if got := send("/ecommerce/orders", "").Get("X-Request-ID"); got == "" {
		t.Error("a proxied response carried no request id")
	}

	// A path the gateway does not serve is among the likeliest to be reported,
	// so it is the least affordable one to answer anonymously.
	if got := send("/nothing/here", "").Get("X-Request-ID"); got == "" {
		t.Error("a 404 carried no request id")
	}

	// The caller's id is kept rather than replaced, so what comes back matches
	// what a proxy in front of us would already have written down.
	const sent = "0198f0c1-6f1e-7c1a-9a3e-6f0b2d4c8e51"
	if got := send("/ecommerce/orders", sent).Get("X-Request-ID"); got != sent {
		t.Errorf("got %q, want the id the caller sent, %q", got, sent)
	}
}

// TestAvatarRouteCarriesMoreThanTheDefaultCap is the routing table agreeing
// with what identity accepts. The edge cap is the JSON-sized default on every
// route but this one, and an avatar is an image: capping it at the default
// would make every upload between the two limits unreachable, refused here
// for a size identity was willing to store.
func TestAvatarRouteCarriesMoreThanTheDefaultCap(t *testing.T) {
	h, seen := newTestGateway(t)

	// Over the default, under the avatar limit: the range the bug closed off.
	body := bytes.Repeat([]byte("a"), maxBodyBytes*2)

	r := httptest.NewRequest(http.MethodPut, "/identity/me/avatar", bytes.NewReader(body))
	r.RemoteAddr = "10.0.0.4:1234"

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("got %d, want 200: an avatar this size is one identity accepts", w.Code)
	}

	if seen.identity.Load() != 1 {
		t.Error("the upload never reached identity")
	}

	// Past what identity would take either, so the edge is still the one that
	// pays for it rather than the upstream.
	huge := bytes.Repeat([]byte("a"), maxAvatarBytes*2)

	r = httptest.NewRequest(http.MethodPut, "/identity/me/avatar", bytes.NewReader(huge))
	r.RemoteAddr = "10.0.0.4:1234"

	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("got %d, want 413", w.Code)
	}

	// The raised cap belongs to the one route that asked for it, and not to
	// the JSON endpoints sharing the same upstream.
	r = httptest.NewRequest(http.MethodPost, "/identity/users", bytes.NewReader(body))
	r.RemoteAddr = "10.0.0.4:1234"

	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("got %d, want 413: only the avatar route is allowed the larger body", w.Code)
	}

	if seen.identity.Load() != 1 {
		t.Errorf("identity was reached %d times, want 1: the refused bodies cost an upstream hop", seen.identity.Load())
	}
}
