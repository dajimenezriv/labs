package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
)

// upstream records what reached the service behind the proxy. Both counters
// are written by the upstream's goroutine and read by the test's.
type upstream struct {
	calls    atomic.Int64
	received atomic.Int64
}

func newTestProxy(t *testing.T) (http.Handler, *upstream) {
	t.Helper()

	var seen upstream

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.calls.Add(1)

		n, _ := io.Copy(io.Discard, r.Body)
		seen.received.Store(n)

		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	return &proxy{target: target, transport: http.DefaultTransport, log: testLogger(), maxBody: maxBodyBytes}, &seen
}

func TestProxyPassesBodiesUnderTheLimit(t *testing.T) {
	p, seen := newTestProxy(t)

	body := bytes.Repeat([]byte("a"), maxBodyBytes/2)

	r := httptest.NewRequest(http.MethodPost, "/identity/users", bytes.NewReader(body))
	w := httptest.NewRecorder()
	p.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", w.Code)
	}

	if seen.received.Load() != int64(len(body)) {
		t.Errorf("upstream received %d bytes, want %d", seen.received.Load(), len(body))
	}
}

func TestProxyRefusesDeclaredOversizedBodies(t *testing.T) {
	p, seen := newTestProxy(t)

	// bytes.Reader has a length, so the request carries a Content-Length.
	body := bytes.Repeat([]byte("a"), maxBodyBytes*2)

	r := httptest.NewRequest(http.MethodPost, "/identity/users", bytes.NewReader(body))
	w := httptest.NewRecorder()
	p.ServeHTTP(w, r)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("got %d, want 413", w.Code)
	}

	// The point of checking the declared length: identity never hears about
	// this request at all.
	if seen.calls.Load() != 0 {
		t.Errorf("upstream was called %d times for a body refused at the edge", seen.calls.Load())
	}
}

func TestProxyRefusesUndeclaredOversizedBodies(t *testing.T) {
	p, seen := newTestProxy(t)

	body := bytes.Repeat([]byte("a"), maxBodyBytes*2)

	// Wrapping the reader hides its length, which is the shape a chunked
	// request arrives in: nothing to check up front, so the cap can only be
	// enforced as the bytes are read.
	r := httptest.NewRequest(http.MethodPost, "/identity/users", io.NopCloser(bytes.NewReader(body)))
	if r.ContentLength != -1 {
		t.Fatalf("ContentLength is %d, want -1: this no longer tests the reader", r.ContentLength)
	}

	w := httptest.NewRecorder()
	p.ServeHTTP(w, r)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("got %d, want 413 — the client's body, not the upstream's fault", w.Code)
	}

	if seen.received.Load() > maxBodyBytes {
		t.Errorf("upstream received %d bytes, more than the %d cap", seen.received.Load(), maxBodyBytes)
	}
}
