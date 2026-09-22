package main

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// ok is the handler behind the limiter: reaching it is what "allowed" means.
var ok = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
})

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// do sends one request from addr and reports what the limiter did with it.
func do(h http.Handler, addr string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, "/identity/login", nil)
	r.RemoteAddr = addr

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	return w
}

func TestZoneNodelayRefusesTheOverflow(t *testing.T) {
	// A rate slow enough that nothing refills during the test, so what is
	// measured is the burst and only the burst.
	h := newZone(testLogger(), 1, 3, nodelay).wrap(ok)

	for i := range 3 {
		if got := do(h, "10.0.0.1:1234").Code; got != http.StatusOK {
			t.Fatalf("request %d within burst: got %d, want 200", i+1, got)
		}
	}

	res := do(h, "10.0.0.1:1234")
	if res.Code != http.StatusTooManyRequests {
		t.Fatalf("request past burst: got %d, want 429", res.Code)
	}

	// 429 and not 503: the client was too quick, the server is not broken.
	if got := res.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After: got %q, want %q", got, "1")
	}
}

func TestZoneDelayQueuesRatherThanRefusing(t *testing.T) {
	// 50/s is a token every 20ms, which is the wait the third request should
	// serve out rather than being refused.
	h := newZone(testLogger(), 50, 2, delay).wrap(ok)

	do(h, "10.0.0.1:1234")
	do(h, "10.0.0.1:1234")

	start := time.Now()

	res := do(h, "10.0.0.1:1234")
	if res.Code != http.StatusOK {
		t.Fatalf("request past burst: got %d, want 200 after a wait", res.Code)
	}

	if elapsed := time.Since(start); elapsed < 10*time.Millisecond {
		t.Errorf("served in %v: it was let through rather than queued", elapsed)
	}
}

func TestZoneDelayRefusesPastTheQueue(t *testing.T) {
	// The queue is the burst deep: 2 tokens plus 2 more within the 100ms
	// (burst/rate) a caller is willing to be held. Everything behind those is
	// told its turn comes too late and is refused on the spot.
	h := newZone(testLogger(), 20, 2, delay).wrap(ok)

	const requests = 10

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		codes = map[int]int{}
	)

	for range requests {
		wg.Add(1)

		go func() {
			defer wg.Done()

			code := do(h, "10.0.0.1:1234").Code

			mu.Lock()
			defer mu.Unlock()

			codes[code]++
		}()
	}

	wg.Wait()

	if codes[http.StatusOK] < 2 {
		t.Errorf("only %d served: the burst should never wait", codes[http.StatusOK])
	}

	if codes[http.StatusOK] == requests {
		t.Errorf("all %d served: the queue is unbounded", requests)
	}

	if codes[http.StatusOK]+codes[http.StatusTooManyRequests] != requests {
		t.Errorf("unexpected statuses: %v", codes)
	}
}

func TestZoneKeysOnRemoteAddr(t *testing.T) {
	h := newZone(testLogger(), 1, 1, nodelay).wrap(ok)

	// The gateway forwards X-Forwarded-For as it was given, so a client can
	// put anything in it. If the limiter keyed on that, a header would be
	// enough to get a fresh bucket for every request.
	first := httptest.NewRequest(http.MethodGet, "/identity/login", nil)
	first.RemoteAddr = "10.0.0.1:1111"
	first.Header.Set("X-Forwarded-For", "1.1.1.1")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, first)

	if w.Code != http.StatusOK {
		t.Fatalf("first request: got %d, want 200", w.Code)
	}

	second := httptest.NewRequest(http.MethodGet, "/identity/login", nil)
	second.RemoteAddr = "10.0.0.1:2222"
	second.Header.Set("X-Forwarded-For", "2.2.2.2")

	w = httptest.NewRecorder()
	h.ServeHTTP(w, second)

	if w.Code != http.StatusTooManyRequests {
		t.Errorf("same address, different X-Forwarded-For and port: got %d, want 429", w.Code)
	}

	// A different address really is a different caller.
	if got := do(h, "10.0.0.2:1111").Code; got != http.StatusOK {
		t.Errorf("different address: got %d, want 200", got)
	}
}

func TestZoneSweepEvictsIdleBuckets(t *testing.T) {
	z := newZone(testLogger(), 1, 1, nodelay)

	z.limiterFor("10.0.0.1")

	// Age the entry and the last sweep past their thresholds rather than
	// sleeping for minutes.
	z.buckets["10.0.0.1"].seen = time.Now().Add(-2 * idleTTL)
	z.lastSweep = time.Now().Add(-2 * sweepEvery)

	z.limiterFor("10.0.0.2")

	if _, found := z.buckets["10.0.0.1"]; found {
		t.Error("idle bucket kept: the map grows with every address ever seen")
	}

	if _, found := z.buckets["10.0.0.2"]; !found {
		t.Error("active bucket swept")
	}
}
