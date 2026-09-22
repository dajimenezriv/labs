package main

import (
	"context"
	"log/slog"
	"maps"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// This is layer one of two. It knows an address and nothing else: it cannot
// tell two customers behind one NAT apart, and it cannot tell a logged-in user
// from an anonymous one — which is exactly why the ratelimit package exists as
// well, running after authentication where the caller has a name. What this
// layer is good at is being cheap and being first: a flood is refused here,
// before it costs a goroutine, a database connection, or an argon2 hash.
//
// The state is a map in this process, not Redis, and that is the honest
// difference from the layer behind it: N replicas of the gateway would allow N
// times the rate. For an edge limit whose job is to blunt a flood rather than
// to meter anyone precisely, a limit that is loose by a factor of the replica
// count is still worth having, and it costs no network hop to enforce.

const (
	// idleTTL is how long a bucket outlives its owner's last request. An entry
	// is only safe to drop once its bucket has refilled, because whoever comes
	// back after that gets a fresh, full one — that takes burst/rate seconds,
	// which every zone here measures in single digits, so this is generous by
	// orders of magnitude. It is only a memory/forgetfulness tradeoff.
	idleTTL = 3 * time.Minute

	// sweepEvery bounds how often a request pays for the sweep below.
	sweepEvery = time.Minute
)

// nodelay and delay name what happens to a request the bucket cannot pay for
// right now, and are nginx's vocabulary: `burst=N nodelay` refuses on the spot,
// a plain `burst=N` makes it wait its turn.
const (
	nodelay = false
	delay   = true
)

// zone is one limit_req_zone: a rate, a burst, and one token bucket per
// address that has been seen lately.
type zone struct {
	limit rate.Limit
	burst int

	// queue is nginx's burst-without-nodelay. A person retyping a password is
	// not an attack, so on the credential routes an over-limit request waits
	// for its turn rather than being refused outright; on everything else
	// queuing a browser's parallel requests would make the site feel broken
	// instead of fast, so the excess is refused immediately.
	queue bool

	// retryAfter is the header value, precomputed: it depends only on the rate.
	retryAfter string

	log *slog.Logger

	mu        sync.Mutex
	buckets   map[string]*bucket
	lastSweep time.Time
}

type bucket struct {
	limiter *rate.Limiter
	seen    time.Time
}

func newZone(log *slog.Logger, limit rate.Limit, burst int, queue bool) *zone {
	// Retry-After is whole seconds, so anything refilling faster than that
	// rounds up to 1 — the smallest thing the header can say.
	seconds := math.Ceil(1 / float64(limit))

	return &zone{
		limit:      limit,
		burst:      burst,
		queue:      queue,
		retryAfter: strconv.Itoa(int(seconds)),
		log:        log,
		buckets:    make(map[string]*bucket),
	}
}

// wrap gives next this zone's per-address ceiling.
func (z *zone) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Keyed on the address we observed, never on X-Forwarded-For. That
		// header is a claim by whoever sent it, and this gateway forwards it
		// as-is, so keying on it would let a client pick its own bucket — and
		// a new one per request — with a header. nginx's $binary_remote_addr
		// is the same choice.
		limiter := z.limiterFor(remoteIP(r))

		if !z.queue {
			if !limiter.Allow() {
				z.reject(w, r)
			} else {
				next.ServeHTTP(w, r)
			}
			return
		}

		// The deadline is what makes this a queue of the burst's depth rather
		// than an unbounded one. The requests already waiting hold the tokens
		// they reserved, so the one after them is told its turn comes later
		// than this and is refused without ever blocking a goroutine.
		ctx, cancel := context.WithTimeout(r.Context(), z.maxWait())
		defer cancel()

		if err := limiter.Wait(ctx); err != nil {
			// The client hanging up mid-wait cancels the same context, and
			// there is nobody left to answer.
			if r.Context().Err() != nil {
				return
			}

			z.reject(w, r)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// maxWait is how long the queue is in seconds: a full burst ahead of you, at
// the rate they drain.
func (z *zone) maxWait() time.Duration {
	return time.Duration(float64(z.burst) / float64(z.limit) * float64(time.Second))
}

func (z *zone) reject(w http.ResponseWriter, r *http.Request) {
	// Logged at warn rather than error: a limiter doing its job is not an
	// outage, but it is worth seeing.
	z.log.WarnContext(r.Context(), "rate limited",
		"request_id", requestIDFrom(r.Context()),
		"path", r.URL.Path,
		"remote", r.RemoteAddr,
	)

	w.Header().Set("Retry-After", z.retryAfter)

	// 503 is nginx's default here and it is a lie: it says the server is broken
	// when the client was simply too quick. 429 is the answer that tells them
	// to slow down, and Retry-After is how long by.
	http.Error(w, `{"error":"too many requests"}`, http.StatusTooManyRequests)
}

// limiterFor returns key's bucket, creating it if this is the first time we
// have seen the address lately.
func (z *zone) limiterFor(key string) *rate.Limiter {
	now := time.Now()

	z.mu.Lock()
	defer z.mu.Unlock()

	z.sweep(now)

	b, ok := z.buckets[key]
	if !ok {
		b = &bucket{limiter: rate.NewLimiter(z.limit, z.burst)}
		z.buckets[key] = b
	}

	b.seen = now

	return b.limiter
}

// sweep drops the buckets nobody has used for a while, so the map does not
// grow with every address that has ever shown up — which is the leak a map
// keyed by client address is otherwise.
//
// Done on the way in, under the lock already held, rather than from a ticker
// of its own: no goroutine to start, none to stop at shutdown, and nothing
// running in a process that is handling nothing.
func (z *zone) sweep(now time.Time) {
	if now.Sub(z.lastSweep) < sweepEvery {
		return
	}

	z.lastSweep = now

	maps.DeleteFunc(z.buckets, func(_ string, b *bucket) bool {
		return now.Sub(b.seen) > idleTTL
	})
}
