package main

// The four ways of reading the key, in the order you would reach for them.
// Every one of them is the same cache-aside idea; what changes is how many
// callers are allowed to be recomputing at once, and whether the ones that are
// not recomputing have to wait for the one that is.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	fixNone         = "none"
	fixSingleflight = "singleflight"
	fixLock         = "lock"
	fixStale        = "stale"
)

var fixes = []string{fixNone, fixSingleflight, fixLock, fixStale}

func (r *replica) get(ctx context.Context, k string) (string, error) {
	switch r.fleet.cfg.fix {
	case fixSingleflight:
		return r.getSingleflight(ctx, k)
	case fixLock:
		return r.getLock(ctx, k)
	case fixStale:
		return r.getStale(ctx, k)
	default:
		return r.getNaive(ctx, k)
	}
}

// getNaive is cache-aside written the way it is usually written: read the
// cache, and on a miss compute the value and put it back. There is nothing
// wrong with this code. It is correct, it is what the pattern says to do, and
// it is the whole of the bug.
func (r *replica) getNaive(ctx context.Context, k string) (string, error) {
	v, ok, err := r.peek(ctx, k)
	if err != nil || ok {
		return v, err
	}
	return r.loadAndStore(ctx, k)
}

// getSingleflight collapses the concurrent misses inside one replica into one
// call. It cannot see the other replicas, so the floor is one load per replica
// per expiry, not one load.
func (r *replica) getSingleflight(ctx context.Context, k string) (string, error) {
	v, ok, err := r.peek(ctx, k)
	if err != nil || ok {
		return v, err
	}
	return r.once(ctx, k, func() (string, error) { return r.loadAndStore(ctx, k) })
}

// getLock adds the agreement singleflight cannot reach: one short lock in
// Redis, so exactly one replica in the whole fleet recomputes. The losers wait
// for the winner to publish and then read it.
//
// The release here is a plain DEL. Deleting a lock whose TTL already expired,
// and which somebody else therefore now holds, is the bug that lab 2 is about;
// it does not bite at these timings because the lock outlives the recompute.
func (r *replica) getLock(ctx context.Context, k string) (string, error) {
	v, ok, err := r.peek(ctx, k)
	if err != nil || ok {
		return v, err
	}
	return r.once(ctx, k, func() (string, error) {
		lock := "lock:" + k
		won, err := r.rdb.SetNX(ctx, lock, r.id, r.fleet.cfg.origin*4).Result()
		if err != nil {
			return "", err
		}
		if !won {
			return r.waitFor(ctx, k)
		}
		defer r.rdb.Del(ctx, lock)
		return r.loadAndStore(ctx, k)
	})
}

// entry is the value under a soft TTL: still servable, but due to be renewed.
type entry struct {
	Value     string `json:"value"`
	RefreshAt int64  `json:"refresh_at"`
}

// getStale is the only one of the four that changes what a request waiting on
// a recompute does, which is to not wait. The key is written with a hard TTL
// several times the soft one, so an expiry means "this is due for renewal",
// not "this is gone". A reader past the soft deadline kicks off the renewal
// and returns the value it already has.
func (r *replica) getStale(ctx context.Context, k string) (string, error) {
	raw, ok, err := r.peek(ctx, k)
	if err != nil {
		return "", err
	}
	if !ok {
		// A true miss: nothing to serve, so this one does have to wait. After
		// the warm-up it only happens if the hard TTL runs out, which means
		// renewal has been failing for several soft periods.
		return r.once(ctx, k, func() (string, error) { return r.loadAndStore(ctx, k) })
	}

	var e entry
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		return "", err
	}
	if time.Now().UnixMilli() >= e.RefreshAt {
		// Renew behind the request, under the same fleet-wide lock, so one
		// replica recomputes and nobody blocks on it.
		r.fleet.wg.Go(func() { r.refresh(ctx, k) })
	}
	return e.Value, nil
}

func (r *replica) refresh(ctx context.Context, k string) {
	if _, err := r.getLockedLoad(ctx, k); err != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "replica %d: refresh %s: %s\n", r.id, k, err)
	}
}

// getLockedLoad is the recompute half of getLock, without the cache read in
// front of it: the caller already knows the value is due.
func (r *replica) getLockedLoad(ctx context.Context, k string) (string, error) {
	return r.once(ctx, k, func() (string, error) {
		lock := "lock:" + k
		won, err := r.rdb.SetNX(ctx, lock, r.id, r.fleet.cfg.origin*4).Result()
		if err != nil || !won {
			return "", err
		}
		defer r.rdb.Del(ctx, lock)
		return r.loadAndStore(ctx, k)
	})
}

// once is this replica's singleflight. Callers that arrive while a recompute
// for the same key is already running here share its result instead of
// starting another.
func (r *replica) once(ctx context.Context, k string, fn func() (string, error)) (string, error) {
	if r.fleet.cfg.fix == fixNone {
		return fn()
	}
	v, err, _ := r.sf.Do(k, func() (any, error) { return fn() })
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

// peek is the cache read, and the only place hits and misses are counted.
func (r *replica) peek(ctx context.Context, k string) (string, bool, error) {
	v, err := r.rdb.Get(ctx, k).Result()
	switch {
	case err == nil:
		r.fleet.st.hit()
		return v, true, nil
	case errors.Is(err, redis.Nil):
		r.fleet.st.miss()
		return "", false, nil
	default:
		return "", false, err
	}
}

// waitFor is what a caller that lost the lock does: poll until the winner
// publishes. This is the step that keeps the tail latency exactly where the
// naive version had it, because waiting for a recompute costs the same as
// doing one.
func (r *replica) waitFor(ctx context.Context, k string) (string, error) {
	deadline := time.Now().Add(r.fleet.cfg.origin * 4)
	for time.Now().Before(deadline) {
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			return "", ctx.Err()
		}
		v, err := r.rdb.Get(ctx, k).Result()
		if err == nil {
			return v, nil
		}
		if !errors.Is(err, redis.Nil) {
			return "", err
		}
	}
	// The winner died or is slower than its own lock. Somebody still has to
	// answer this request.
	return r.loadAndStore(ctx, k)
}

func (r *replica) loadAndStore(ctx context.Context, k string) (string, error) {
	v, err := r.fleet.origin(ctx)
	if err != nil {
		return "", err
	}
	return v, r.store(ctx, k, v)
}

func (r *replica) store(ctx context.Context, k, v string) error {
	ttl := r.fleet.ttl()
	if r.fleet.cfg.fix != fixStale {
		return r.rdb.Set(ctx, k, v, ttl).Err()
	}
	raw, err := json.Marshal(entry{Value: v, RefreshAt: time.Now().Add(ttl).UnixMilli()})
	if err != nil {
		return err
	}
	// Soft deadline in the value, hard deadline on the key. The gap between
	// them is how long renewal may keep failing before readers start blocking.
	return r.rdb.Set(ctx, k, raw, ttl*staleFactor).Err()
}

// ttl is where jitter is applied: at write time, per key. Applying it anywhere
// else would not spread anything, because what has to be spread is the moment
// each key expires.
func (f *fleet) ttl() time.Duration {
	if f.cfg.jitter == 0 {
		return f.cfg.ttl
	}
	return time.Duration(float64(f.cfg.ttl) * (1 + f.cfg.jitter*(2*rand.Float64()-1)))
}
