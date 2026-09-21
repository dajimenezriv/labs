package main

// The four ways of holding the lock, in the order you would reach for them.
// Every one of them is SET NX PX; what changes is who is allowed to release
// it, whether the holder keeps it alive while it works, and whether anything
// downstream can tell a current holder from a stale one.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	fixNone  = "none"
	fixToken = "token"
	fixRenew = "renew"
	fixFence = "fence"
)

var fixes = []string{fixNone, fixToken, fixRenew, fixFence}

// releaseAny is the naive release: delete the key, whoever owns it. It is
// written as a script only so the lab can tell the three cases apart — ours,
// nobody's, or somebody else's. The DEL happens in all three, which is the
// bug: only the third one does any harm, and plain DEL cannot see it coming.
var releaseAny = redis.NewScript(`
	local v = redis.call("get", KEYS[1])
	redis.call("del", KEYS[1])
	if v == ARGV[1] then return 1 end
	if v == false then return 0 end
	return -1
`)

// releaseMine is the fix: compare and delete in one round trip, so a lock
// that is no longer ours is left alone for the worker that now holds it.
var releaseMine = redis.NewScript(`
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	end
	return 0
`)

// renewMine extends the lease, and only ours. PEXPIRE on a key we no longer
// hold would hand our successor's lock a deadline we chose.
var renewMine = redis.NewScript(`
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("pexpire", KEYS[1], ARGV[2])
	end
	return 0
`)

// lease is one acquisition of one job's lock.
type lease struct {
	job int
	key string
	// token identifies this acquisition, not this worker. A worker that takes
	// the same lock twice must not be able to release the second holder's
	// lock with the first holder's credentials.
	token string
	// fence is the monotonic number Redis hands out with the lock. It is the
	// only thing in this lab that travels with the write instead of staying
	// inside the worker that believes it holds the lock.
	fence int64
	// lost is set by the renewer when it finds the lock gone. A worker that
	// is still running knows to stop; a frozen worker never reads it.
	lost atomic.Bool
	stop func()
}

func (l *lease) stopRenewer() {
	if l.stop != nil {
		l.stop()
	}
}

// acquire is SET NX PX: set the key if nobody has it, with a deadline. A nil
// lease and a nil error means somebody else holds the lock.
func (w *worker) acquire(ctx context.Context, j int) (*lease, error) {
	buf := make([]byte, 8)
	rand.Read(buf)
	l := &lease{job: j, key: lockKey(j), token: hex.EncodeToString(buf)}

	won, err := w.rdb.SetNX(ctx, l.key, l.token, w.fleet.cfg.ttl).Result()
	if err != nil {
		return nil, ignoreShutdown(err)
	}
	if !won {
		w.fleet.st.acquire("contended")
		return nil, nil
	}
	w.fleet.st.acquire("acquired")

	// One INCR per acquisition, from the same Redis that hands out the lock.
	// Monotonic across workers and across a lock that timed out and was taken
	// by somebody else, which is the whole point. Every variant takes a
	// number, so the store can always tell afterwards which writes landed out
	// of order; only -fix fence lets it act on that while the write is
	// happening, which is the difference between measuring the damage and
	// preventing it.
	l.fence, err = w.rdb.Incr(ctx, fenceKey(j)).Result()
	if err != nil {
		return nil, ignoreShutdown(err)
	}
	if w.renews() {
		rctx, cancel := context.WithCancel(ctx)
		l.stop = cancel
		go w.renew(rctx, l)
	}
	return l, nil
}

func (w *worker) renews() bool {
	return w.fleet.cfg.fix == fixRenew || w.fleet.cfg.fix == fixFence
}

// renew keeps the lease alive for as long as the work takes, so the TTL no
// longer has to be a guess at the worst case. It is a goroutine in the
// worker's own process, which is exactly why it is not a safety mechanism:
// whatever stopped the worker stopped this too.
func (w *worker) renew(ctx context.Context, l *lease) {
	every := w.fleet.cfg.ttl / renewDivisor
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
		case <-ctx.Done():
			return
		}
		w.waitThaw(ctx)
		ttl := w.fleet.cfg.ttl.Milliseconds()
		ok, err := renewMine.Run(ctx, w.rdb, []string{l.key}, l.token, ttl).Int()
		if err != nil {
			return
		}
		if ok == 0 {
			// The lock is gone: it expired and somebody else has it. Renewal
			// found out, which is more than the naive version ever does.
			w.fleet.st.renew("lost")
			l.lost.Store(true)
			return
		}
		w.fleet.st.renew("ok")
	}
}

// release ends the critical section. Under -fix none it deletes whatever is
// there; from -fix token on it deletes only its own lock.
func (w *worker) release(ctx context.Context, l *lease) error {
	l.stopRenewer()
	ctx = context.WithoutCancel(ctx)

	if w.fleet.cfg.fix != fixNone {
		_, err := releaseMine.Run(ctx, w.rdb, []string{l.key}, l.token).Int()
		return ignoreShutdown(err)
	}
	held, err := releaseAny.Run(ctx, w.rdb, []string{l.key}, l.token).Int()
	if err != nil {
		return ignoreShutdown(err)
	}
	if held < 0 {
		// We just deleted a lock we did not hold. Whoever was inside the
		// critical section is still inside it, and the next worker to ask is
		// about to be let in as well.
		w.fleet.st.stolen()
	}
	return nil
}

func ignoreShutdown(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return err
}
