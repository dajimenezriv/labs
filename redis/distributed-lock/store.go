package main

import (
	"context"
	"sync"
	"time"
)

// guard is the lab's oracle: an in-process record of who took each job's lock
// last, and how many workers are inside the critical section right now.
// Nothing in production has this view — a service that could ask "am I still
// the holder?" and get a trustworthy answer would not need a fencing token in
// the first place. It exists here only to count what went wrong.
type guard struct {
	st   *stats
	mu   sync.Mutex
	jobs []*jobState
}

type jobState struct {
	// owner is the token of the most recent acquisition. A worker whose token
	// is not this one is stale, whatever Redis told it when it started.
	owner   string
	inside  int
	lastRun time.Time
	idle    time.Duration
}

func newGuard(n int, st *stats) *guard {
	g := &guard{st: st, jobs: make([]*jobState, n)}
	now := time.Now()
	for i := range g.jobs {
		g.jobs[i] = &jobState{lastRun: now}
	}
	return g
}

func (g *guard) enter(l *lease) {
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.jobs[l.job]
	if s.inside > 0 {
		// Two workers inside the same critical section. This is the failure
		// the lock exists to prevent, and no amount of Redis correctness
		// prevents it: the first worker was told it held the lock, and by the
		// time it stopped believing that, the second one was already in.
		g.st.overlap()
	}
	if s.inside == 0 {
		s.idle = max(s.idle, time.Since(s.lastRun))
	}
	s.inside++
	g.st.holding(1)
	s.owner = l.token
	s.lastRun = time.Now()
}

func (g *guard) leave(l *lease) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.jobs[l.job].inside--
	g.st.holding(-1)
}

func (g *guard) current(l *lease) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.jobs[l.job].owner == l.token
}

// gap reports the longest a job went without anybody running it, which is
// what a lock nobody released costs: the work does not stop being due, it just
// stops being done until the TTL lets somebody else in.
func (g *guard) gap(end time.Time) time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	var worst time.Duration
	for _, s := range g.jobs {
		worst = max(worst, s.idle)
		if s.inside == 0 {
			worst = max(worst, end.Sub(s.lastRun))
		}
	}
	return worst
}

// store is what the lock is protecting: the downstream write. It keeps the
// highest fence token it has seen per job, and under -fix fence it refuses
// anything older. That is the only defence in this lab that does not depend
// on the writer knowing it is stale, which is the only kind that survives a
// writer that is not running.
type store struct {
	mu    sync.Mutex
	fence map[int]int64
}

func newStore() *store {
	return &store{fence: map[int]int64{}}
}

func (s *store) write(ctx context.Context, w *worker, l *lease, pauseAt *time.Duration) {
	st, g := w.fleet.st, w.fleet.guard

	if l.lost.Load() {
		// The last check a careful worker can make: the renewer noticed the
		// lock was gone while the work was still running, so this write is
		// never issued. It catches a lot. It cannot catch anything that
		// happens after this line.
		st.abort()
		return
	}
	// The write is a round trip to another system, not an instant. Whether
	// the lock is still ours is decided when it lands, not when it is sent.
	if !w.elapse(ctx, writeTime, pauseAt) {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !g.current(l) {
		// The worker stopped holding the lock somewhere between acquiring it
		// and this write landing, and wrote anyway. Mutual exclusion has
		// already failed by this point, in every variant.
		st.stale()
	}
	// Out of order means a lower number than the store has already seen: this
	// write is undoing one that came after it. A stale write that gets here
	// first is not this — it is an old value that the current holder is about
	// to overwrite with a higher number, which is the order the fence exists
	// to keep.
	outOfOrder := l.fence < s.fence[l.job]
	s.fence[l.job] = max(s.fence[l.job], l.fence)

	switch {
	case outOfOrder && w.fleet.cfg.fix == fixFence:
		st.write("rejected")
	case outOfOrder:
		// A worker that no longer holds the lock just clobbered the work of
		// the one that does. This is the number the incident is about.
		st.write("corrupt")
	default:
		st.write("ok")
	}
}
