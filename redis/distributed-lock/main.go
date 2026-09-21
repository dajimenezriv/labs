package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

const (
	// One lock key per job. The job is the unit of mutual exclusion: whatever
	// it is that only one worker in the fleet may be doing at a time.
	lockFormat  = "lock:job:%02d"
	fenceFormat = "fence:job:%02d"
	redisAddr   = "localhost:6379"
	metricsAddr = ":9301"
	// How often the lease is renewed, as a fraction of the TTL. Three renewals
	// per TTL leaves room for two to be lost to a blip before the lock goes.
	renewDivisor = 3
	// The downstream write is a round trip to another system, not an instant.
	// Its length is the part of the critical section that sits after the last
	// check a worker can make, so it is what -fix renew cannot cover.
	writeTime = 50 * time.Millisecond
	// An injected pause outlasts the default TTL, which is the only property
	// of it that matters. The rates are how many faults a run is worth
	// looking at, not a dial with anything to say.
	pauseTime = 4 * time.Second
	pauseRate = 0.15
	crashRate = 0.02
)

func lockKey(j int) string  { return fmt.Sprintf(lockFormat, j) }
func fenceKey(j int) string { return fmt.Sprintf(fenceFormat, j) }

type config struct {
	label    string
	fix      string
	workers  int
	jobs     int
	work     time.Duration
	ttl      time.Duration
	pause    bool
	crash    bool
	duration time.Duration
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("distributed-lock", flag.ExitOnError)
	fix := fs.String("fix", fixNone, strings.Join(fixes, " | "))
	jobs := fs.Int("jobs", 8, "how many independently locked jobs the workers compete over")
	work := fs.Duration("work", 200*time.Millisecond, "how long one job takes inside the critical section")
	ttl := fs.Duration("ttl", 3*time.Second, "lock TTL, the lease a worker gets on acquire")
	pause := fs.Bool("pause", false, "freeze workers mid-job for longer than the lock TTL")
	crash := fs.Bool("crash", false, "kill workers while they hold the lock")
	duration := fs.Duration("duration", 20*time.Second, "app duration")
	report := fs.String("report", "row", "row | header")
	label := fs.String("label", "naive lock", "row label")
	fs.Parse(os.Args[1:])

	if *report == "header" {
		printHeader()
		return nil
	}

	cfg := config{
		label:    *label,
		fix:      *fix,
		workers:  6,
		jobs:     *jobs,
		work:     *work,
		ttl:      *ttl,
		pause:    *pause,
		crash:    *crash,
		duration: *duration,
	}

	if cfg.jobs < 1 {
		return errors.New("-jobs must be at least 1")
	}
	if !slices.Contains(fixes, cfg.fix) {
		return fmt.Errorf("-fix must be one of %s, got %q", strings.Join(fixes, ", "), cfg.fix)
	}

	st := newStats()
	serveMetrics(st)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()

	f, err := newFleet(ctx, cfg, st)
	if err != nil {
		return err
	}
	defer f.close()

	f.load(ctx)
	st.freeze()
	st.printRow(cfg, f.guard)
	return nil
}

type fleet struct {
	cfg     config
	st      *stats
	guard   *guard
	store   *store
	workers []*worker
}

// worker is one instance of the job runner. Separate client, separate
// connection pool: as far as Redis is concerned these are separate processes,
// which is what makes a lock necessary in the first place.
type worker struct {
	id    int
	rdb   *redis.Client
	fleet *fleet
	// When this worker is frozen until, as unix nanos. A stop-the-world pause
	// stops every goroutine in the process, so the lease renewer reads this
	// too; a renewer that kept ticking through the pause would be modelling a
	// separate process, not a garbage collector.
	thawAt atomic.Int64
}

func newFleet(ctx context.Context, cfg config, st *stats) (*fleet, error) {
	f := &fleet{cfg: cfg, st: st, guard: newGuard(cfg.jobs, st), store: newStore()}
	for i := range cfg.workers {
		// Stock client: default pool size, default timeouts. Nothing in this
		// lab is waiting out a Redis problem.
		rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
		if err := rdb.Ping(ctx).Err(); err != nil {
			f.close()
			return nil, err
		}
		f.workers = append(f.workers, &worker{id: i + 1, rdb: rdb, fleet: f})
	}
	// Clean keys, so no lock and no fence counter survives from the run
	// before: a leftover fence would reject this run's first writes.
	if err := f.workers[0].rdb.FlushDB(ctx).Err(); err != nil {
		f.close()
		return nil, err
	}
	return f, nil
}

func (f *fleet) close() {
	for _, w := range f.workers {
		w.rdb.Close()
	}
}

func (f *fleet) load(ctx context.Context) {
	var wg sync.WaitGroup
	for _, w := range f.workers {
		wg.Go(func() { w.run(ctx) })
	}
	wg.Wait()
}

// run is the worker loop: pick a job, try to take its lock, run it if you got
// it. Picking at random rather than round robin keeps the workers from lining
// up into a rotation where they never contend.
func (w *worker) run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := w.attempt(ctx, rand.IntN(w.fleet.cfg.jobs)); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "worker %d: %s\n", w.id, err)
			return
		}
	}
}

func (w *worker) attempt(ctx context.Context, j int) error {
	l, err := w.acquire(ctx, j)
	if err != nil || l == nil {
		if l == nil && err == nil {
			// Somebody else holds it. Back off a little instead of spinning:
			// a retry loop at full speed would make this lab a benchmark of
			// how fast Redis can say no.
			w.sleep(ctx, 5*time.Millisecond)
		}
		return err
	}
	return w.process(ctx, l)
}

// process is the critical section: the part the lock exists to protect. It
// ends with a write to the downstream store, which is where a worker that
// lost its lock does the damage. The guard is left before the lock is
// released, not after, because the section ends when the work does; the
// release round trip that follows is not part of it.
func (w *worker) process(ctx context.Context, l *lease) error {
	w.fleet.guard.enter(l)

	pauseAt := w.pauseAt()
	if w.elapse(ctx, w.fleet.cfg.work, &pauseAt) {
		if w.fleet.cfg.crash && rand.Float64() < crashRate {
			// The worker dies here, holding the lock: no write, no release,
			// and the renewer goes with it. Only the TTL gets this job back.
			w.fleet.st.crash()
			w.fleet.guard.leave(l)
			l.stopRenewer()
			return nil
		}
		w.fleet.store.write(ctx, w, l, &pauseAt)
	}

	w.fleet.guard.leave(l)
	return w.release(ctx, l)
}

// pauseAt picks where in the critical section this run freezes, or -1 for a
// run that does not. Uniform over the work and the downstream write together:
// a pause is not polite enough to wait for the work to finish, and the one
// that lands while the write is in flight is the one nothing can check for.
func (w *worker) pauseAt() time.Duration {
	cfg := w.fleet.cfg
	if !cfg.pause || rand.Float64() >= pauseRate {
		return -1
	}
	return time.Duration(rand.Int64N(int64(cfg.work + writeTime)))
}

// elapse spends d of the critical section, freezing partway through if this
// run's pause falls inside this stretch. A stop-the-world pause freezes the
// whole worker, renewer included: a GC, a hypervisor steal, a swapped-out
// page, a laptop lid. The cause does not matter, only that the process stops
// for longer than a lock TTL and Redis is not told.
func (w *worker) elapse(ctx context.Context, d time.Duration, pauseAt *time.Duration) bool {
	if *pauseAt < 0 || *pauseAt > d {
		*pauseAt -= d
		return w.sleep(ctx, d)
	}
	before := *pauseAt
	*pauseAt = -1
	if !w.sleep(ctx, before) {
		return false
	}
	w.fleet.st.pause()
	w.thawAt.Store(time.Now().Add(pauseTime).UnixNano())
	if !w.sleep(ctx, pauseTime) {
		return false
	}
	return w.sleep(ctx, d-before)
}

// waitThaw is how the renewer joins the pause.
func (w *worker) waitThaw(ctx context.Context) {
	for {
		d := time.Until(time.Unix(0, w.thawAt.Load()))
		if d <= 0 || !w.sleep(ctx, d) {
			return
		}
	}
}

// sleep reports whether it slept the whole way, so callers can tell a
// finished job from a run that ended underneath them.
func (w *worker) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func serveMetrics(st *stats) {
	reg := prometheus.NewRegistry()
	st.register(reg)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	go func() {
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			fmt.Fprintf(os.Stderr, "metrics: %s\n", err)
		}
	}()
}
