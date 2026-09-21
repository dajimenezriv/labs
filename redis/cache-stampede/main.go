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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

const (
	// The cached value: a rollup for one site, the kind of thing that is
	// expensive to compute and read on every dashboard load.
	keyFormat   = "site:%04d:rollup"
	redisAddr   = "localhost:6379"
	metricsAddr = ":9300"
	// How much longer the key survives than its soft deadline, under -fix
	// stale. Renewal has this many soft periods to keep failing before readers
	// start blocking again.
	staleFactor = 4
)

func keyFor(i int) string { return fmt.Sprintf(keyFormat, i) }

type config struct {
	label    string
	fix      string
	replicas int
	keys     int
	jitter   float64
	rate     float64
	origin   time.Duration
	ttl      time.Duration
	duration time.Duration
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("cache-stampede", flag.ExitOnError)
	fix := fs.String("fix", fixNone, strings.Join(fixes, " | "))
	keys := fs.Int("keys", 1, "how many cached keys the load is spread over")
	jitter := fs.Float64("jitter", 0, "random fraction of the TTL added to or taken off each write")
	rate := fs.Float64("rate", 2000, "requests per second, all replicas together")
	duration := fs.Duration("duration", 9*time.Second, "app duration")
	origin := fs.Duration("origin", 200*time.Millisecond, "how long one recompute takes")
	report := fs.String("report", "row", "row | header")
	label := fs.String("label", "naive cache-aside", "row label")
	fs.Parse(os.Args[1:])

	if *report == "header" {
		printHeader()
		return nil
	}

	cfg := config{
		label:    *label,
		duration: *duration,
		ttl:      5 * time.Second,
		fix:      *fix,
		replicas: 6,
		keys:     *keys,
		jitter:   *jitter,
		rate:     *rate,
		origin:   *origin,
	}

	if cfg.keys < 1 {
		return errors.New("-keys must be at least 1")
	}
	if !slices.Contains(fixes, cfg.fix) {
		return fmt.Errorf("-fix must be one of %s, got %q", strings.Join(fixes, ", "), cfg.fix)
	}

	st := newStats()
	serveMetrics(st)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fleet, err := newFleet(ctx, cfg, st)
	if err != nil {
		return err
	}
	defer fleet.close()

	// Warm every key before the clock starts. Otherwise the cold start is
	// itself a stampede, in every variant, and it would be counted as one of
	// the TTL expiries this lab is about. Written straight in rather than
	// through the origin: this is a cache that a previous steady state, a
	// deploy or a backfill has already filled.
	if err := fleet.warm(ctx); err != nil {
		return err
	}
	st.reset()

	fleet.load(ctx)
	st.freeze()
	st.printRow(cfg)
	return nil
}

// origin stands in for the slow read: a rollup query, a fan-out to another
// service, anything that takes long enough that requests arrive while it runs.
// Fixed latency, on purpose. A variable one would make every herd a different
// size and there would be nothing to compare.
func (f *fleet) origin(ctx context.Context) (string, error) {
	f.st.originStart()
	defer f.st.originEnd()
	select {
	case <-time.After(f.cfg.origin):
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return fmt.Sprintf("rollup@%d", time.Now().UnixMilli()), nil
}

type fleet struct {
	cfg      config
	st       *stats
	replicas []*replica
	// Holds both the in-flight requests and the background renewals started
	// under -fix stale, so a run cannot end with a recompute still running.
	wg sync.WaitGroup
}

// replica is one instance of the service. Separate client, separate connection
// pool: as far as Redis is concerned these are separate processes, which is
// what makes the replica count a real variable rather than a loop bound.
type replica struct {
	id    int
	rdb   *redis.Client
	fleet *fleet
	// This replica's singleflight, and the reason the fix ladder has a floor
	// at one load per replica: it is a map in this process and knows nothing
	// about the other eleven.
	sf singleflight.Group
}

func newFleet(ctx context.Context, cfg config, st *stats) (*fleet, error) {
	f := &fleet{cfg: cfg, st: st}
	for i := range cfg.replicas {
		// Stock client: default pool size, default timeouts. Nothing in this
		// lab is waiting out a Redis problem.
		rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
		if err := rdb.Ping(ctx).Err(); err != nil {
			f.close()
			return nil, err
		}
		f.replicas = append(f.replicas, &replica{id: i + 1, rdb: rdb, fleet: f})
	}
	// Clean keys, so the first TTL is this run's and not the previous run's.
	if err := f.replicas[0].rdb.FlushDB(ctx).Err(); err != nil {
		f.close()
		return nil, err
	}
	return f, nil
}

func (f *fleet) warm(ctx context.Context) error {
	for i := range f.cfg.keys {
		if err := f.replicas[0].store(ctx, keyFor(i), "warm"); err != nil {
			return err
		}
	}
	return nil
}

func (f *fleet) close() {
	for _, r := range f.replicas {
		r.rdb.Close()
	}
}

// load sends requests at a fixed arrival rate and does not wait for them. The
// rate is the independent variable: if the generator held a fixed number of
// workers instead, a stalled request would stop the next one from being sent,
// the arrival rate would collapse exactly when the herd forms, and the p99
// would come out looking fine. That is coordinated omission, and it hides the
// only thing this lab measures.
func (f *fleet) load(ctx context.Context) {
	defer f.wg.Wait()

	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()

	start := time.Now()
	last := start
	var due float64
	for now := range tick.C {
		if now.Sub(start) >= f.cfg.duration {
			return
		}
		due += f.cfg.rate * now.Sub(last).Seconds()
		last = now

		for ; due >= 1; due-- {
			// Round-robin would line the replicas up with the ticker and give
			// every replica the same arrival pattern. Real requests land on
			// whichever instance the load balancer picked.
			r := f.replicas[rand.IntN(len(f.replicas))]
			k := keyFor(rand.IntN(f.cfg.keys))
			sent := now
			f.wg.Go(func() {
				if _, err := r.get(ctx, k); err != nil && ctx.Err() == nil {
					fmt.Fprintf(os.Stderr, "replica %d: %s\n", r.id, err)
					return
				}
				f.st.latency(time.Since(sent))
			})
		}
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
