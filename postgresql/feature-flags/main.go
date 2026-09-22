package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	dsn       = "postgres://postgres:postgres@localhost:5555/db"
	instances = 20
	gap       = time.Second
	maxFlips  = 30
)

type flip struct {
	key       string
	updatedAt time.Time
	at        time.Time
}

func main() {
	mode := flag.String("mode", "listen", "poll | listen")
	interval := flag.Duration("interval", 5*time.Second, "poll interval (-mode poll)")
	idle := flag.Bool("idle", false, "issue no flag changes; measures the standing cost of the mechanism")
	kill := flag.Duration("kill", 0, "terminate every listening backend this often; 0 never")
	settle := flag.Duration("settle", 0, "wait this long after the last change before the final staleness count")
	header := flag.Bool("header", false, "print the column header and exit")
	flag.Parse()

	flips := maxFlips
	if *idle {
		flips = 0
	}

	if *header {
		fmt.Println("mode\t| every\t| p50ms\t| p99ms\t| maxms\t| missed\t| queries\t| notifs\t| backends\t| drops\t| stale\t| stale+settle")
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The shared pool every instance would already have for its own queries.
	// Polling rides on it, and so does the watchdog -- neither needs a
	// connection of its own, which is the whole difference from LISTEN.
	cfg := must(pgxpool.ParseConfig(dsn))
	cfg.MaxConns = 8
	cfg.ConnConfig.RuntimeParams["application_name"] = "flag-pool"
	pool := must(pgxpool.NewWithConfig(ctx, cfg))
	defer pool.Close()

	// Tagged so -kill can find exactly these sessions and nothing else.
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	listenConnDSN := dsn + sep + "application_name=flag-listener"
	must(pgx.ParseConfig(listenConnDSN))

	caches := make([]*Cache, instances)
	var wg sync.WaitGroup
	for i := range caches {
		c := newCache()
		caches[i] = c
		switch *mode {
		case "poll":
			wg.Go(func() { c.poll(ctx, pool, *interval) })
		case "listen":
			wg.Go(func() { c.listen(ctx, listenConnDSN) })
		default:
			fmt.Fprintf(os.Stderr, "unknown -mode %q\n", *mode)
			os.Exit(2)
		}
	}

	// Warm. Measuring propagation before every instance has a first
	// snapshot measures startup instead.
	head := dbHead(ctx, pool)
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		if warm(caches, head) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !warm(caches, head) {
		fmt.Fprintln(os.Stderr, "instances never warmed up")
		os.Exit(1)
	}

	if *kill > 0 {
		wg.Go(func() { killer(ctx, pool, *kill) })
	}

	// Sampled for the whole measurement rather than once, because the
	// number that matters for capacity is the peak, not whatever happened
	// to be open when somebody looked. Its own queries are deliberately not
	// counted in the queries column -- that column is what the mechanism
	// costs, not what watching it costs.
	var peak atomic.Int64
	wg.Go(func() { watchBackends(ctx, pool, &peak) })

	issued := make([]flip, 0, flips)
	for i := range flips {
		k := fmt.Sprintf("flag_%03d", rand.IntN(200)+1)
		at := time.Now()
		var v time.Time
		err := pool.QueryRow(ctx,
			`UPDATE lab.flags SET enabled = NOT enabled WHERE key = $1 RETURNING updated_at`, k).Scan(&v)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		issued = append(issued, flip{k, v, at})
		if i < flips-1 {
			time.Sleep(gap)
		}
	}

	// A grace period long enough for a delivered notification to be applied
	// and far too short for any watchdog to fire, so the "stale" column is
	// what delivery alone achieved.
	time.Sleep(time.Second)
	stale := staleInstances(caches, dbFlags(ctx, pool))

	staleAfter := "-"
	if *settle > 0 {
		time.Sleep(*settle)
		staleAfter = fmt.Sprint(staleInstances(caches, dbFlags(ctx, pool)))
	}

	cancel()
	wg.Wait()

	var lats []time.Duration
	missed := 0
	for _, f := range issued {
		for _, c := range caches {
			if t, ok := c.arrival(f.key, f.updatedAt); ok {
				lats = append(lats, t.Sub(f.at))
			} else {
				missed++
			}
		}
	}
	slices.Sort(lats)

	var queries, notifs, drops int64
	for _, c := range caches {
		queries += c.queries.Load()
		notifs += c.notifs.Load()
		drops += c.dropouts.Load()
	}

	label := *mode
	if *idle {
		label += " idle"
	}

	every := "-"
	switch *mode {
	case "poll":
		every = interval.String()
	}

	fmt.Printf("%s\t| %s\t| %.1f\t| %.1f\t| %.1f\t| %d\t| %d\t| %d\t| %d\t| %d\t| %d\t| %s\n",
		label, every, pct(lats, 0.50), pct(lats, 0.99), pct(lats, 1.0),
		missed, queries, notifs, peak.Load(), drops, stale, staleAfter)
}

// Instances serving at least one flag older than what is committed. Counted
// per instance rather than per flag, because one instance with one wrong kill
// switch is the incident.
func staleInstances(caches []*Cache, truth map[string]time.Time) int {
	n := 0
	for _, c := range caches {
		if c.stale(truth) > 0 {
			n++
		}
	}
	return n
}

func warm(caches []*Cache, head time.Time) bool {
	for _, c := range caches {
		if c.Head().Before(head) {
			return false
		}
	}
	return true
}

// What a network blip, a failover, a restarted pooler or a firewall reaping
// idle connections all look like from the client: the session is gone and
// nothing else is wrong.
func killer(ctx context.Context, pool *pgxpool.Pool, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
			                WHERE application_name = 'flag-listener' AND pid <> pg_backend_pid()`)
		}
	}
}

// Client backends on the database, sampled twice a second, keeping the
// maximum. LISTEN needs one session per instance and cannot share the pool,
// so this is the column where that shows up.
func watchBackends(ctx context.Context, pool *pgxpool.Pool, peak *atomic.Int64) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			var n int64
			if pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			                       WHERE datname = 'db' AND backend_type = 'client backend'`).Scan(&n) != nil {
				continue
			}
			if n > peak.Load() {
				peak.Store(n)
			}
		}
	}
}

func dbHead(ctx context.Context, pool *pgxpool.Pool) time.Time {
	var v time.Time
	if err := pool.QueryRow(ctx,
		`SELECT coalesce(max(updated_at), to_timestamp(0)) FROM lab.flags`).Scan(&v); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return v
}

func dbFlags(ctx context.Context, pool *pgxpool.Pool) map[string]time.Time {
	rows, err := pool.Query(ctx, `SELECT key, updated_at FROM lab.flags`)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer rows.Close()
	out := map[string]time.Time{}
	for rows.Next() {
		var k string
		var v time.Time
		rows.Scan(&k, &v)
		out[k] = v
	}
	return out
}

func pct(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	return float64(sorted[int(p*float64(len(sorted)-1))]) / 1e6
}

func must[T any](v T, err error) T {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return v
}
