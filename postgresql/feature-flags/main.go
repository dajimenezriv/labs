package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
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
	settle := flag.Duration("settle", time.Second, "wait this long after the last change before the final staleness count")
	header := flag.Bool("header", false, "print the column header and exit")
	flag.Parse()

	flips := maxFlips
	if *idle {
		flips = 0
	}

	if *header {
		fmt.Println("mode\t| every\t| p50ms\t| p99ms\t| maxms\t| missed\t| queries\t| notifs\t| backends\t| drops\t| stale")
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := mustReturn(pgxpool.ParseConfig(dsn))
	cfg.MaxConns = 8
	cfg.ConnConfig.RuntimeParams["application_name"] = "flag-pool"
	pool := mustReturn(pgxpool.NewWithConfig(ctx, cfg))
	defer pool.Close()

	// Tagged so -kill can find exactly these sessions and nothing else.
	listenConnDSN := dsn + "?application_name=flag-listener"
	mustReturn(pgx.ParseConfig(listenConnDSN))

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
			die(fmt.Errorf("unknown -mode %q\n", *mode))
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
		die(errors.New("instances never warmed up"))
	}

	if *kill > 0 {
		wg.Go(func() { killer(ctx, pool, *kill) })
	}

	var peak atomic.Int64
	wg.Go(func() { watchBackends(ctx, pool, &peak) })

	issued := make([]flip, 0, flips)
	for i := range flips {
		k := fmt.Sprintf("flag_%03d", rand.IntN(200)+1)
		at := time.Now()
		var v time.Time
		must(pool.QueryRow(ctx,
			`UPDATE lab.flags SET enabled = NOT enabled WHERE key = $1 RETURNING updated_at`, k).Scan(&v))
		issued = append(issued, flip{k, v, at})
		if i < flips-1 {
			time.Sleep(gap)
		}
	}

	time.Sleep(*settle)
	stale := staleInstances(ctx, caches, pool)

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

	fmt.Printf("%s\t| %s\t| %.1f\t| %.1f\t| %.1f\t| %d\t| %d\t| %d\t| %d\t| %d\t| %d\n",
		label, every, pct(lats, 0.50), pct(lats, 0.99), pct(lats, 1.0),
		missed, queries, notifs, peak.Load(), drops, stale)
}

// Instances serving at least one flag older than what is committed. Counted
// per instance rather than per flag, because one instance with one wrong kill
// switch is the incident.
func staleInstances(ctx context.Context, caches []*Cache, pool *pgxpool.Pool) int {
	rows := mustReturn(pool.Query(ctx, `SELECT key, updated_at FROM lab.flags`))
	defer rows.Close()
	truth := map[string]time.Time{}
	for rows.Next() {
		var k string
		var v time.Time
		rows.Scan(&k, &v)
		truth[k] = v
	}

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
	must(pool.QueryRow(ctx,
		`SELECT coalesce(max(updated_at), to_timestamp(0)) FROM lab.flags`).Scan(&v))
	return v
}

func pct(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	return float64(sorted[int(p*float64(len(sorted)-1))]) / 1e6
}

func mustReturn[T any](v T, err error) T {
	must(err)
	return v
}

func must(err error) {
	if err != nil {
		die(err)
	}
}

func die(err error, a ...any) {
	fmt.Fprintln(os.Stderr, err, a)
	os.Exit(1)
}
