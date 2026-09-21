package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The exec modes differ in how they bound parameters ($1).
const query = `SELECT count(*) FROM lab.accounts WHERE balance > $1`
const minBalance = 500

// cache_statement, pgx's default, PARSEs under a generated name and BINDs to
// that name on later calls -- which only holds if the two land on the same
// server connection. That used to make it unusable behind pgbouncer in
// transaction mode; pgbouncer >= 1.21 tracks the names itself and it is fine
// now. describe_exec is the one that is still not, because its Describe and
// Execute are separate exchanges with no name for pgbouncer to track.
var execModes = map[string]pgx.QueryExecMode{
	"cache_statement": pgx.QueryExecModeCacheStatement,
	"cache_describe":  pgx.QueryExecModeCacheDescribe,
	"describe_exec":   pgx.QueryExecModeDescribeExec,
	"exec":            pgx.QueryExecModeExec,
	"simple":          pgx.QueryExecModeSimpleProtocol,
}

func main() {
	dsn := flag.String("dsn", "postgres://postgres:postgres@localhost:5555/db", "connection string")
	workers := flag.Int("workers", 100, "concurrent callers")
	pool := flag.Int("pool", 0, "pgxpool MaxConns; 0 means no pool (connect per query)")
	dur := flag.Duration("duration", 10*time.Second, "how long to run")
	execMode := flag.String("exec", "cache_statement", "pgx query exec mode")
	header := flag.Bool("header", false, "print the column header for the rows below and exit")
	flag.Parse()

	if *header {
		fmt.Println("pool\t| exec\t| qps\t| p50ms\t| p99ms\t| waitms (mean)\t| ok\t| err\t| codes")
		return
	}

	mode, ok := execModes[*execMode]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown -exec %q\n", *execMode)
		os.Exit(2)
	}

	cfg, err := pgxpool.ParseConfig(*dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	cfg.ConnConfig.DefaultQueryExecMode = mode
	if *pool > 0 {
		// The default is the greater of 4 or runtime.NumCPU(), which in my laptop is 16.
		cfg.MaxConns = int32(*pool)
		cfg.MinConns = 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	var (
		okCount, errCount atomic.Int64
		waitTotal         atomic.Int64 // nanoseconds
		mu                sync.Mutex
		lats              []time.Duration
		byCode            = map[string]int{}
	)

	record := func(w time.Duration, d time.Duration, err error) {
		waitTotal.Add(int64(w))
		if err != nil {
			errCount.Add(1)
			mu.Lock()
			byCode[codeOf(err)]++
			mu.Unlock()
			return
		}
		okCount.Add(1)
		mu.Lock()
		lats = append(lats, d)
		mu.Unlock()
	}

	var run func(context.Context) (time.Duration, time.Duration, error)

	if *pool > 0 {
		p, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer p.Close()
		run = func(ctx context.Context) (time.Duration, time.Duration, error) {
			t0 := time.Now()
			c, err := p.Acquire(ctx)
			wait := time.Since(t0)
			if err != nil {
				return wait, wait, err
			}
			defer c.Release()
			var n int64
			err = c.QueryRow(ctx, query, minBalance).Scan(&n)
			return wait, time.Since(t0), err
		}
	} else {
		run = func(ctx context.Context) (time.Duration, time.Duration, error) {
			t0 := time.Now()
			c, err := pgx.ConnectConfig(ctx, cfg.ConnConfig)
			wait := time.Since(t0)
			if err != nil {
				return wait, wait, err
			}
			defer c.Close(context.Background())
			var n int64
			err = c.QueryRow(ctx, query, minBalance).Scan(&n)
			return wait, time.Since(t0), err
		}
	}

	start := time.Now()
	var wg sync.WaitGroup
	for range *workers {
		wg.Go(func() {
			for ctx.Err() == nil {
				wait, total, err := run(ctx)
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					return
				}
				record(wait, total, err)
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)

	slices.Sort(lats)
	attempts := okCount.Load() + errCount.Load()

	// waitMs is time spent inside Acquire, or inside Connect when -pool 0: the
	// part of the latency the query is not responsible for.
	var waitMs float64
	if attempts > 0 {
		waitMs = float64(waitTotal.Load()) / float64(attempts) / 1e6
	}
	codes := "-"
	if len(byCode) > 0 {
		codes = fmt.Sprint(byCode)
	}

	fmt.Printf("%d\t| %s\t| %.0f\t| %.1f\t| %.1f\t| %.1f\t| %d\t| %d\t| %s\n",
		*pool, *execMode,
		float64(okCount.Load())/elapsed.Seconds(),
		pct(lats, 0.50), pct(lats, 0.99), waitMs,
		okCount.Load(), errCount.Load(), codes)
}

// SQLSTATE if Postgres said no, otherwise a short tag. The codes this lab is
// about: 53300 too_many_connections, 42P05 duplicate_prepared_statement,
// 26000 invalid_sql_statement_name.
func codeOf(err error) string {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "other"
}

func pct(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	return float64(sorted[int(p*float64(len(sorted)-1))]) / 1e6
}
