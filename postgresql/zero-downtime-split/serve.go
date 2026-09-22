package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Where a request is served from.
const (
	monolith = "monolith" // the original database
	payments = "payments" // the new service's database
	shadow   = "shadow"   // read both, answer from the monolith, count the difference
)

type app struct {
	mono, pay *pgxpool.Pool

	// Writers hold this for reading for the duration of their write; the
	// freeze takes it for writing. That gives both halves of a quiesce in
	// one primitive: new writes block, and Lock does not return until the
	// in-flight ones have committed. It is the whole trick behind a
	// cutover that loses nothing.
	gate sync.RWMutex

	mu     sync.RWMutex
	writes string
	reads  string
	frozen bool

	ok, failed, mismatch atomic.Int64
}

func serve(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	monoDSN := fs.String("monolith", "postgres://postgres:postgres@localhost:5555/db", "monolith database")
	payDSN := fs.String("payments", "postgres://postgres:postgres@localhost:5556/db", "payments database")
	addr := fs.String("addr", ":8088", "listen address")
	fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a := &app{writes: monolith, reads: monolith}
	a.mono = mustReturn(pgxpool.New(ctx, *monoDSN))
	a.pay = mustReturn(pgxpool.New(ctx, *payDSN))
	defer a.mono.Close()
	defer a.pay.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /pay", a.handlePay)
	mux.HandleFunc("GET /pay", a.handleRead)
	mux.HandleFunc("POST /control", a.handleControl)
	mux.HandleFunc("GET /stats", a.handleStats)

	srv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()

	fmt.Fprintf(os.Stderr, "serving on %s\n", *addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		die(err)
	}
}

func (a *app) route() (writes, reads string) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.writes, a.reads
}

func (a *app) handlePay(w http.ResponseWriter, r *http.Request) {
	// Held, not rejected. A request that waits 400 ms is slow; a request
	// that gets a 503 is downtime. This is the line between the two.
	a.gate.RLock()
	defer a.gate.RUnlock()

	pool := a.mono
	if writes, _ := a.route(); writes != monolith {
		pool = a.pay
	}
	var id int64
	err := pool.QueryRow(r.Context(),
		`INSERT INTO lab.payments DEFAULT VALUES RETURNING id`).Scan(&id)
	if err != nil {
		a.failed.Add(1)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	a.ok.Add(1)
	fmt.Fprint(w, id)
}

func (a *app) handleRead(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)
	_, reads := a.route()

	count := func(p *pgxpool.Pool) (int, error) {
		var n int
		err := p.QueryRow(r.Context(),
			`SELECT count(*) FROM lab.payments WHERE id = $1`, id).Scan(&n)
		return n, err
	}

	var n int
	var err error
	switch reads {
	case monolith:
		n, err = count(a.mono)
	case payments:
		n, err = count(a.pay)
	case shadow:
		n, err = count(a.mono)
		if err == nil {
			if m, serr := count(a.pay); serr != nil || m != n {
				a.mismatch.Add(1)
			}
		}
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprint(w, n)
}

// POST /control?writes=&reads=&freeze=on|off
//
// freeze=on does not return until the write path is quiesced, so the caller
// knows that when it gets its response, nothing is in flight.
func (a *app) handleControl(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	if v := q.Get("freeze"); v != "" {
		a.mu.Lock()
		switch {
		case v == "on" && !a.frozen:
			a.frozen = true
			a.mu.Unlock()
			a.gate.Lock()
		case v == "off" && a.frozen:
			a.frozen = false
			a.mu.Unlock()
			a.gate.Unlock()
		default:
			a.mu.Unlock()
		}
	}

	a.mu.Lock()
	if v := q.Get("writes"); v != "" {
		a.writes = v
	}
	if v := q.Get("reads"); v != "" {
		a.reads = v
	}
	a.mu.Unlock()

	if q.Get("reset") != "" {
		a.ok.Store(0)
		a.failed.Store(0)
		a.mismatch.Store(0)
	}
	a.handleStats(w, r)
}

func (a *app) handleStats(w http.ResponseWriter, r *http.Request) {
	writes, reads := a.route()
	a.mu.RLock()
	frozen := a.frozen
	a.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"writes":   writes,
		"reads":    reads,
		"frozen":   frozen,
		"ok":       a.ok.Load(),
		"failed":   a.failed.Load(),
		"mismatch": a.mismatch.Load(),
	})
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

func die(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
