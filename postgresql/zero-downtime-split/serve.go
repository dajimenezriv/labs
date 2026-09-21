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

// Where a request is served from. The migration is a sequence of changes to
// these two values, and every interesting question in the lab is "what
// happens to in-flight requests at the moment one of them changes".
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

	// Fraction of writes, in percent, that return after the payment is
	// committed but before the order is updated. Stands in for the process
	// dying between two commits that used to be one -- see atomicity.sh.
	halfWrite atomic.Int64

	ok, failed, mismatch, halved atomic.Int64
}

func serve(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	monoDSN := fs.String("monolith", "postgres://postgres:postgres@localhost:5558/db", "monolith database")
	payDSN := fs.String("payments", "postgres://postgres:postgres@localhost:5559/db", "payments database")
	addr := fs.String("addr", ":8088", "listen address")
	fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a := &app{writes: monolith, reads: monolith}
	var err error
	if a.mono, err = pgxpool.New(ctx, *monoDSN); err != nil {
		die(err)
	}
	if a.pay, err = pgxpool.New(ctx, *payDSN); err != nil {
		die(err)
	}
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

// POST /pay?order_id=N&amount=N
//
// Paying an order touches two tables: it inserts a payment and marks the
// order paid. Note what the request carries. While both tables are in one
// database the amount could be read from lab.orders inside the transaction;
// once payments moves, that column is in another service's database, so the
// caller has to send it. The API contract changes before the data does.
func (a *app) handlePay(w http.ResponseWriter, r *http.Request) {
	orderID, _ := strconv.ParseInt(r.URL.Query().Get("order_id"), 10, 64)
	amount, _ := strconv.ParseInt(r.URL.Query().Get("amount"), 10, 64)
	if orderID == 0 {
		http.Error(w, "order_id required", http.StatusBadRequest)
		return
	}

	// Held, not rejected. A request that waits 400 ms is slow; a request
	// that gets a 503 is downtime. This is the line between the two.
	a.gate.RLock()
	defer a.gate.RUnlock()

	writes, _ := a.route()
	var err error
	if writes == monolith {
		err = a.payMonolith(r.Context(), orderID, amount)
	} else {
		err = a.paySplit(r.Context(), orderID, amount)
	}
	if err != nil {
		a.failed.Add(1)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	a.ok.Add(1)
	w.WriteHeader(http.StatusNoContent)
}

// One database, one transaction. Either both rows change or neither does,
// and nothing in the application has to think about it.
func (a *app) payMonolith(ctx context.Context, orderID, amount int64) error {
	tx, err := a.mono.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	ref := "svc-" + strconv.FormatInt(orderID, 10)
	if _, err := tx.Exec(ctx,
		`INSERT INTO lab.payments (order_id, amount_cents, provider_ref) VALUES ($1, $2, $3)`,
		orderID, amount, ref); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE lab.orders SET status = 'paid' WHERE id = $1`, orderID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Two databases, two commits, and no way to make them one. The payment is
// the authoritative record so it goes first; the order update is a second
// call that can fail on its own.
func (a *app) paySplit(ctx context.Context, orderID, amount int64) error {
	ref := "svc-" + strconv.FormatInt(orderID, 10)
	if _, err := a.pay.Exec(ctx,
		`INSERT INTO lab.payments (order_id, amount_cents, provider_ref) VALUES ($1, $2, $3)`,
		orderID, amount, ref); err != nil {
		return err
	}

	if n := a.halfWrite.Load(); n > 0 && orderID%100 < n {
		a.halved.Add(1)
		return nil // as if the process died here
	}

	_, err := a.mono.Exec(ctx,
		`UPDATE lab.orders SET status = 'paid' WHERE id = $1`, orderID)
	return err
}

// GET /pay?order_id=N -> number of payments recorded for that order.
//
// In shadow mode the monolith still answers and the new database is queried
// alongside it purely to be compared. That is the only way to find out what
// cutting reads over would have returned without cutting them over.
func (a *app) handleRead(w http.ResponseWriter, r *http.Request) {
	orderID, _ := strconv.ParseInt(r.URL.Query().Get("order_id"), 10, 64)
	_, reads := a.route()

	count := func(p *pgxpool.Pool) (int, error) {
		var n int
		err := p.QueryRow(r.Context(),
			`SELECT count(*) FROM lab.payments WHERE order_id = $1`, orderID).Scan(&n)
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

// POST /control?writes=&reads=&freeze=on|off&halfwrite=N
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

	if v := q.Get("halfwrite"); v != "" {
		n, _ := strconv.ParseInt(v, 10, 64)
		a.halfWrite.Store(n)
	}
	if q.Get("reset") != "" {
		a.ok.Store(0)
		a.failed.Store(0)
		a.mismatch.Store(0)
		a.halved.Store(0)
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
		"halved":   a.halved.Load(),
	})
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
