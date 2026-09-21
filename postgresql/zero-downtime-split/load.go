package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// The workload. Every order is paid exactly once, by exactly one worker, so
// the order id is the identity of the write: an id in acked.txt that has no
// payment row afterwards is a write the service acknowledged and lost.
//
// Each successful write is immediately read back through the same API. That
// read is the one the migration breaks -- a client that writes and then
// reads its own write is the first to notice that reads and writes are
// pointed at two different databases.
func load(args []string) {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	base := fs.String("url", "http://localhost:8088", "service base url")
	workers := fs.Int("workers", 16, "concurrent clients")
	rate := fs.Int("rate", 400, "target writes per second")
	dur := fs.Duration("duration", 60*time.Second, "how long to run")
	first := fs.Int64("first-order", 200001, "first unpaid order id")
	last := fs.Int64("last-order", 500000, "last order id that exists")
	timeout := fs.Duration("timeout", 3*time.Second, "per-request client timeout")
	out := fs.String("acked", "out/acked.txt", "file to write acknowledged order ids to")
	fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *dur)
	defer cancel()

	os.MkdirAll("out", 0o755)
	f, err := os.Create(*out)
	if err != nil {
		die(err)
	}
	defer f.Close()
	acked := bufio.NewWriter(f)
	defer acked.Flush()

	var (
		next    atomic.Int64
		ok      atomic.Int64
		failed  atomic.Int64
		rawMiss atomic.Int64 // acknowledged, then not visible to the very next read
		mu      sync.Mutex
		lats    []time.Duration
		errs    = map[string]int{}
	)
	next.Store(*first)

	// The client timeout is the definition of downtime here: a write held
	// longer than this is an error to the caller no matter what the
	// service thinks it is doing.
	client := &http.Client{Timeout: *timeout}

	// Paced, not saturated. A service running flat out has no headroom to
	// show, and the point of the cutover measurements is what happens to
	// latency and errors when the migration perturbs a normal load.
	tick := time.NewTicker(time.Second / time.Duration(*rate))
	defer tick.Stop()

	post := func(ctx context.Context, url string) (int, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
		if err != nil {
			return 0, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, nil
	}

	var wg sync.WaitGroup
	for range *workers {
		wg.Go(func() {
			for ctx.Err() == nil {
				select {
				case <-tick.C:
				case <-ctx.Done():
					return
				}

				// Every order is paid once, so the ids are consumed rather
				// than reused. Running off the end of the seeded range would
				// produce foreign key violations that have nothing to do
				// with the migration, so the run stops instead.
				id := next.Add(1)
				if id > *last {
					return
				}
				amount := (id%900 + 100) * 10

				t0 := time.Now()
				code, err := post(ctx, fmt.Sprintf("%s/pay?order_id=%d&amount=%d", *base, id, amount))
				d := time.Since(t0)
				if ctx.Err() != nil {
					return
				}

				mu.Lock()
				lats = append(lats, d)
				switch {
				case err != nil:
					errs[classify(err)]++
				case code >= 300:
					errs["http "+strconv.Itoa(code)]++
				}
				mu.Unlock()

				if err != nil || code >= 300 {
					failed.Add(1)
					continue
				}
				ok.Add(1)
				mu.Lock()
				fmt.Fprintln(acked, id)
				mu.Unlock()

				// Read your own write, through the same API, right now.
				// A read that never got an answer because the run ended is
				// not a miss, so the check is dropped once ctx is done.
				if n := readBack(ctx, client, *base, id); n < 1 && ctx.Err() == nil {
					rawMiss.Add(1)
				}
			}
		})
	}

	// One line per second, so the cutover window is visible in the output
	// rather than averaged away by the summary.
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		start := time.Now()
		var lastOK, lastFail, lastMiss int64
		var cursor int
		fmt.Println("t\tok/s\terr/s\trawmiss/s\tp99ms")
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				o, fl, ms := ok.Load(), failed.Load(), rawMiss.Load()
				mu.Lock()
				window := slices.Clone(lats[cursor:])
				cursor = len(lats)
				mu.Unlock()
				fmt.Printf("%.0f\t%d\t%d\t%d\t%.0f\n",
					time.Since(start).Seconds(), o-lastOK, fl-lastFail, ms-lastMiss,
					float64(pct(window, 0.99).Microseconds())/1000)
				lastOK, lastFail, lastMiss = o, fl, ms
			}
		}
	}()

	wg.Wait()
	<-done
	acked.Flush()

	mu.Lock()
	defer mu.Unlock()
	fmt.Println()
	fmt.Printf("acknowledged        %d\n", ok.Load())
	fmt.Printf("errors              %d\n", failed.Load())
	fmt.Printf("read-after-write    %d misses\n", rawMiss.Load())
	fmt.Printf("p50 / p99 / max     %.0f / %.0f / %.0f ms\n",
		msOf(pct(lats, 0.50)), msOf(pct(lats, 0.99)), msOf(pct(lats, 1)))
	for k, v := range errs {
		fmt.Printf("  %-18s%d\n", k, v)
	}
}

func readBack(ctx context.Context, c *http.Client, base string, id int64) int {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/pay?order_id=%d", base, id), nil)
	if err != nil {
		return -1
	}
	resp, err := c.Do(req)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	n, _ := strconv.Atoi(string(b))
	return n
}

func classify(err error) string {
	if os.IsTimeout(err) {
		return "timeout"
	}
	return "transport"
}

func pct(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := slices.Clone(d)
	slices.Sort(s)
	i := int(float64(len(s)-1) * p)
	return s[i]
}

func msOf(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
