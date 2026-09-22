package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	snapshotSQL = `SELECT key, enabled, updated_at FROM lab.flags`
	oneFlagSQL  = `SELECT key, enabled, updated_at FROM lab.flags WHERE key = $1`
	headSQL     = `SELECT coalesce(max(updated_at), to_timestamp(0)) FROM lab.flags`
)

// Field order matches snapshotSQL and oneFlagSQL for RowToStructByPos. A
// flag only ever enters the cache from one of those two queries -- never
// from a notification -- which is why there is nothing to decode here.
type Flag struct {
	Key       string
	Enabled   bool
	UpdatedAt time.Time
}

// One service instance's view of the flag table. The request path reads
// Enabled; nothing on the request path talks to Postgres.
type Cache struct {
	mu    sync.RWMutex
	flags map[string]Flag
	head  time.Time // newest updated_at this instance has applied

	// Every advance of a single key, with the local time it happened. The
	// latency table is a post-hoc join of this against when each flip was
	// issued, so nothing on the hot path has to know a measurement is
	// running. Per key rather than per head, because head is not a
	// staleness measure here: apply a notification for one key and head
	// moves past the updated_at of another key you never heard about.
	steps []step

	queries  atomic.Int64 // round trips to Postgres
	notifs   atomic.Int64 // notifications delivered
	resyncs  atomic.Int64 // full reloads
	dropouts atomic.Int64 // times the listening connection went away
}

type step struct {
	key       string
	updatedAt time.Time
	at        time.Time
}

func newCache() *Cache { return &Cache{flags: map[string]Flag{}} }

func (c *Cache) Enabled(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.flags[key].Enabled
}

func (c *Cache) apply(fs []Flag) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, f := range fs {
		// updated_at is wall clock, so "newer" is only as monotonic as the
		// server's clock. Fine at the rate a human flips flags; a sequence
		// would be the answer if two writes could land in the same
		// microsecond.
		if cur, ok := c.flags[f.Key]; !ok || f.UpdatedAt.After(cur.UpdatedAt) {
			c.flags[f.Key] = f
			c.steps = append(c.steps, step{f.Key, f.UpdatedAt, now})
		}
		if f.UpdatedAt.After(c.head) {
			c.head = f.UpdatedAt
		}
	}
}

// arrival returns when this instance first held key at updatedAt or newer,
// which is the honest definition of "no longer stale for this flag": a flag
// flipped twice in a row is not stale at the first write if the instance is
// already holding the second.
func (c *Cache) arrival(key string, updatedAt time.Time) (time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, s := range c.steps {
		if s.key == key && !s.updatedAt.Before(updatedAt) {
			return s.at, true
		}
	}
	return time.Time{}, false
}

func (c *Cache) Head() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.head
}

// stale counts the flags this instance is serving older than what is
// committed. This is the number that matters and it is not derivable from
// head.
func (c *Cache) stale(truth map[string]time.Time) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n := 0
	for k, t := range truth {
		if c.flags[k].UpdatedAt.Before(t) {
			n++
		}
	}
	return n
}

type querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func (c *Cache) load(ctx context.Context, q querier, sql string, args ...any) error {
	c.queries.Add(1)
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	fs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Flag])
	if err != nil {
		return err
	}
	c.apply(fs)
	return nil
}

// poll: no LISTEN at all. Every instance re-reads the whole table on a timer
// off the shared pool, so it costs no dedicated connection and the staleness
// window is the interval.
func (c *Cache) poll(ctx context.Context, pool *pgxpool.Pool, every time.Duration) {
	c.resyncs.Add(1)
	c.load(ctx, pool, snapshotSQL)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.resyncs.Add(1)
			c.load(ctx, pool, snapshotSQL)
		}
	}
}

// listen: a dedicated session that LISTENs, with the reconnect loop everybody
// writes. resync decides the one thing that separates the two versions of
// this function -- whether coming back means re-reading the table, or only
// re-subscribing to what happens next.
func (c *Cache) listen(ctx context.Context, dsn string, pool *pgxpool.Pool, resync bool, watchdog time.Duration) {
	if resync && watchdog > 0 {
		go c.watch(ctx, pool, watchdog)
	}

	first := true
	backoff := 500 * time.Millisecond

	for ctx.Err() == nil {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			sleep(ctx, backoff)
			continue
		}

		// LISTEN before the snapshot, never after. Between the two there is
		// a window in which a change is neither in the rows already read nor
		// in a subscription that exists yet, and a change that falls in it
		// is lost with no trace on either side.
		if _, err := conn.Exec(ctx, "LISTEN flags"); err != nil {
			conn.Close(ctx)
			sleep(ctx, backoff)
			continue
		}
		if first || resync {
			c.resyncs.Add(1)
			c.load(ctx, conn, snapshotSQL)
			first = false
		}

		for {
			n, err := conn.WaitForNotification(ctx)
			if err != nil {
				break
			}
			c.notifs.Add(1)
			// The payload named a key; the table says what that key is now.
			// Deliberately not "what the notification said it became": this
			// read may return a row newer than the change that triggered
			// it, and that is the correct answer rather than a race. It is
			// also why a notification that arrives late, twice or out of
			// order costs a wasted lookup and nothing else.
			c.load(ctx, conn, oneFlagSQL, n.Payload)
		}

		conn.Close(context.WithoutCancel(ctx))
		if ctx.Err() == nil {
			c.dropouts.Add(1)
			sleep(ctx, backoff)
		}
	}
}

// watch is the admission that delivery is not a guarantee. One indexed
// lookup on the shared pool, slowly: it asks whether anything exists past
// what this instance holds, and reloads if so. It runs on the pool rather
// than the listening connection on purpose -- it has to still work in
// exactly the situations where that connection is the broken thing.
func (c *Cache) watch(ctx context.Context, pool *pgxpool.Pool, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.queries.Add(1)
			var head time.Time
			if pool.QueryRow(ctx, headSQL).Scan(&head) != nil {
				continue
			}
			if head.After(c.Head()) {
				c.resyncs.Add(1)
				c.load(ctx, pool, snapshotSQL)
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
