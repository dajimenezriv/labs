# Cache

## Where caches live

Client → CDN → gateway → in-process (L1) → distributed cache (L2, Redis) → DB buffer pool.

- A two-level setup (in-process map + Redis) is common.
- L1 introduces its own invalidation problem across replicas — usually solved with a short TTL or a pub/sub invalidation channel.

## Patterns

### Cache-aside

Lazy loading. App reads cache, misses, reads DB, writes cache.

- Can contain stale data.
- Cold start misses.

```go
// Reads cache.
if cached, ok := s.cachedShipment(ctx, *order.ShipmentID); ok {
  return cached
}
// Miss and reads DB.
shipment, err := s.carrier.Shipment(ctx, principal.AccessToken, *order.ShipmentID)
// Writes cache. Usually we do this synchronously since it's very fast (~0.2ms).
s.cacheShipment(ctx, *order.ShipmentID, shipment)
```

### Read-through

The only difference from cache-aside is who holds the DB handle. Here the handler holds a loader function. But in runtime they do the same.

However here we use singleflight, so we deduplicate every request that asks for the same key.

```go
type ReadThrough[T any] struct {
  cache *cache.Cache
  load  func(context.Context, string) (T, error)
  ttl   time.Duration
  group singleflight.Group
}

func (r *ReadThrough[T]) Get(ctx context.Context, key string) (T, error) {
  var zero T

  // Reads cache.
  if raw, ok, err := r.cache.Get(ctx, key); err == nil && ok {
    var v T
    if json.Unmarshal(raw, &v) == nil {
      // Return value from cache.
      return v, nil
    }
  }

  v, err, _ := r.group.Do(key, func() (any, error) {
    // Reads DB.
    v, err := r.load(ctx, key)
    if raw, err := json.Marshal(v); err == nil {
      // Writes cache.
      _ = r.cache.Set(ctx, key, raw, r.ttl)
    }
    return v, nil
  })
  if err != nil {
    return zero, err
  }
  return v.(T), nil
}
```

### Write-through

The cache synchronously writes the DB, then updates its own entry.

- Higher write latency.
- If 2 writes happens concurrently, maybe the first one sets the cache later and we have a stale value until the TTL.
- We can set keys that maybe are never read.

```go
func (w *WriteThrough[T]) Set(ctx context.Context, key string, v T) error {
  // Writes DB.
  err := w.store(ctx, key, v)
  raw, err := json.Marshal(v)
  // Writes cache.
  return w.cache.Set(ctx, key, raw, w.ttl)
}
```

### Write-behind

The write goes to the cache, returns immediately and a background worker flushes it to DB in batches.

- Faster writes.
- Risk of data-loss. Only acceptable where losing a second is fine.

```go
type WriteBehind struct {
  deltas chan Delta
  done   chan struct{}
}

func (w *WriteBehind) Incr(shipmentID uuid.UUID) {
  select {
  case w.deltas <- Delta{shipmentID, 1}:
  default: // full: drop rather than block the request
    observability.ObserveWriteBehind("dropped")
  }
}

func (w *WriteBehind) run(ctx context.Context) {
  defer close(w.done)
  tick := time.NewTicker(time.Second)
  defer tick.Stop()

  // The accumulator usually lives in Redis, otherwise in case of multiple replicas we
  // we are settings two different buffers.
  pending := map[uuid.UUID]int64{}
  for {
    select {
    case d := <-w.deltas:
      pending[d.ID] += d.N // coalescing: 1000 views → one UPDATE
      if len(pending) >= 500 {
        w.flush(ctx, pending)
        pending = map[uuid.UUID]int64{}
      }
    case <-tick.C:
      w.flush(ctx, pending)
      pending = map[uuid.UUID]int64{}
    case <-ctx.Done():
      w.flush(context.WithoutCancel(ctx), pending) // drain on shutdown
      return
    }
  }
}
```

### Refresh-ahead

Renew the entry before it expires, so nobody ever waits on a miss.

- Avoids miss latency.
- Wasted work on cold keys.

#### Stale-while-revalidate

Soft TTL under a hard TTL.

```go
type entry[T any] struct {
  Value     T         `json:"value"`
  RefreshAt time.Time `json:"refresh_at"`
}

// soft = 60s, hard = 300s
func (r *RefreshAhead[T]) Get(ctx context.Context, key string) (T, error) {
  e, ok := r.read(ctx, key)
  if !ok {
    return r.loadAndStore(ctx, key)   // true miss: block
  }

  if time.Now().After(e.RefreshAt) {
    detached := context.WithoutCancel(ctx)
    go r.group.Do(key, func() (any, error) {   // refresh in background
      return r.loadAndStore(detached, key)
    })
  }

  return e.Value, nil   // serve the stale value now, either way
}
```

#### Probabilistic early expiration (XFetch)

```go
// delta = how long the last recompute took, beta ≈ 1
if time.Now().Add(time.Duration(-delta * beta * math.Log(rand.Float64()))).After(expiry) {
  go recompute(key)
}

```

## Consistency — the part interviewers press on

There is no atomic transaction across Postgres and Redis, so every scheme is a race-condition analysis.

- **Delete, don't update** the cache on writes — updating races with concurrent writers.
- **Write DB first, then delete cache.** Understand the residual race: a reader that missed _before_ your write can write a stale value _after_ your delete.
  - Mitigations: short TTLs as a safety net, delayed double-delete, or versioned keys (`user:42:v7`).
- **Event-driven invalidation via Kafka**: services publish change events, a consumer invalidates keys. Also gives a story for read-your-writes.
- Say out loud that caching is an availability/latency win bought with **eventual consistency**, and name the TTL that bounds the staleness.

### Read-your-writes

Other users are allowed to see stale data, but the person who just clicked Save must not see their old value.

## The three classic failure modes

### mpede / thundering herd

A hot key expires and hundreds of goroutines hit Postgres at once.

- Fix: singleflight (`golang.org/x/sync/singleflight`), a per-key lock, or probabilistic early expiration.

### etration

Repeated lookups for keys that don't exist.

- Fix: negative caching with a short TTL, or a Bloom filter.

### lanche

Many keys expiring at once, or Redis going down.

- Fix: TTL jitter (small, random variance added to expiration times, this prevents a synchronization expiration "cache stampede"), staggered warm-up, and a circuit breaker so a Redis outage degrades to slow rather than down.

## Redis internals worth actually knowing

- **Single-threaded command execution** → one O(N) command blocks everyone. `KEYS` is forbidden; use `SCAN`. Watch for big keys and hot keys.
- **Expiration** is lazy + sampled active, not precise.
- **Eviction**: `maxmemory-policy` — `noeviction` vs `allkeys-lru` vs `allkeys-lfu` vs `volatile-*`. Redis LRU is _approximate_. Picking `noeviction` for a cache is a classic incident.
- **Persistence**: RDB vs AOF. Replication is **async** — a failover can lose acknowledged writes. Never treat Redis as the source of truth.
- **Topology**: Sentinel vs Cluster; 16384 hash slots, cross-slot restrictions, hash tags `{user:42}`.
- **Atomicity**: pipelining ≠ transactions. `MULTI`/`EXEC`, `WATCH` (optimistic locking), and **Lua scripts** for read-modify-write.
- **Distributed locks**: `SET key val NX PX ttl`, release via a compare-and-delete Lua script. Know why Redlock is contested (Kleppmann's critique, fencing tokens).
- **Data structures** beyond strings: Hash, Sorted Set (leaderboards, sliding-window rate limits), Set, Bitmap, HyperLogLog, Streams with consumer groups — and why Streams is not a Kafka replacement.

## Ops

- Track: hit ratio, `keyspace_misses`, `evicted_keys`, p99 latency, connection pool saturation.
- In Go with `go-redis`: pool size, read/write timeouts.
- Rule: a Redis error never fails the request — fall through to the DB.
