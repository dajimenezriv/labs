# Cache Stampede

- [What is a cache stampede?](#what-is-a-cache-stampede)
- [Why doesn't the hit ratio show it?](#why-doesnt-the-hit-ratio-show-it)
- [How does singleflight help?](#how-does-singleflight-help)
- [Why add a Redis lock on top?](#why-add-a-redis-lock-on-top)
- [What does stale-while-revalidate change?](#what-does-stale-while-revalidate-change)
- [What is probabilistic early expiration?](#what-is-probabilistic-early-expiration)
- [What if many keys expire at once?](#what-if-many-keys-expire-at-once)
- [Which fix fixes what?](#which-fix-fixes-what)
- [Observability](#observability)

## What is a cache stampede?

- A hot key expires and every request that misses recomputes it at the same time.
- Also called thundering herd or dog-piling.
- Load on the origin (DB, slow query) = `rate × recompute time` per expiry.
- `2000 req/s × 200ms = 400` loads of the same value, all at once.

```go
// Cache-aside. Correct code, and the whole bug.
v, err := rdb.Get(ctx, key).Result()
if err == nil {
  return v, nil
}
v, _ = origin(ctx) // 400 callers land here together
rdb.Set(ctx, key, v, 5*time.Second)
```

## Why doesn't the hit ratio show it?

- The stampede is short: it only lasts one recompute (200ms) every TTL (5s).
- With 2000 req/s, a 5s TTL and a 200ms origin: hit ratio ~97.8%, a healthy cache.
- But every request in that 200ms window waits for the origin: **p99 = origin latency**.
- The damage is on the origin: 400 concurrent loads of an expensive query can take it down.

## How does singleflight help?

- `golang.org/x/sync/singleflight`: concurrent calls with the same key share one execution.
- Only works **inside one process**. With 6 replicas, the floor is 6 loads per expiry, not 1.
- Callers still wait for the load, so p99 barely moves.

```go
var group singleflight.Group

v, err, _ := group.Do(key, func() (any, error) {
  return loadAndStore(ctx, key)
})
```

## Why add a Redis lock on top?

- Singleflight can't see the other replicas. A short Redis lock makes it one load for the whole fleet.
- The winner recomputes, the losers poll the cache until the value appears.
- TTL of the lock: a few times the recompute (`origin × 4`), so a dead winner doesn't block the key.
- **p99 still equals the origin latency**: waiting for a recompute costs the same as doing one.
- Release has the same bug as any lock: a plain `DEL` can free someone else's lock. See [distributed-lock.md](distributed-lock.md).

```go
won, _ := rdb.SetNX(ctx, "lock:"+key, token, 4*originLatency).Result()
if !won {
  return waitFor(ctx, key) // poll GET every few ms until the winner writes
}
defer release(ctx, "lock:"+key, token)
return loadAndStore(ctx, key)
```

## What does stale-while-revalidate change?

- The only fix that changes what the waiting requests do: **they don't wait**.
- Two TTLs: a **soft** deadline stored inside the value, a **hard** TTL on the key (several times longer).
- Past the soft deadline: serve the old value, and refresh in the background under the lock.
- Result: p99 drops from ~200ms to ~2ms, hit ratio ~100%.
- Cost: readers see data up to one refresh old. The gap between soft and hard TTL is how long refresh can keep failing before readers block again.

```go
type entry struct {
  Value     string `json:"value"`
  RefreshAt int64  `json:"refresh_at"` // soft deadline
}

if time.Now().UnixMilli() >= e.RefreshAt {
  go refresh(context.WithoutCancel(ctx), key) // under the Redis lock
}
return e.Value, nil // serve stale now
```

## What is probabilistic early expiration?

- Also called XFetch. Each reader may decide to recompute **before** expiry, with a probability that grows as expiry gets closer.
- Slower recomputes start earlier. Usually one reader refreshes and nobody sees a miss.
- No lock: store `delta` and `expiry` with the value. Same idea as stale-while-revalidate, decided randomly instead of by a soft TTL.

```go
// delta = how long the last recompute took, beta ≈ 1
if time.Now().Add(time.Duration(-delta * beta * math.Log(rand.Float64()))).After(expiry) {
  go recompute(key)
}
```

## What if many keys expire at once?

- Cache avalanche: keys written together (warm-up, deploy, batch job) with the same TTL expire together.
- **TTL jitter**: add random variance at write time, e.g. `ttl × (1 ± 0.2)`.
- Jitter spreads the loads over time. It **doesn't reduce them**: every key still expires once.
- With 500 keys: jitter cut peak concurrent loads 388 → 111, total loads stayed ~920.
- The lock cuts the work to one load per key (~920 → 500). Combine both.

```go
func ttl(base time.Duration, jitter float64) time.Duration {
  return time.Duration(float64(base) * (1 + jitter*(2*rand.Float64()-1)))
}
```

## Which fix fixes what?

| Fix                    | Reduces                        | Doesn't fix                          |
| ---------------------- | ------------------------------ | ------------------------------------ |
| Singleflight           | Loads per replica              | Loads across replicas, p99           |
| Redis lock             | Loads across the fleet (to 1)  | p99: losers still wait               |
| Stale-while-revalidate | p99: nobody waits              | Staleness, true misses (cold start)  |
| XFetch                 | Misses, without a lock         | Occasional duplicate recomputes      |
| TTL jitter             | Peak concurrent loads          | Total loads                          |

- Production answer: singleflight + short Redis lock + stale-while-revalidate, with jitter on the TTL.

## Observability

- The metric that shows it is **requests reaching the origin**, not the hit ratio.
- Hit ratio and p50 look healthy. The signal is in:
  - **Origin loads per second** spiking every TTL.
  - **Peak concurrent origin loads** (DB active connections, slow query log).
  - **p99 latency** matching the origin latency in a sawtooth with the TTL.
