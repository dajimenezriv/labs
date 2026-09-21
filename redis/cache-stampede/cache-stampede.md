# Cache Stampede

- [The setup](#the-setup)
- [Cache herd](#cache-herd)
- [Singleflight, Redis lock and Stale](#singleflight-redis-lock-and-stale)
- [TTL jitter](#ttl-jitter)
- [Observability](#observability)

```bash
docker compose up
```

## The setup

- **One hot key**: `site:0000:rollup`.
- **2000 requests/s** spread over **6 replicas** at random.
- **TTL of 5s** and **duration of 9s** so the lab runs quickly.

```go
v, err := r.rdb.Get(ctx, key).Result()
if err == nil {
    return v, nil
}
// Miss.
v, _ = r.fleet.origin(ctx)
r.rdb.Set(ctx, key, v, r.fleet.cfg.ttl)
```

## Cache herd

```bash
{
  go run . -report header
  go run .
} | column -t -s $'\t'
```

| origin | requests | hit % | origin loads | peak concurrent | p50   | p99   | max   |
| ------ | -------- | ----- | ------------ | --------------- | ----- | ----- | ----- |
| 25ms   | 17998    | 99.71 | 52           | 52              | 970µs | 2ms   | 28ms  |
| 50ms   | 17999    | 99.43 | 102          | 101             | 980µs | 2ms   | 53ms  |
| 100ms  | 17999    | 98.87 | 204          | 202             | 980µs | 101ms | 103ms |
| 200ms  | 17998    | 97.76 | 403          | 402             | 990µs | 202ms | 203ms |
| 400ms  | 17998    | 95.56 | 800          | 800             | 990µs | 402ms | 405ms |

We are doing `2000 req/s`. When the hot key is expired, the origin takes `200 ms` and in that time the replicas do `400 req`.

- The **hit ratio is 97.77%**, a healthy cache.
- The **p99 is 202ms**, shows the problem.
- For the origin `25ms` and `50ms`, `p99` is just `2ms` because the hits are above 99%.

## Singleflight, Redis lock and Stale

```bash
{
  go run . -report header
  go run . -fix singleflight -label "singleflight"
  go run . -fix lock         -label "+ redis lock"
  go run . -fix stale        -label "+ stale while revalidate" -duration 22s
} | column -t -s $'\t'
```

| variant                  | requests | hit %  | origin loads | peak concurrent | p50   | p99   | max   |
| ------------------------ | -------- | ------ | ------------ | --------------- | ----- | ----- | ----- |
| singleflight             | 17997    | 97.76  | 6            | 6               | 1ms   | 115ms | 203ms |
| + redis lock             | 17998    | 97.76  | 1            | 1               | 990µs | 116ms | 206ms |
| + stale while revalidate | 43998    | 100.00 | 4            | 1               | 1ms   | 2ms   | 5ms   |

- **`singleflight`** collapses the concurrent misses inside one replica.
- **A Redis lock** sets a lock in Redis with TTL origin \* 4. Only the first origin load gets it.
- **Stale while revalidate** writes a soft deadline inside the value and a hard
  TTL several times longer on the key. A reader past the soft deadline kicks off
  the renewal and returns the value it already has.

## TTL jitter

```bash
{
  go run . -report header
  go run . -keys 500                        -label "fixed TTL"
  go run . -keys 500 -jitter 0.2            -label "+ jitter"
  go run . -keys 500             -fix lock  -label "+ lock"
  go run . -keys 500 -jitter 0.2 -fix lock  -label "+ jitter + lock"
  go run . -keys 500 -jitter 0.2 -fix stale -label "+ jitter + stale"
} | column -t -s $'\t'
```

Adds random variance to TTL to prevent thousands of entries from expiring at the exact same moment.

| variant          | requests | hit %  | origin loads | peak concurrent | p50   | p99   | max   |
| ---------------- | :------- | ------ | ------------ | --------------- | ----- | ----- | ----- |
| fixed TTL        | 17998    | 94.91  | 916          | 388             | 990µs | 202ms | 204ms |
| + jitter         | 17998    | 94.87  | 923          | 111             | 1ms   | 202ms | 205ms |
| + lock           | 17998    | 95.03  | 500          | 257             | 990µs | 203ms | 214ms |
| + jitter + lock  | 17999    | 94.97  | 515          | 63              | 970µs | 203ms | 208ms |
| + jitter + stale | 17998    | 100.00 | 515          | 65              | 960µs | 2ms   | 7ms   |

Three fixes, three different columns:

- **Jitter** cuts peak concurrency 388 -> 111 and does not reduce the work at all (916 -> 923 loads, up if anything).
- **Lock** cuts work nearly in half (916 -> 500, one load per key) and barely touches the peak (388 -> 257).

## Observability

The metric that shows the issue is the count of requests arriving at whatever is _behind_ the cache.
