1. Cache stampede on a hot key

Put a Go service in front of a deliberately slow Postgres query, cache the result in Redis, and hammer it with load until the key expires and hundreds of requests hit the database at once. Then fix it step by step with singleflight, a short Redis lock around recomputation, TTL jitter, and early background refresh, measuring DB load and p99 latency after each change.

2. Distributed lock that silently breaks

Build a job worker that uses a Redis lock (SET NX PX) so only one instance processes a task. Then break it: add a GC-style pause longer than the TTL, kill a worker mid-task, and release a lock that someone else now holds. Fix it with token-checked release via Lua, lease renewal, and fencing tokens on the downstream write, and be ready to discuss why Redlock is controversial.

3. Redis failure and graceful degradation

Run Redis with a replica and Sentinel (or a small Cluster), then kill the primary under load and watch what your Go client does: timeouts, connection pool exhaustion, lost writes during failover, stale reads from replication lag. Add sensible timeouts, a circuit breaker, and a fallback path so the service degrades instead of going down. Along the way, fill memory to trigger eviction and find big or hot keys.