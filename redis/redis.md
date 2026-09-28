# Redis

- [What is Redis?](#what-is-redis)
- [Why is it fast?](#why-is-it-fast)
- [What data structures does it have?](#what-data-structures-does-it-have)
- [How do keys expire?](#how-do-keys-expire)
- [What happens when memory is full?](#what-happens-when-memory-is-full)
- [Does Redis lose data?](#does-redis-lose-data)
- [How does replication work?](#how-does-replication-work)
- [Sentinel vs Cluster?](#sentinel-vs-cluster)
- [Which operations are atomic?](#which-operations-are-atomic)
- [Pipelining vs transactions?](#pipelining-vs-transactions)
- [Pub/Sub vs Streams?](#pubsub-vs-streams)
- [What should you never do?](#what-should-you-never-do)
- [What is it used for?](#what-is-it-used-for)

## What is Redis?

- An in-memory key-value store.
- Values are data structures, not just strings.
- Clients talk to it over TCP with a simple text protocol (RESP).
- Usually a cache or shared state in front of a real database.

```bash
SET user:42:name "Ada" EX 60   # set with a 60s TTL
GET user:42:name               # "Ada"
TTL user:42:name               # 57
```

## Why is it fast?

- Everything lives in RAM: a command is ~microseconds, a round trip is ~0.2ms on a LAN.
- **Commands execute on a single thread**, one after another. No locks, no context switches.
- Since Redis 6, I/O threads can read and write sockets, but execution is still single-threaded.
- Consequence: one slow command blocks every other client.

## What data structures does it have?

| Type        | Example use                                | Commands                             |
| ----------- | ------------------------------------------ | ------------------------------------ |
| String      | Cache entry, counter, lock                 | `GET`, `SET`, `INCR`                 |
| Hash        | An object's fields (`user:42` → name, age) | `HGET`, `HSET`, `HINCRBY`            |
| List        | Simple queue                               | `LPUSH`, `RPOP`, `BLPOP`             |
| Set         | Unique members, tags                       | `SADD`, `SISMEMBER`, `SINTER`        |
| Sorted Set  | Leaderboard, sliding-window rate limit     | `ZADD`, `ZRANGE`, `ZREMRANGEBYSCORE` |
| Bitmap      | Daily active users by user id              | `SETBIT`, `BITCOUNT`                 |
| HyperLogLog | Approximate unique count in 12KB           | `PFADD`, `PFCOUNT`                   |
| Stream      | Append-only log with consumer groups       | `XADD`, `XREADGROUP`, `XACK`         |

- Sorted Set is the one interviewers like: members ordered by a score, `O(log N)` inserts.

## How do keys expire?

- A TTL is set per key (`EX`, `PX`, `EXPIRE`). No TTL means the key lives forever.
- **Lazy**: when a key is accessed, Redis checks whether it expired and deletes it.
- **Active**: a background cycle samples keys with a TTL and deletes the expired ones.
- So an expired key is never returned, but its memory may be freed a bit later.
- Overwriting a key with `SET` removes its TTL unless you pass one again (or `KEEPTTL`).

## What happens when memory is full?

- `maxmemory` sets the limit. `maxmemory-policy` decides what happens when it's reached.
- **Default is `noeviction`**: writes fail with `OOM command not allowed`. Fine for a store, an incident for a cache.

| Policy         | Evicts                                    |
| -------------- | ----------------------------------------- |
| `noeviction`   | Nothing, writes fail                      |
| `allkeys-lru`  | Least recently used, any key              |
| `allkeys-lfu`  | Least frequently used, any key            |
| `volatile-lru` | Least recently used, only keys with a TTL |
| `volatile-ttl` | Keys closest to expiring                  |

- LRU and LFU are approximate: Redis samples a few keys (`maxmemory-samples` = 5) and evicts the best candidate.
- For a cache: `allkeys-lru` or `allkeys-lfu`.

## Does Redis lose data?

- Without persistence, a restart loses everything.
- **RDB**: a snapshot of memory to disk every N seconds/writes. Compact, fast restart, loses everything since the last snapshot.
- **AOF**: appends every write to a log. `appendfsync everysec` (the default) loses at most ~1s.
- Both can be enabled. Snapshots `fork()` the process, so memory can briefly double on write-heavy loads.

## How does replication work?

- One primary takes writes, replicas copy it.
- **Replication is async**: the primary acks the client before replicas have the write.
- If the primary dies, the promoted replica may never have seen the last writes. **Acknowledged writes can be lost.**
- `WAIT n timeout` blocks until n replicas have the write, but it's still not a consensus guarantee.
- Reads from replicas can be stale (replication lag).

## Sentinel vs Cluster?

|          | Sentinel                          | Cluster                                     |
| -------- | --------------------------------- | ------------------------------------------- |
| Solves   | High availability                 | High availability + scaling beyond one node |
| Data     | All on one primary                | Sharded across primaries                    |
| Failover | Sentinels vote, promote a replica | Nodes vote, promote a replica               |
| Client   | Asks Sentinel who the primary is  | Follows `MOVED` / `ASK` redirects           |

- Cluster splits keys into **16384 hash slots**: `CRC16(key) mod 16384`.
- Multi-key commands (`MGET`, transactions, Lua) only work if all keys are in the same slot.
- **Hash tags** force that: only the part in `{}` is hashed, so `{user:42}:cart` and `{user:42}:orders` share a slot.

## Which operations are atomic?

- **Every single command is atomic**: it's single-threaded. `INCR`, `SET NX`, `LPUSH` can't interleave.
- The problem is read-modify-write across commands: `GET`, change in Go, `SET` races with other clients.
- **Lua scripts** run as one command: nothing else runs in between.

```lua
-- Increment only if below a limit, atomically.
local n = tonumber(redis.call("GET", KEYS[1]) or "0")
if n < tonumber(ARGV[1]) then
  return redis.call("INCR", KEYS[1])
end
return -1
```

- Keep scripts short: they block the whole server while they run.

## Pipelining vs transactions?

- **Pipelining**: send many commands without waiting for each reply. Saves round trips. **Not atomic**, other clients' commands can run in between.
- **`MULTI` / `EXEC`**: commands are queued and run together, nothing interleaves. **No rollback**: if one fails, the others still apply.
- **`WATCH`**: optimistic locking. `EXEC` aborts if a watched key changed since `WATCH`, and you retry.

```go
// Pipeline: 3 commands, 1 round trip.
pipe := rdb.Pipeline()
pipe.Incr(ctx, "views")
pipe.Expire(ctx, "views", time.Hour)
_, err := pipe.Exec(ctx)
```

## Pub/Sub vs Streams?

|                    | Pub/Sub                         | Streams                          |
| ------------------ | ------------------------------- | -------------------------------- |
| Storage            | None, fire-and-forget           | Persisted log                    |
| Subscriber offline | Misses the message              | Reads it later                   |
| Acks               | No                              | `XACK`, pending list for retries |
| Use                | Cache invalidation, live events | Lightweight job queue            |

- Streams is not a Kafka replacement: it lives in memory, and replication is async so a failover can lose entries.

## What should you never do?

- `KEYS *`: O(N) over the whole keyspace, blocks the server. Use `SCAN` with a cursor.
- **Big keys**: a hash with millions of fields. Deleting it blocks (use `UNLINK`, which frees memory in the background).
- **Hot keys**: one key taking most of the traffic. In a cluster it lands on one node.
- Treat Redis as the source of truth: async replication means a failover can lose writes.
- Let a Redis error fail the request when it's a cache: fall through to the DB.

## What is it used for?

- **Cache**: see the cache patterns in [README.md](README.md).
- **Distributed lock**: `SET NX PX`, see [distributed-lock.md](distributed-lock.md).
- **Rate limiting**: `INCR` + `EXPIRE` for fixed windows, Sorted Set for sliding windows.
- **Sessions**: a key per session with a TTL.
- **Leaderboards**: Sorted Set.
- **Counters and dedup**: `INCR`, `SET NX`, HyperLogLog.
