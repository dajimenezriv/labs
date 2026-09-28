# Distributed Lock

- [How do you build a lock with Redis?](#how-do-you-build-a-lock-with-redis)
- [Why check a token on release?](#why-check-a-token-on-release)
- [What if the work takes longer than the TTL?](#what-if-the-work-takes-longer-than-the-ttl)
- [What does a GC pause break?](#what-does-a-gc-pause-break)
- [What is a fencing token?](#what-is-a-fencing-token)
- [Why not just raise the TTL?](#why-not-just-raise-the-ttl)
- [Why is Redlock controversial?](#why-is-redlock-controversial)
- [When is a single SET NX enough?](#when-is-a-single-set-nx-enough)
- [Observability](#observability)

## How do you build a lock with Redis?

- `SET key token NX PX ttl`: set only if absent, with an expiry.
- `NX` gives mutual exclusion, `PX` makes sure a dead holder doesn't lock the job forever.
- `token` is random per acquisition, so the holder can prove it's still the holder.

```go
won, _ := rdb.SetNX(ctx, "lock:job:42", token, 3*time.Second).Result()
if !won {
  return // somebody else has it
}
defer release(ctx, "lock:job:42", token)
work()
```

## Why check a token on release?

- A plain `DEL` frees whatever lock is there, even one that expired and was taken by someone else.
- Every stolen `DEL` hands the job to a third worker: the overlaps cascade.
- Compare and delete must be one atomic step, so it's a Lua script.

```lua
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
end
return 0
```

- It stops the cascade, not the cause: the lock still expired under a running worker.

## What if the work takes longer than the TTL?

- The lock expires, a second worker takes it, two workers run the same job.
- Nothing has to be injected: a slower downstream, one more retry, a bigger batch.
- Fix: **lease renewal**. A goroutine extends the lock every `ttl/3` while the work runs, with the same token check.
- Cost is duplicate work, not corrupt data: the overrunning worker writes before its successor, so the newer value wins.

```lua
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0
```

## What does a GC pause break?

- A stop-the-world pause (GC, hypervisor steal, swapped page) freezes the whole process, renewer included.
- Pause longer than the TTL → the lock expires → another worker takes it → the paused one wakes up and writes anyway.
- If it wakes up after the new holder wrote, its stale write **undoes a newer one**.
- Token release and renewal don't help: a frozen process can't check anything.
- Renewal can abort the write if it notices the lock is gone, but a pause between the last check and the write can't be detected.

## What is a fencing token?

- A monotonically increasing number handed out with each acquisition (`INCR` in the same Redis).
- It travels with the write. The downstream rejects any number lower than the highest it has seen.
- The downstream enforces the order, it doesn't ask the worker anything.

```go
fence, _ := rdb.Incr(ctx, "fence:job:42").Result()
// ... work ...
// downstream, atomically:
// UPDATE jobs SET result = $1, fence = $2 WHERE id = $3 AND fence < $2
```

- Two workers can still run the same job. The fence only makes sure the loser's result doesn't survive.
- Side effects outside the fenced resource (an email, a charge) still happen twice. Those need idempotency keys.
- Requires a downstream that can compare: a DB row, a storage system that supports conditional writes.

## Why not just raise the TTL?

- The TTL trades **safety** against **liveness**.
- TTL below the longest pause → mutual exclusion fails.
- TTL above it → if the holder crashes, the job sits idle for the whole TTL.
- Picking it means knowing the longest pause the worker will ever see, before it happens.
- The fence is the only fix that doesn't require guessing that number.

## Why is Redlock controversial?

- Redlock: `SET NX PX` on 5 independent masters, lock held if a majority answered well inside the TTL.
- It fixes one failure: a single Redis losing the key on failover, because replication is async.
- It doesn't fix a paused holder. No quorum knows the worker stopped running.
- It depends on clocks: "inside the TTL" is elapsed time measured on machines that don't agree. NTP jumps or a paused VM break it.
- Kleppmann's critique: too heavy for efficiency locks, not safe enough for correctness locks. Use fencing.

## When is a single SET NX enough?

- **Efficiency lock**: avoids doing the expensive thing twice. Occasional overlap costs duplicate work. `SET NX PX` + token release is enough.
- **Correctness lock**: overlap corrupts data. No lock gives that, it must be enforced downstream with a fence (or use a consensus system like etcd/ZooKeeper, which still needs a fence).

## Observability

- Redis looks healthy the whole time: it honours the TTL and hands the lock to the next asker exactly as told.
- The failure is the gap between what Redis promised and what a frozen worker still believes. No Redis metric shows it.
- Export from the application:
  - `lock_downstream_writes_total{result="rejected"}`: the fence firing. Above zero means mutual exclusion is failing upstream. The alert you can build.
  - `lock_renewals_total{result="lost"}`: lock expired under a worker still using it. Undercounts (a frozen process doesn't report), but above zero means the TTL is too short for the work.
