# Distributed Lock

- [The setup](#the-setup)
- [The lock that works](#the-lock-that-works)
- [Work that outlives its lock](#work-that-outlives-its-lock)
- [The pause](#the-pause)
- [Fencing tokens](#fencing-tokens)
- [The TTL dial](#the-ttl-dial)
- [Why Redlock is controversial](#why-redlock-is-controversial)
- [Observability](#observability)

```bash
docker compose up
```

## The setup

- **8 jobs**, one lock key each, and **6 workers** picking one at random.
- **A job takes 250ms**: 200ms of work, then a 50ms write downstream.
- **Lock TTL 3s**, twelve times longer than the job it protects.

```go
won, _ := rdb.SetNX(ctx, "lock:job:00", token, ttl).Result()
if !won {
    // Somebody else has it.
    return
}
defer rdb.Del(ctx, "lock:job:00")
work()
```

Every column is a count of something that should be impossible:

- **overlaps** — a worker entered a critical section another was already inside.
- **stolen releases** — a `DEL` that freed a lock the releaser did not hold.
- **stale writes** — a downstream write from a worker that had stopped holding the lock.
- **corrupt writes** — a stale write that landed out of order, undoing a newer one.
- **rejected** — a write the fence turned away.
- **aborted** — a write never issued, because renewal had already found the lock gone.
- **worst idle** — the longest a job went with nobody running it. Noise unless jobs
  are scarcer than workers, so only the TTL table reads it.

Overlaps, stale writes and corrupt writes are counted by an in-process oracle that
knows which worker really holds each lock. No real service has one. If it did, none
of this would be hard.

## The lock that works

```bash
{
  go run . -report header
  go run . -duration 30s
} | column -t -s $'\t'
```

| variant    | runs | overlaps | stolen releases | stale writes | corrupt writes | rejected | aborted | worst idle |
| ---------- | ---- | -------- | --------------- | ------------ | -------------- | -------- | ------- | ---------- |
| naive lock | 683  | 0        | 0               | 0            | 0              | 0        | 0       | 1.1s       |

683 runs, six workers, one Redis, and nothing wrong. The TTL is twelve times the
job, the release always runs, and `SET NX PX` does exactly what the documentation
says it does.

This row is why the lab exists. Everything below is the same lock code under
conditions nobody typed into a config file.

## Work that outlives its lock

Nothing is injected here. The job simply takes longer than whoever typed `3s`
expected: a slower downstream, one more retry, a batch that grew.

```bash
{
  go run . -report header
  go run . -duration 40s -work 4s
  go run . -duration 40s -work 4s -fix token -label "+ token release"
  go run . -duration 40s -work 4s -fix renew -label "+ lease renewal"
} | column -t -s $'\t'
```

| variant         | runs | overlaps | stolen releases | stale writes | corrupt writes | rejected | aborted | worst idle |
| --------------- | ---- | -------- | --------------- | ------------ | -------------- | -------- | ------- | ---------- |
| naive lock      | 54   | 13       | 11              | 13           | 0              | 0        | 0       | 16.2s      |
| + token release | 54   | 8        | 0               | 8            | 0              | 0        | 0       | 16.2s      |
| + lease renewal | 54   | 0        | 0               | 0            | 0              | 0        | 0       | 12.2s      |

- **The naive lock** loses mutual exclusion on 13 of 54 runs and deletes a lock it
  does not hold 11 times. The second number feeds the first: every stolen `DEL`
  hands the job straight to a third worker.
- **Token-checked release** takes stolen releases to zero and overlaps only from 13
  to 8. It stops the cascade, not the cause. The value is the acquisition's random
  token, and the release compares and deletes in one round trip:

  ```lua
  if redis.call("get", KEYS[1]) == ARGV[1] then
      return redis.call("del", KEYS[1])
  end
  return 0
  ```

- **Lease renewal** fixes the cause. A goroutine extends the lock every `ttl/3`
  while the work runs, so the TTL stops being a guess at the worst case and
  overlaps go to zero.

**Corrupt writes stay at zero down the whole table.** The overrunning worker always
writes *before* the one that took the job from it, so the two writes land in order
and the successor's value is the one that survives. Work that outlives its lock
costs duplicate work, not clobbered data — it charges the card twice, it does not
corrupt the row.

## The pause

A stop-the-world pause freezes the whole process, renewer included, wherever it
happens to be: a GC, a hypervisor steal, a swapped-out page. Here it is 4 seconds
against a 3 second TTL, on 15% of runs, landing at a uniform point anywhere in the
critical section — including in the middle of the 50ms write.

```bash
{
  go run . -report header
  go run . -duration 30s -pause
  go run . -duration 30s -pause -fix token -label "+ token release"
  go run . -duration 30s -pause -fix renew -label "+ lease renewal"
  go run . -duration 30s -pause -fix fence -label "+ fencing token"
} | column -t -s $'\t'
```

| variant         | runs | overlaps | stolen releases | stale writes | corrupt writes | rejected | aborted | worst idle |
| --------------- | ---- | -------- | --------------- | ------------ | -------------- | -------- | ------- | ---------- |
| naive lock      | 227  | 33       | 11              | 19           | 13             | 0        | 0       | 5.9s       |
| + token release | 186  | 32       | 0               | 20           | 17             | 0        | 0       | 5.3s       |
| + lease renewal | 167  | 29       | 0               | 7            | 5              | 0        | 24      | 5.3s       |
| + fencing token | 191  | 33       | 0               | 5            | 0              | 4        | 20      | 6.6s       |

- **Overlaps do not move: 33, 32, 29, 33.** Not one of the fixes reduces how often
  two workers are inside the same critical section. By the time the lock expired,
  the first worker had stopped executing instructions; there is nothing left in it
  to fix.
- **Token-checked release buys nothing here.** It takes stolen releases to zero,
  which is strictly more correct, and its corrupt writes come out at 17 against the
  naive 13 — the two are the same number inside run-to-run noise. Stolen releases
  cascade when the work simply overran; under a pause they are a symptom, not the
  cause.
- **Renewal helps and does not hold.** 24 runs aborted: the renewer came back from
  the freeze, found the lock gone and stopped the write before it was issued.
  Corrupt writes fall from 17 to 5 and stop there. A pause that lands *after* the
  last check and during the write cannot be checked for, and that window is
  `write / (work + write)` of every critical section.

## Fencing tokens

Every fix above asks a process that may not be running to notice something. The
fence does not ask the worker anything. Each acquisition takes a number from the
same Redis that handed out the lock, and the number travels with the write:

```go
l.fence, _ = rdb.Incr(ctx, "fence:job:00").Result()
// ... work ...
if l.fence < store.highest[job] {
    return errStale // this write would undo a newer one
}
```

| variant         | runs | overlaps | stolen releases | stale writes | corrupt writes | rejected | aborted | worst idle |
| --------------- | ---- | -------- | --------------- | ------------ | -------------- | -------- | ------- | ---------- |
| + lease renewal | 167  | 29       | 0               | 7            | 5              | 0        | 24      | 5.3s       |
| + fencing token | 191  | 33       | 0               | 5            | 0              | 4        | 20      | 6.6s       |

- **Corrupt writes: zero.** Four writes arrived out of order and the store turned
  all four away. This is the only row in the lab where that column is zero under a
  pause.
- **Overlaps: 33, unchanged.** The fence does not stop two workers running the same
  job. It stops the loser's result from being the one that survives. If the job's
  only effect is the write, that is the whole problem solved. If it also sends an
  email, the email went out twice and nothing here helps.
- **Five stale writes, four rejected.** The fifth arrived before its successor had
  written anything, so its number was still the highest the store had seen and it
  was accepted — correctly. The holder then overwrote it with a higher number. The
  fence does not detect stale writers, it enforces an order, and the order is
  enough.

## The TTL dial

The obvious answer to all of the above is to raise the TTL until the pause fits
inside it. It works, and this is the bill. Same 4s pause, plus 2% of runs where the
worker dies holding the lock and never releases it, over 4 jobs so that a job
nobody is running reads as idle time instead of as scheduling noise.

```bash
{
  go run . -report header
  for t in 1s 3s 10s 30s; do
    go run . -duration 30s -jobs 4 -pause -crash -ttl $t -fix token -label "ttl $t"
  done
} | column -t -s $'\t'
```

| variant | runs | overlaps | stolen releases | stale writes | corrupt writes | rejected | aborted | worst idle |
| ------- | ---- | -------- | --------------- | ------------ | -------------- | -------- | ------- | ---------- |
| ttl 1s  | 190  | 129      | 0               | 28           | 27             | 0        | 0       | 2.3s       |
| ttl 3s  | 171  | 69       | 0               | 19           | 18             | 0        | 0       | 3s         |
| ttl 10s | 86   | 0        | 0               | 0            | 0              | 0        | 0       | 10s        |
| ttl 30s | 73   | 0        | 0               | 0            | 0              | 0        | 0       | 23.2s      |

- **Below the pause the lock barely exists.** At 1s the job still finishes in a
  quarter of its lease, and mutual exclusion fails 129 times: a frozen worker
  leaves its job open for 4 seconds and the other five cycle through it every
  250ms.
- **Above the pause, safety is free and liveness is not.** At 10s the overlaps and
  the corrupt writes are gone. A job whose worker died now sits untouched for the
  whole 10 seconds, and throughput halves, 171 runs down to 86. At 30s the worst
  idle is 23.2s, which is only that number because the run ended before the lock
  did.
- **The TTL is the only dial, and it has safety at one end and liveness at the
  other.** Nothing here removes the tradeoff; it only moves along it.

The setting that looks right in this table — 10s, comfortably over a 4s pause — was
chosen by knowing the length of the pause in advance. In production that number is
the longest GC pause, disk stall or hypervisor steal the worker will ever see, and
it is picked once, in a config file, before any of them have happened. The fence is
the only line in this lab that does not require guessing it.

## Why Redlock is controversial

Redlock is the multi-node version of this lock: the same `SET NX PX` on five
independent Redis masters, and the lock counts as held if a majority answered yes
well inside the TTL. It is aimed at a failure this lab never showed — a single
Redis losing the lock key when it dies, because replication is asynchronous and
the promoted replica may never have seen the write.

What it does not change is a single number in the tables above. Redlock makes the
lock harder to *lose*. Every row here was broken by a holder that stopped running,
and no quorum has an opinion about that. The first half of the argument against it
is that the extra nodes buy a property most people were not missing, while the
property they were missing — a downstream that can tell a current holder from a
stale one — is not for sale at any number of nodes.

The second half is about clocks. "A majority answered well inside the TTL" is a
statement about elapsed time measured on machines that do not agree. A leap in one
of their clocks, an NTP correction, a VM paused and resumed, and the majority that
looked valid was not. One `SET NX PX` has the same problem in a milder form: the
TTL is Redis's clock and the deadline the worker believes is its own.

The useful conclusion is not that Redlock is wrong, it is that "how do I make the
lock more correct" has a ceiling well below what people expect a lock to give them.
If the downstream write must never be clobbered, that has to be enforced
downstream. If the lock is only an efficiency measure — one worker doing the
expensive thing instead of six — then one `SET NX PX` with a token-checked release
was already enough, and Redlock is buying very little.

## Observability

The metric that shows the issue is the count of writes arriving at the resource
from someone who is no longer the holder. Nothing Redis exports can produce it.

Redis is not malfunctioning in any row of this lab. It takes the `SET NX`, honours
the TTL to the millisecond, and hands the lock to the next asker exactly as told.
`redis_commands_processed_total` is flat, `redis_expired_keys_total` shows the
lease expiring on schedule, and neither is a symptom of anything. The failure lives
in the gap between what Redis promised and what a frozen worker still believed, and
that gap is not in any Redis metric, any client metric, or any lock library's
dashboard.

Which leaves two things worth exporting from the application:

- **`lock_downstream_writes_total{result="rejected"}`** — the fence firing. A rate
  above zero means mutual exclusion is failing somewhere upstream and the resource
  is absorbing it. It is the alert you can actually build.
- **`lock_renewals_total{result="lost"}`** — a lock that expired under the worker
  still using it. It undercounts by exactly the cases that matter most, because a
  process that is frozen does not renew and does not report, but a nonzero rate
  still means the TTL is too short for the work.
