# Feature Flags with LISTEN/NOTIFY

- **Writes are rare**: a few a day, from a human.
- **Reads are constant**: thousands a second. Use an in-memory `map[string]Flag`.

## The setup

```bash
docker compose up
psql postgres://postgres:postgres@localhost:5555/db -f seed.sql
```

### The payload is a key, not a flag

Two options:

- Send the whole row. It's faster, but if we change a key twice it can deliver wrong. Also `pg_notify` caps the payload at **8000 bytes** and raises `22023 payload string too long`.
- Send just the updated key and then to a `SELECT` to check the updated value.

### What LISTEN/NOTIFY actually promises

Two halves, and they pull in opposite directions:

- **Transactional.** The notification is queued by the trigger and delivered at `COMMIT`. Roll back and nothing is sent, ever.
- **Not durable.** At-most-once. No retention, no replay, no offset.

So NOTIFY is a latency optimisation over polling. It is never the thing that
makes a listener correct.

## 1. Propagation: polling versus listening

```bash
./propagate.sh
```

20 instances, 30 flag changes a second apart. `p50/p99/max` are wall-clock
from issuing the `UPDATE` to the flag being live in an instance's map;
`queries` and `notifs` are totals across all 20.

| mode   | every |   p50ms |    p99ms |   maxms | queries | notifs |
| ------ | ----: | ------: | -------: | ------: | ------: | -----: |
| poll   |   15s |  7832.7 |  15002.9 | 15003.3 |      60 |      0 |
| poll   |    5s |  2861.0 |   4984.5 |  4987.7 |     140 |      0 |
| poll   |    1s |   852.9 |    982.5 |   983.5 |     620 |      0 |
| listen |     - | **9.3** | **10.8** |    12.0 |     620 |    600 |
| resync |   10s |     9.2 |     12.8 |    13.1 |     680 |    600 |

Polling's p99 is the interval and its p50 is half of it, which is arithmetic
rather than a finding. The finding is the standing cost, with nothing
happening at all:

| mode                  | every | queries in 30s | backends |
| --------------------- | ----: | -------------: | -------: |
| baseline (stack idle) |     - |              - |        1 |
| poll                  |    1s |            640 |        9 |
| poll                  |    5s |            140 |        9 |
| listen                |     - |             20 |       22 |
| resync                |   10s |             80 |       29 |

That is the trade in two numbers. Sub-second propagation by polling costs
640 queries per 30 seconds, forever, to be told nothing changed. Listening
costs 20 — one snapshot per instance at startup and nothing after — and lands
in single-digit milliseconds.

The `backends` column is where it charges you instead. Polling's 20 instances
share 8 pooled connections; listening needs **22 backends for the same 20
instances**, because `LISTEN` is session state and cannot come from a pool.
One parked connection per instance, not shared, not returnable, counted
against `max_connections` all day — and the one connection in the service
that cannot sit behind a transaction-mode pooler, for the reasons in
[connection-pooling.md](../connection-pooling/connection-pooling.md). At 100
instances that is a capacity decision, not a detail.

`resync` costs 29 because its watchdogs run on the shared pool, which is the
point of running them there: 20 dedicated listener sessions plus a pool that
still behaves like a pool.

## 2. The listener that stops listening

Connections end. Failover, a network blip, a restarted pooler, a firewall
reaping something it decided was idle. So everyone writes the reconnect loop,
and the reconnect loop looks right:

```go
for ctx.Err() == nil {
    conn, err := pgx.Connect(ctx, dsn)      // reconnect
    conn.Exec(ctx, "LISTEN flags")          // resubscribe
    for {
        n, err := conn.WaitForNotification(ctx)
        if err != nil { break }             // dropped; go round again
        apply(n.Payload)
    }
}
```

It reconnects, it resubscribes, it succeeds, it logs nothing. And every change
published between the drop and the resubscribe was delivered to nobody, is
not replayed, and is never mentioned again.

```bash
./resilience.sh
```

Same 20 instances and 30 changes, now terminating every listening backend
every 5 seconds — `pg_terminate_backend`, which from the client is
indistinguishable from all four real causes. `stale` counts instances serving
at least one flag at an older version than the committed one, one second
after the last change; `stale+settle` is the same count 25 seconds later.

| mode   | every |  p50ms |       p99ms | missed | drops |     stale | stale+settle |
| ------ | ----: | -----: | ----------: | -----: | ----: | --------: | -----------: |
| listen |     - |    9.2 | **16128.2** | **80** |   220 | **20/20** |    **20/20** |
| resync |   10s |    9.1 |       508.2 |      0 |   220 |         0 |            0 |
| poll   |    5s | 2860.7 |      4982.5 |      0 |     0 |         0 |            0 |

Every one of the 20 instances ends up serving a flag at the wrong value, and
25 seconds later every one of them still is. Nothing errored. Each instance
reconnected within half a second, resubscribed successfully, and sat there
healthy and wrong — `pg_stat_activity` shows twenty connected listeners the
whole time.

Two columns are worth reading carefully. `missed` counts 80 flag changes that
never reached an instance at all, and `p99` of 16 seconds is the changes that
_did_ arrive, very late. Both come from the same mechanism: the only thing
that can repair a missed notification is **another change to the same key**,
because that is what triggers the next re-read of that row. A flag nobody
touches again stays wrong forever, and one that happens to be flipped again
16 seconds later gets silently repaired by the second flip. Neither outcome
is something the instance can distinguish from working correctly.

`poll` is in that table because it has nothing to lose. It holds no
subscription and no position, so a dead connection costs it one interval.
That is not a tuning difference, it is the structural argument for polling,
and it is why the fix below is polling.

### The fix is to keep polling, slowly

`resync` differs from `listen` in two small ways:

- **on every reconnect, reload the whole table** rather than only
  resubscribing. The gap is exactly where the missed changes are.
- **a watchdog**, every 10 seconds, asking one indexed question —
  `SELECT max(version) FROM lab.flags` — and reloading everything if the
  answer is ahead of what this instance holds. It runs on the shared pool, not
  the listening connection, because it has to work in precisely the situation
  where that connection is the broken thing.

That second one is what fixes the table above, and its cost is visible in the
`p99` column: 508 ms, which is the reconnect backoff. Changes that land while
an instance is between connections are not lost, they are late by one
backoff. Everything else still arrives in 9 ms.

Notifications become the fast path and the slow poll becomes the correct one.
Which is the same conclusion as the outbox in the Kafka lab from the other
direction: the mechanism that is fast when everything works is not allowed to
be the only mechanism.

## What this costs you

- **A staleness bound you cannot see.** Nothing errors, no metric moves, and
  `pg_stat_activity` shows a healthy connected listener. The only way to know
  an instance is wrong is to ask it what version it holds. Export that, and
  alert on the spread across instances — not on the listener being connected,
  which it will be.
- **A backend per instance, unpoolable.** 22 backends for 20 instances
  here, against 9 for polling. At a hundred instances that is a real fraction of
  `max_connections`, and it is the one connection in the service that cannot
  go behind a transaction-mode pooler — see
  [connection-pooling.md](../connection-pooling/connection-pooling.md).
- **An 8000-byte ceiling on the payload**, enforced against the `UPDATE`
  rather than against delivery.
- **No replay.** Everything above follows from this one line. If what you are
  propagating cannot tolerate at-most-once, LISTEN/NOTIFY is the wrong
  transport and the outbox is the right one.
