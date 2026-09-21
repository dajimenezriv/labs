# Feature Flags with LISTEN/NOTIFY

- **writes are rare** — a few a day, from a human.
- **reads are constant** — thousands a second, and the answer almost never changes. Use an in-memory `map[string]Flag`.

So the read path cannot be a query. A round trip and a pooled connection to
answer a question whose answer changed twice this week is a way to take your
own database down with your own configuration. Every instance holds the flag
table in a `map[string]Flag` and evaluates from memory, which turns a flag
check into a map lookup.

That is not the interesting part. The interesting part is that instance
memory is now a **copy**, and the entire rest of this lab is one question:
when somebody flips a flag, how does an instance find out its copy is wrong,
and what is it serving until it does?

It matters because a flag is how you turn a broken feature off. A kill switch
that takes thirty seconds to land is a thirty-second outage you chose.

## The setup

```bash
docker compose up -d
psql postgres://postgres:postgres@localhost:5555/db -f seed.sql
```

200 flags, a monotonic `version` per change, and a trigger that announces
every change on a channel:

```sql
CREATE FUNCTION lab.flag_changed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.version := nextval('lab.flags_version');
  NEW.updated_at := clock_timestamp();
  PERFORM pg_notify('flags', NEW.key);
  RETURN NEW;
END $$;
```

### The payload is a key, not a flag

Sending the whole row is the obvious optimisation — the listener applies it
straight to its map and never queries at all — and it is the wrong default
for a reason unrelated to speed: **a payload is a statement about the past.**

Delivery is at-most-once and unordered with respect to everything else the
listener is doing. By the time a payload is applied, the row it describes may
have changed twice more, and the listener may already hold a newer version
from a reconnect snapshot. Applying it then walks the flag _backwards_, to a
value that was briefly true and is now wrong, and the instance stays there
until the next change to that key, because nothing will contradict it.
Guarding with a version check makes that safe without making it useful: the
correct handling of a stale payload is to discard it, which leaves an
announcement that carried nothing.

Re-reading has no such state. It converges on the current row however many
notifications were missed, duplicated or delivered late, because the answer
comes from the table and not from the message. The notification is reduced to
what it is actually good for — a fast hint that something changed — and the
code handling it is the same code that handles a resync in §2.

It also sidesteps a ceiling. `pg_notify` caps the payload at **8000 bytes**
and raises `22023 payload string too long` above it; because the call sits
inside the trigger, that error fails the `UPDATE` that fired it. A flag whose
targeting rules outgrow the cap would stop being _writable_, not merely
undeliverable. A key is never going to approach 8000 bytes.

The cost is one indexed lookup per flag change per instance, which is the
`queries` column below.

### What LISTEN/NOTIFY actually promises

Two halves, and they pull in opposite directions:

**Transactional.** The notification is queued by the trigger and delivered at
`COMMIT`. Roll back and nothing is sent, ever. That is atomicity between a
data change and its announcement, and it is precisely what an outbox table
exists to fake when the broker lives outside the database — compare
[kafka/cmd/service/outbox.go](../../../kafka/cmd/service/outbox.go), which is
a whole table, a poller and a publisher buying this one property.

**Not durable.** At-most-once. No retention, no replay, no offset. A
notification published while a listener is between connections is not queued
for it and not redelivered. It is gone, and nothing on either side records
that it existed.

The instinct to file this under "pub/sub is unreliable" is half right and the
wrong half is the one that costs you. Redis pub/sub behaves like this. Kafka
does not: a consumer that dies and comes back resumes from a committed
offset, so the messages it missed are still there. `LISTEN` has no committed
position to resume from, and §2 is what that absence looks like from
production.

So NOTIFY is a latency optimisation over polling. It is never the thing that
makes a listener correct.

## 1. Propagation: polling versus listening

```bash
./propagate.sh
```

20 instances, 30 flag changes a second apart. `p50/p99/max` are wall-clock
from issuing the `UPDATE` to the flag being live in an instance's map;
`queries` and `notifs` are totals across all 20.

TABLE_1_HERE

Polling's p99 is the interval and its p50 is half of it, which is arithmetic
rather than a finding. The finding is the standing cost, with nothing
happening at all:

TABLE_2_HERE

That is the trade in two numbers. Sub-second propagation by polling costs
QPS_1S queries per 30 seconds, forever, to be told nothing changed. Listening
costs zero queries at rest and lands in single-digit milliseconds — and costs
LISTEN_BACKENDS backends instead of POLL_BACKENDS, because `LISTEN` is session
state. It cannot come from the pool. One parked connection per instance, not
shared, not returnable, counted against `max_connections` all day.

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

TABLE_3_HERE

STALE_PARA

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

RESYNC_PARA

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
- **A backend per instance, unpoolable.** LISTEN_BACKENDS backends for 20
  instances here. At a hundred instances that is a real fraction of
  `max_connections`, and it is the one connection in the service that cannot
  go behind a transaction-mode pooler — see
  [connection-pooling.md](../connection-pooling/connection-pooling.md).
- **An 8000-byte ceiling on the payload**, enforced against the `UPDATE`
  rather than against delivery.
- **No replay.** Everything above follows from this one line. If what you are
  propagating cannot tolerate at-most-once, LISTEN/NOTIFY is the wrong
  transport and the outbox is the right one.
