# Feature Flags with LISTEN/NOTIFY

```bash
docker compose up
```

```sql
CREATE TABLE lab.flags (
  key text PRIMARY KEY,
  enabled boolean NOT NULL DEFAULT false,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION lab.flag_changed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := clock_timestamp();
  PERFORM pg_notify('flags', NEW.key);
  RETURN NEW;
END $$;

CREATE TRIGGER flag_changed
BEFORE INSERT OR UPDATE ON lab.flags
FOR EACH ROW EXECUTE FUNCTION lab.flag_changed();
```

- **Writes are rare**: a few a day, from a human.
- **Reads are constant**: thousands a second. Use an in-memory `map[string]Flag`.

`LISTEN/NOTIFY`:

- **Transactional.** The notification is queued by the trigger and delivered at `COMMIT`. Roll back and nothing is sent, ever.
- **Not durable.** At-most-once. No retention, no replay, no offset.

So `NOTIFY` is a latency optimisation over polling, but since is at-most-once we can miss updates. We need polling + listening.

What to send in `NOTIFY`?

- Send the whole row: faster, but if we change a key twice it can deliver wrong. Also `pg_notify` caps the payload at **8000 bytes** and raises `22023 payload string too long`.
- Send just the updated key and then to a `SELECT` to check the updated value.

## 1. Propagation: polling versus listening

```bash
./propagate.sh
```

| mode        | every |  p50ms |  p99ms | queries | notifs | backends |
| ----------- | ----: | -----: | -----: | ------: | -----: | -------: |
| poll        |    5s | 2873.9 | 4985.7 |     140 |      0 |        8 |
| poll idle   |    5s |      - |      - |     140 |      0 |        8 |
| listen      |     - |    8.5 |   11.4 |     620 |    600 |       21 |
| listen idle |     - |      - |      - |      20 |      0 |       22 |

- 20 instances.
- Polling's p99 is the interval and its p50 is half of it, which is arithmetic.
- Polling costs the same whether or not anything happens. Number of backends is `pool.MaxConns`.
- Listening costs nothing at rest and scales with changes. Each listening session is a connection, because `LISTEN` cannot come from a pool. We need to be careful to not saturate `max_connections`.

## 2. The listener that stops listening

Connections end. Failover, a network blip, a restarted pooler, a firewall
reaping something it decided was idle. So everyone writes the reconnect loop,
and the reconnect loop looks right:

```go
for ctx.Err() == nil {
    conn, err := pgx.Connect(ctx, dsn) // reconnect
    conn.Exec(ctx, "LISTEN flags") // resubscribe
    for {
        n, err := conn.WaitForNotification(ctx)
        if err != nil { break } // dropped; go round again
        apply(n.Payload)
    }
}
```

Every change published between the drop and the resubscribe is never delivered.

```bash
./resilience.sh
```

| mode   | every |  p50ms |  p99ms |  maxms | missed | queries | notifs | backends | drops | stale     |
| ------ | ----: | -----: | -----: | -----: | -----: | ------: | -----: | -------: | ----- | --------- |
| listen |     - |    8.5 |   15.4 |   15.7 |    100 |     520 |    500 |       22 | 220   | **20/20** |
| poll   |    5s | 2852.7 | 4985.5 | 4987.1 |      0 |     240 |      0 |        8 | 0     | 0         |

- 20 instances.
- Listening should have had 620 queries, but it missed 100.
- Listening drops its connection 220 times.
- All 20 instances had an stale value in at least one of their keys.
