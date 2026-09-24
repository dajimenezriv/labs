# Prometheus alerts: runnable

The hands-on half of [../prometheus-alerts.md](../prometheus-alerts.md). No Kafka: the metrics are a text file you edit.

| container      | what it is                                                        |
| -------------- | ----------------------------------------------------------------- |
| `app`          | serves [metrics.txt](metrics.txt) as `/metrics`                   |
| `prometheus`   | scrapes `app`, evaluates [rules.yml](rules.yml) — `:9090/alerts`  |
| `alertmanager` | groups and routes, [alertmanager.yml](alertmanager.yml) — `:9093` |
| `receiver`     | the fake pager: prints every notification it gets                |

Timings are scaled down (stock → lab): `evaluation_interval` 1m → 5s, `for` 5m → 30s, `group_wait` 30s → 10s, `group_interval` 5m → 30s.

```sh
docker compose up   # shows only the receiver: the pager
```

Nothing is printed until a notification arrives. With the lag at `5000`, the first page shows up **~45–60s** after `up`: a few seconds for `go run` to compile, then scrape + eval, `for: 30s`, and `group_wait: 10s`. Everything at `0` means no alerts, so nothing is printed at all.

Timings below are from one run.

## 1. Pending → firing → one page

In `metrics.txt`, set partitions 0 and 1 to `5000`.

```
18:22:39  edit
18:22:44  both alerts pending         (next scrape + eval)
18:23:14  both firing                 (+30s: for)
18:23:23  → pager firing {alertname: ConsumerLagHigh, job: app} (2 alerts)   (+10s: group_wait)
```

- Two series, **one notification**: `group_by: [alertname, job]`.
- Watch the states at http://localhost:9090/alerts.

## 2. A resolve waits for `group_interval`

Set partition 0 back to `0`.

```
18:24:07  edit
18:24:35  → pager firing (2 alerts): partition 0 resolved, partition 1 firing
```

- Prometheus dropped it in ≤10s; Alertmanager held the change until the group's next tick (≤30s here, ≤5m on stock).

## 3. The first DLQ record never alerts

Append a line the counter didn't have before, already at 1 (as a `CounterVec` does on its first `.Inc()`):

```
kafka_retries_total{topic="sensor.readings",destination="sensor.readings.dlq"} 1
```

```sh
curl -s localhost:9090/api/v1/query --data-urlencode 'query=increase(kafka_retries_total[1m])'
# value "0": no alert
```

Now change it to `2`:

```
18:25:53  → ticket firing {alertname: RecordsInDLQ}      (routed to ticket: severity isn't page)
18:26:53  → ticket resolved                              (the 1m window slid past the step)
```

- The alert resolves on its own while the records are still in the DLQ. `increase()` alerts on arrivals, not on depth.
- Same thing as a unit test: `docker compose exec prometheus sh -c 'cd /etc/prometheus && promtool test rules rules_test.yml'`.

## 4. The service dies, and a page stays open forever

Set partition 2 to `5000`, wait for the page, then `docker compose stop app`.

```
18:27:08  → pager firing {alertname: ConsumerLagHigh} (partition 2)
18:27:13  app stopped: its series go stale, ConsumerLagHigh resolves in Prometheus
18:27:38  → pager firing {alertname: ServiceDown}
          …and no "resolved" for ConsumerLagHigh, ever
```

- `up` is `0` here, not missing: `static_configs` keep the target. With Docker/Kubernetes discovery the target vanishes and only `absent(up{...})` catches it.
- **The lag resolve is lost, not delayed.** When its group flushed, `ServiceDown` was inhibiting it, and Alertmanager drops muted alerts, resolves included. On a real pager, that incident stays open until someone closes it by hand.
- Proof: with `inhibit_rules` removed, the same run sends `→ pager resolved {alertname: ConsumerLagHigh}` 30s after the stop.

Bring it back with `docker compose start app`.

## Useful commands

```sh
docker compose kill -s HUP prometheus     # reload rules.yml / prometheus.yml
docker compose kill -s HUP alertmanager   # reload alertmanager.yml
docker compose exec prometheus promtool check rules /etc/prometheus/rules.yml
docker compose exec alertmanager amtool config routes test \
  --config.file=/etc/alertmanager/alertmanager.yml severity=page job=app   # → pager
docker compose exec alertmanager amtool silence add alertname=ConsumerLagHigh \
  --duration=10m --comment=lab --alertmanager.url=http://localhost:9093
docker compose down
```
