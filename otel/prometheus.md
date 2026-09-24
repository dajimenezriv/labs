# Prometheus

- Collects and stores real-time numerical performance data.
- It scraps an HTTP /metrics endpoint at a fixed interval (usually 15s). Targets must be discoverable (static config, DNS, Docker).

## Metrics

- **Counter**: just increases. Use `rate()` or `increase()`.
- **Gauge**: goes up and down. Queue depth, memory, connections in use.
- **Histogram**: cumulative buckets (`_bucket` with an `le` label) plus `_sum` and `_count`.

| Group        | Count | Who wrote it                           |
| ------------ | ----- | -------------------------------------- |
| `go_*`       | 31    | Go runtime collector.                  |
| `process_*`  | 9     | Linux `/proc`.                         |
| `scrape_*`   | 5     | Prometheus, about the scrape itself.   |
| `promhttp_*` | 2     | The metrics endpoint measuring itself. |

**How do Prometheus alerts work end to end?**
Prometheus evaluates rule expressions every `evaluation_interval`; each series the expression returns is an alert, which goes pending, then firing once it has held for `for`. Prometheus pushes firing alerts to Alertmanager on every evaluation, and Alertmanager groups them by `group_by`, routes them through a tree to receivers, and handles silences, inhibition, and repeats.

**Your alert has `for: 5m`. How long until someone gets paged?**
Worst case scrape interval + evaluation interval + `for` + `group_wait`: on stock-ish settings 5s + 60s + 300s + 30s ≈ 6.5 minutes after the condition becomes true. `for` dominates, so a faster scrape barely helps; the lever is a smoothed expression with a shorter `for`.

**Why didn't the alert fire when the service went down?**
Because the metric went away with it: the target vanished from discovery, Prometheus marked its series stale, and the expression returned nothing, which reads as healthy (the lag alert even sent "resolved"). `up == 0` doesn't help when the target is gone, not failing; you need `absent(up{job=...})`, and ideally measure things like consumer lag from outside the process.

**Why didn't the alert fire on the first error?**
A labelled counter doesn't exist until its first increment, so it appears already at 1 and `increase()` sees no step. Initialise the series to 0 at startup for the label sets you alert on.

**What's `group_by` for, and what goes wrong without it?**
It decides which alerts share a notification. Group by the incident, not the symptom: `[alertname, job]` turns three partition alerts into one page; grouping by `partition` would page three times for one outage.

**Silence vs inhibition?**
A silence is a human muting matchers for a time window, for planned maintenance or a known issue. An inhibition is a config rule: when a source alert fires (`ServiceDown`), suppress targets that share labels (`job`), because the source explains them.

**Why is my p99 latency alert stuck at 5?**
`histogram_quantile` can't interpolate into the `+Inf` bucket and returns the highest finite bound. Keep thresholds well under the top bucket, or alert on the fraction of requests over a bucket boundary, which is exact.

**How do you test alerts?**
`promtool check rules` for syntax, `promtool test rules` with synthetic `input_series` and expected alerts at given times, and `amtool config routes test` to check which receiver a label set lands on.
