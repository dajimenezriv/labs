# Hot Partition

- [The setup](#the-setup)
- [1. One tenant sends 80%, and scaling out does nothing](#1-one-tenant-sends-80-and-scaling-out-does-nothing)
  - [The partition next door lags too](#the-partition-next-door-lags-too)
- [2. Three ways out, at steady load](#2-three-ways-out-at-steady-load)
- [3. The same three when the whale spikes](#3-the-same-three-when-the-whale-spikes)
  - [Salt is not a spread, it is 16 dice](#salt-is-not-a-spread-it-is-16-dice)
  - [Ordering damage shows up exactly when you are lagging](#ordering-damage-shows-up-exactly-when-you-are-lagging)
  - [Pooling vs isolation](#pooling-vs-isolation)

```bash
docker compose up --build
```

## The setup

One broker, 6 partitions per topic. Replication has nothing to do with this: a partition is exactly as hot on three brokers.

- **100 tenants**, `t00`..`t99`. `t00` is the whale: a big customer with 400
  devices. Everyone else has 4.
- **1000 records/s** total, produced at a steady rate. Each record carries a
  sequence number per tenant and per device, assigned by the producer, so the
  consumer can tell when it processed event 7 after event 8.
- **2ms of work per record**, sequentially, in the ordinary poll loop. That is
  ~450 records/s per member.
- Every member is its own franz-go client (its own group member as far as the
  broker knows) inside one process, so latency and ordering are measured on one
  clock.

Everything is stock: franz-go's default partitioner (murmur2 on the key, the
same hash as the Java client), `cooperative-sticky`, 45s session timeout,
autocommit. The consumers join and get their partitions before production
starts, so no latency number includes the initial rebalance.

The capacity math is what makes this a lab: **3 members is ~1350/s against a
1000/s load**. Nothing below is short of consumers.

## 1. One tenant sends 80%, and scaling out does nothing

```bash
./script.sh # 1
```

Same 1000/s, same key, but `t00` sends 800 of it. Start with 3 members and go to 6
at 60s.

**t=60s, 3 members**

| partition | owner | in/s | done/s |       lag | whale |
| --------- | ----- | ---: | -----: | --------: | ----- |
| 0         | m1    |   35 |     35 |         0 |       |
| 1         | m2    |   31 |     31 |         0 |       |
| 2         | m3    |  835 |    412 | **25401** | yes   |
| 3         | m3    |   29 |     19 |   **586** |       |
| 4         | m1    |   37 |     37 |         0 |       |
| 5         | m2    |   32 |     32 |         1 |       |

It lags partition 3 since it's also consumed in owner m3.

**t=120s, 6 members**

| partition | owner | in/s | done/s |       lag | whale |
| --------- | ----- | ---: | -----: | --------: | ----- |
| 0         | m1    |   35 |     35 |         0 |       |
| 1         | m2    |   31 |     31 |         0 |       |
| 2         | m3    |  836 |    433 | **49623** | yes   |
| 3         | m4    |   28 |     43 |         0 |       |
| 4         | m5    |   36 |     36 |         0 |       |
| 5         | m6    |   33 |     33 |         0 |       |

The lag slope on partition 2:

| members | total capacity | lag growth on p2 |
| ------: | -------------: | ---------------: |
|       3 |        ~1350/s |        **423/s** |
|       6 |        ~2700/s |        **404/s** |

We added 167% more consumers and the lag slope dropped 4%. That 4% is m3 no
longer sharing its time with partition 3.

- **A partition is consumed by exactly one member of a group.** That is what
  keeps it ordered. So the partition's throughput ceiling is one member's
  throughput, whatever the group size.
- **Members beyond the partition count are idle.** m7 and m8 joined, triggered
  a rebalance, and got nothing. Autoscaling on lag would keep adding them.
- `cooperative-sticky` left partition 2 on m3 through both scale-outs. It moved
  the cold partitions to the new members, which is correct and changes nothing.

The dashboard's "Lag by owning member" panel shows it in one line: all of the
group's lag belongs to m3.

### The partition next door lags too

At 3 members, partition 3 had **586** records of lag and received 29/s while
processing 19/s. It has nothing to do with the whale. It just shares a member
with partition 2, and the poll loop processes one member's fetches in sequence,
so its records wait behind whatever the hot partition's fetch returned.

## 2. Three ways out, at steady load

```bash
./script.sh # 2
```

6 members in every row. Route splits them: 4 on the whale's own topic, 2 on
everyone else's. 90s each.

| variant                | max lag | whale p99 | small p99 | small p99, worst partition | device order breaks | tenant order breaks (whale / small) |
| ---------------------- | ------: | --------: | --------: | -------------------------: | ------------------: | ----------------------------------: |
| key=tenant             |   36497 |     43.2s |     39.4s |                      43.2s |                   0 |                               0 / 0 |
| key=t00/device         |       3 |      19ms |      21ms |                       21ms |                   0 |                           44829 / 0 |
| key=t00#salt           |       2 |      21ms |      22ms |                       25ms |                 262 |                           44781 / 0 |
| route t00 to own topic |       0 |      25ms |      19ms |                       19ms |                   0 |                           46091 / 0 |

- **key=tenant** is section 2 again. The small tenants' p99 is 39s even though
  5 of 6 partitions keep up: 18 of the 99 small tenants hash to partition 2,
  which is far more than 1% of small traffic, so their latency _is_ the p99.
  Those tenants did nothing wrong except share a hash bucket with a big customer.
- **Change the key** (`tenant/device`, for everyone): lag gone, and the
  ordering you had becomes ordering per device. The whale's per-tenant order
  broke 44829 times, which is expected once 400 devices run in parallel. The
  small tenants lost it too: **28** breaks. That is the cost of changing the key
  for everyone to fix one customer.
- **Salt the hot key only** (`t00#0`..`t00#15`): lag gone, small tenants keep
  tenant order. The whale loses even **device** order (262): two events from the
  same device can take different salts, land on different partitions, and get
  processed in the wrong order.
- **Route the whale to its own topic**, keyed by device there: lag gone, device
  order intact, and the 99 other tenants keep per-tenant ordering untouched.

At steady load, the three fixes look like a pure ordering tradeoff. They are
not, and it takes a spike to show it.

## 3. The same three when the whale spikes

```bash
./script.sh # 3
```

Same rows, 120s each, and the whale's traffic goes 4x between 30s and 60s:
3200 + 200 = 3400/s against ~2700/s of consumer capacity. This isn't a tuned
failure. It's a backfill, a bulk import, or a retry storm on the customer's
side.

| variant                | max lag at end | whale p99 | small p99 | small p99, worst partition | device order breaks | tenant order breaks (whale / small) |
| ---------------------- | -------------: | --------: | --------: | -------------------------: | ------------------: | ----------------------------------: |
| key=tenant             |         120793 |   1m20.8s |       45s |                    1m18.8s |                   0 |                               0 / 0 |
| key=t00/device         |              1 |     13.6s |     12.3s |                      14.3s |                   0 |                          129260 / 0 |
| key=t00#salt           |          10009 |     42.2s |     41.3s |                        43s |           **83707** |                          130721 / 0 |
| route t00 to own topic |           6778 |       48s |  **19ms** |                   **19ms** |                   0 |                          116314 / 0 |

### Salt is not a spread, it is 16 dice

Salted, the whale lagged 3x longer than keyed by device, on the same 6 members.
Running Kafka's murmur2 on the 16 salted keys shows why:

| partition    |   0 |     1 |   2 |   3 |   4 |   5 |
| ------------ | --: | ----: | --: | --: | --: | --: |
| `t00#N` keys |   1 | **5** |   3 |   2 |   3 |   2 |
| `t00/dNNN`   |  60 |    59 |  76 |  69 |  69 |  67 |

Partition 1 gets 5/16 of the whale: 250/s at steady load, which fits under the
~450/s ceiling, and 1000/s during the spike, which doesn't. Partition 0 gets
1/16. Salting spreads a key over `buckets` keys and then hashes those, and 16
keys into 6 partitions is a small sample. 400 device keys are a big one.

More buckets make it more even. They also split the whale's traffic into more
parallel streams, which makes the next problem worse.

### Ordering damage shows up exactly when you are lagging

Device order breaks under salt: **262** at steady load, **83707** during the
spike. Small tenants' tenant-order breaks under the device key: **28**, then
**4255**.

A break needs one event to overtake an earlier one for the same key on a
different partition. When every partition runs at 20ms, a whale device's events are
~500ms apart and nothing overtakes. When one partition is 40 seconds behind
and another is current, everything does. So the tradeoff is invisible in
testing and in normal operation, and it goes off during the incident, when
nobody is looking at correctness.

### Pooling vs isolation

- **Device key** (pooled): every tenant shares all 6 members. The spike
  exceeded total capacity, so **everyone** lagged: small tenants at 12.3s p99.
  But it drained fastest (max lag 1 by the end), because the whale got all the
  capacity there was.
- **Route** (isolated): the whale had 4 members (~1800/s) for 3200/s, so the
  whale lagged longer (48s p99, 6778 still queued). The other 99 tenants never
  noticed: **19ms**, the same as steady state.

Neither is better in general. Pooling maximizes throughput, while isolation
caps the blast radius at the tenant who caused it. Routing costs a second
topic, a second consumer deployment, and a list of which keys are whales that
someone has to maintain.
