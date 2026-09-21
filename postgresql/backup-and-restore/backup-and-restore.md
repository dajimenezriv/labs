# Backup and Restore

- [The setup](#the-setup)
- [1. A base backup is half a backup](#1-a-base-backup-is-half-a-backup)
  - [What is actually in it](#what-is-actually-in-it)
  - [The segment that is not in the archive](#the-segment-that-is-not-in-the-archive)
- [2. The host is gone](#2-the-host-is-gone)
  - [RTO (Recovery Time Objective): where the time goes](#rto-recovery-time-objective-where-the-time-goes)
  - [RPO (Recovery Point Objective): what did not come back](#rpo-recovery-point-objective-what-did-not-come-back)
  - [The control](#the-control)
- [3. The DELETE with the wrong WHERE](#3-the-delete-with-the-wrong-where)
  - [Finding the transaction](#finding-the-transaction)
  - [Stopping before it](#stopping-before-it)
  - [What PITR costs to use](#what-pitr-costs-to-use)
- [4. The drill](#4-the-drill)
- [What this costs you](#what-this-costs-you)
- [Notes](#notes)

A backup is a claim. A restore is the only test of the claim, and until you
have run one you do not have a backup system, you have a directory that files
accumulate in.

```bash
docker compose up
psql postgres://postgres:postgres@localhost:5556/db -f seed.sql
```

```bash
./backup.sh     # what a base backup is and what it costs
./restore.sh    # total loss: RTO and RPO, measured
./pitr.sh       # recover to the instant before a bad DELETE
./drill.sh      # the automated restore drill, exits non-zero on failure
```

```bash
docker compose exec postgres bash
```

In Postgres 18 `PGDATA` is moved to `/var/lib/postgresql/18/docker`. The whole cluster. Inside:

- `base/`: the actual data and index data. One subdirectory per database.
- `global/`: cluster-wide catalogs shared by every database: roles, tablespaces, databases list.
- `pg_wal/`: write-ahead log segments, 16 MB each by default. Every change is written here before it touches `base/`.
- `pg_xact/`: transaction commit status (committed/aborted) for each transaction ID.
- `postgresql.conf`: main config.

Commands:

- `pg_dump`: logical backup. It connects as a normal client, reads schema and rows and writes to a `.sql`. A dump is also a single point-in-time snapshot, there's no way to replay forward from it.
- `pg_basebackup` is a physical backup. It copies the entire data directory. The restore target must match major version, architecture and page layout. In services for the two things `pg_dump` can't do — setting up a streaming replica, and point-in-time recovery.

Operations:

- **Crash recovery**: on startup Postgres notices it wasn't shut down cleanly, finds the last checkpoint and replays the WAL in `pg_wal/` forward to the end.
- **PITR**: needs a base backup plus archived WAL. It handles the case where the data directory isn't there or when the data directory is perfectly healthy and that's a problem.

## The setup

- 1M rows, 91 MB table, 99 MB database.
- Two containers. `postgres` is the primary on 5556. `restore` is an idle
  container with no server running — the scripts start one on it by hand with
  `pg_ctl`, because that is what a restore is.
- Four volumes, and the split matters: `postgres-data`, `restore-data`,
  `backups` and `archive`. The restore container mounts `backups` and
  `archive` and **not** `postgres-data`. A restore drill that can see the
  primary's disk proves nothing.

The only non-default settings are the ones that make PITR possible at all:

```bash
wal_level = replica # this is set by default
archive_mode = on
archive_command = 'test ! -f /archive/%f && cp %p /archive/%f'
```

`wal_level` stays at its default of `replica`, which is already enough.
`archive_mode` is not: it ships **off**, so a stock Postgres keeps no WAL
history whatsoever and the best recovery it can offer is whatever your last
`pg_dump` said. `archive_timeout` also stays at its default of `0`, and §2
measures what that costs.

The `test ! -f` guard in the archive command is load-bearing. Archiving must
never overwrite an existing segment — a retried archive after a crash can
otherwise replace a good segment with a different one carrying the same name,
and the corruption is not discovered until a restore needs that segment.

## 1. A base backup is half a backup

```bash
./backup.sh
```

A base backup is a physical copy of the data directory taken **while the
database is running and changing**. It is not a snapshot of a moment. Files
are copied over a span of seconds and the copy is internally inconsistent when
it lands; WAL replay from the backup start LSN (Log Sequence Number) is what reconciles it. That is
the whole reason a base backup without its WAL is useless.

The lab keeps a writer running for the duration to make the point:

```
rows before backup:        1000000
rows after backup:         1120000
committed during backup:   120000   <- none of them blocked
backup took:               3915 ms
```

120 000 rows committed while the backup ran. `pg_basebackup` takes no lock
that blocks writes, which is why it is safe to run against a primary — and
also why the copy needs replay before it means anything.

### What is actually in it

| file              |     size |
| ----------------- | -------: |
| `base.tar.gz`     |  24.4 MB |
| `pg_wal.tar.gz`   |   9.2 MB |
| `backup_manifest` |   0.2 MB |
| (live database)   | 108.3 MB |

3.2x compression, and three files that do different jobs:

- **`base.tar.gz`** is the data directory.
- **`pg_wal.tar.gz`** exists because of `-Xs`, which streams the WAL generated
  _during_ the backup alongside it. Without `-X`, the backup cannot reach
  consistency on its own and depends entirely on those segments still being in
  the archive.
- **`backup_manifest`** is a checksum per file. `pg_verifybackup` reads it and
  catches the truncated upload and the half-written file **without a restore**,
  which makes it the cheapest check in the lab. It is check 1 of the drill.

### The segment that is not in the archive

```
segments archived        15
failed                   0
archive on disk          224.0 MB
current segment          00000001000000000000000F
```

Two things worth reading twice.

**224 MB of archived WAL for a 99 MB database.** Loading a table writes far
more WAL than the table occupies, and every byte of it ships to the archive
and is stored there until retention lets it go. The archive is not a rounding
error next to the backups, it is usually the larger line item.

**`current segment` is not in the archive and will not be until it fills.**
Segments are archived when they are complete — 16 MB — or when something
forces a switch. `archive_timeout` is `0` by default, so nothing forces one.
Every transaction committed into the open segment is outside the backup, and
§2 is what that costs.

## 2. The host is gone

```bash
./restore.sh
```

20 seconds of writes after the backup, each line of the writer's log appended
only **after `COMMIT` returned** — so the log is the list of transactions the
application was told were durable. Then `docker compose kill`: SIGKILL, not a
shutdown, because power loss is the failure worth drilling and nothing gets
flushed or archived on the way out.

### RTO (Recovery Time Objective): where the time goes

```
lay down data dir + reach consistency      1717 ms
replay archive to end + promote            1517 ms
RTO (total)                                3234 ms
```

Roughly half the time is unpacking 26 MB and half is replaying WAL. Only the
first half scales with database size. The second scales with **how much WAL
has accumulated since the base backup**, which is the number that actually
determines your RTO in production: a base backup from Sunday and an incident
on Friday means replaying five days of WAL, single-threaded, no matter how
small the database is. Backup frequency is an RTO decision at least as much as
an RPO one.

### RPO (Recovery Point Objective): what did not come back

|        | acknowledged |  restored |
| ------ | -----------: | --------: |
| rows   |    1 736 000 | 1 714 000 |
| max id |    1 736 000 | 1 714 000 |

```
rows lost:                 22000
RPO (wall clock):          0.8 s of acknowledged commits
```

**22 000 rows that the database said were committed did not survive.** Not a
bug, not a misconfiguration — that is what archive-based backup _is_. Those
commits were durable in the open segment on the primary's disk, and the open
segment never reached the archive.

The 0.8 s is the number to be careful with, because it is a property of this
workload, not of Postgres. This lab writes ~8 MB/s of WAL, so a 16 MB segment
fills in about two seconds and the unarchived tail is always small. **The gap
is bounded in bytes, not in time.** A quiet database writing 16 MB of WAL per
hour has exactly the same one-segment exposure and an RPO of an hour. The
fixes bound it in time instead:

- `archive_timeout` forces a switch every N seconds — at the price of a
  padded 16 MB segment per interval, whether or not there is anything in it.
- `pg_receivewal` streams WAL continuously to the archive host instead of
  shipping completed segments, cutting the tail to near zero.
- Synchronous replication removes it entirely, by refusing to acknowledge a
  commit until a second machine has it.

### The control

```
rows after crash recovery: 1736000
```

The primary's volume survived the SIGKILL, so when it comes back its own local
WAL replay returns all 1 736 000 rows. Nothing was lost. That is a genuinely
different guarantee from the one above — it is the guarantee you have when the
_process_ died, and precisely the one you do not have when the _hardware_ did.
Those two failures feel identical from the application's side and have
completely different recovery stories.

## 3. The DELETE with the wrong WHERE

1. Stop whatever is still writing to the damaged table (or just accept the mess depending).
2. Spin up a fresh Postgres somewhere else. Restore the base backup + archived WAL into it with `recovery_target_time` (or better, `recovery_target_xid`) set to just before the bad transaction.
3. That scratch instance now holds the table as it looked at 14:29:59.
4. `pg_dump -t accounts` from the scratch instance, load it into production under a temp name, INSERT ... SELECT the missing rows back into the live table.
5. Delete the scratch instance.

```bash
./pitr.sh
```

Nothing is broken here. The data directory is healthy, the database is
serving, and 177 600 rows are gone because a `DELETE` did exactly what it was
told. Restoring "the backup" is not the answer — the answer is the database as
it existed one transaction earlier.

### Finding the transaction

This is the hard part, and it is the part most write-ups skip. Recovery needs
a target, and in a real incident nobody hands you one.

The first move is not a restore. It is forcing a WAL switch, because the
mistake is in the open segment and **nothing can be recovered past the last
archived segment**:

```
forced a WAL switch; 00000001000000000000001F is now in the archive
```

Then find it in the WAL, narrowing to the segment range around the incident:

```bash
pg_waldump -p /archive 00000001000000000000001C 00000001000000000000001F \
  | grep 'desc: DELETE' | grep 'rel 1663/16384/16397'
```

```
scanning 00000001000000000000001C .. 00000001000000000000001F
transaction found:         1150
matches ground truth:      yes
```

The script records the real xid at the time the `DELETE` runs and then
discovers it the way an on-call engineer would, purely so the two can be
compared. They match.

`pg_waldump` takes a **start segment and an end segment**, not a list of
files. Handed a glob it silently reads the first two and reports on those,
which looks exactly like "no DELETEs found" — and an empty recovery target is
not an error, it is a restore-to-end-of-archive that faithfully replays the
mistake. The script refuses to restore without a target for this reason.

### Stopping before it

```
recovery_target_xid = '1150'
recovery_target_inclusive = false
recovery_target_action = 'pause'
```

```
LOG:  starting point-in-time recovery to XID 1150
LOG:  consistent recovery state reached at 0/1B000120
LOG:  recovery stopping before commit of transaction 1150
```

**`recovery_target_inclusive` defaults to `true`.** Left alone, recovery stops
_after_ the target commits, and you have spent your outage window producing an
exact copy of the database you were trying to escape.

**`recovery_target_action = 'pause'`** stops at the target and holds there,
still in recovery, so the target can be checked before it is made permanent:

```
paused at target after 2382 ms, still in recovery

rows, paused at target                1776000
  of which customer_id<100             177600
```

All 177 600 rows are there. `pg_wal_replay_resume()` then ends recovery and
promotes. If the target had been wrong, the instance gets shut down and
restarted with a different one — cheap, and only possible because it had not
promoted yet. `promote` is the default and it is a one-way door.

### What PITR costs to use

| state             |      rows |
| ----------------- | --------: |
| before the DELETE | 1 776 000 |
| after the DELETE  | 1 598 400 |
| live now          | 1 618 400 |
| restored          | 1 776 000 |

```
victim rows recovered:     177600 / 177600
good writes lost with it:  20000   <- everything after the target, mistake or not
```

Every victim row is back. So are the 20 000 rows of ordinary traffic that
committed after the mistake — as in, they are back to not existing.

**Recovery is a time machine for the whole cluster, not a surgical tool.** It
cannot replay the good transactions and skip the bad one; "before the DELETE"
means before everything else too. The real runbook almost never promotes this
instance into production. It restores to a side instance, `pg_dump`s the 177
600 rows out of it, and copies them back into the live database, which keeps
the 20 000 good writes. That is slower, more manual, and correct.

After promotion the instance is on **timeline 2**. The archive still holds
timeline 1, and a later recovery has to be told which history to follow. This
is also why a promoted PITR instance must never write into the archive the
primary is using.

## 4. The drill

```bash
./drill.sh
```

The part that makes the other three worth having: restore the latest backup
into a throwaway instance, check it, report, tear it down, exit non-zero on
any failure. This is the thing you put on a schedule.

```
  [1/6] backup verifies against its manifest       PASS  backup successfully verified
  [2/6] restores into a clean data directory       PASS  2188 ms
  [3/6] reaches end of recovery and promotes       PASS  53 ms
  [4/6] RTO within 60s budget                      PASS  2.2 s
  [5/6] structural check (pg_amcheck)              PASS  0 lines of complaint
  [6/6] freshness within 300s budget               PASS  15s behind

  restored rows        1618400
  RTO                  2.2 s
  RPO                  15 s

DRILL PASSED
```

What each check is actually for:

- **1** catches a broken backup without paying for a restore.
- **2 and 3** are the only checks that prove the backup is a database rather
  than a tarball. Note that they are separate: `hot_standby` is on by default,
  so the server accepts read-only connections _while it is still replaying_.
  "It accepted a connection" is not "the restore is done", and
  `pg_is_in_recovery()` going false is.
- **4 and 6** are the checks that fail on a **working** backup system, which
  is what makes them worth having. Nothing is broken when the RTO budget is
  blown; the database simply grew, or the base backup got older, and the
  restore now takes longer than the business agreed to. That is the failure
  you want to find on a Tuesday.
- **5** is the one a row count cannot do. `pg_amcheck --heapallindexed` walks
  every btree and confirms each index entry has the heap tuple it claims to.
  It needs the `amcheck` extension **installed before the backup was taken** —
  verification tooling has to be inside the thing you backed up.

The check that is deliberately absent is "matches production". Live is a
moving target and will never equal a restore taken seconds earlier, so the
drill measures **freshness against the clock** instead: how far behind the
restore landed, against the budget. Comparing to live only works if you
quiesce writes, and a drill that requires an outage is a drill nobody runs.

## What this costs you

- **The archive is not an optional extra, it is half the backup.** A base
  backup without the WAL that covers it is an inconsistent file copy. `-Xs`,
  archive retention and the archive's own durability are all part of "do we
  have a backup", and the archive is usually the bigger storage line —
  224 MB of WAL for a 99 MB database here.
- **RPO is bounded in bytes, not in time.** The exposure is "whatever is in
  the open segment", which is 0.8 s under this lab's write rate and an hour
  on a quiet database. If the number you promise is in seconds, you need
  `archive_timeout` or `pg_receivewal`, not a more frequent base backup.
- **RTO is mostly WAL replay, so backup frequency is an RTO decision.** A
  bigger database costs you in the copy; a _staler base backup_ costs you in
  the replay, and that is the half that grows every day you do not take one.
- **Finding the recovery target is the hard part of PITR.** The restore itself
  is three settings. Knowing which transaction to stop before, and getting the
  last segment into the archive before you start, is the actual work — and
  `recovery_target_inclusive` defaults to replaying the thing you are running
  from.
- **PITR recovers the cluster, not the rows.** Everything committed after the
  target is gone too. For a bad `DELETE` the real runbook is restore-aside,
  extract, copy back.
- **A backup you have never restored has an unknown state, not a good one.**
  Every failure this lab produces — unreadable file permissions, a silently
  empty recovery target, a database that came up mid-replay — reports success
  at backup time and only fails at restore time. The drill is what converts
  that into a Tuesday morning email.

## Notes

- `pg_basebackup` writes its output `0600`. A backup taken as root inside the
  container is one the `postgres` user on the restore host cannot read — a
  backup that fails only at restore time. Both containers run as uid 70 here.
- `pg_isready` inside the container answers on the unix socket, which initdb's
  temporary server is already listening on during first boot. It reports ready
  before the database accepts TCP connections; poll over TCP from the host
  instead.
- `restore_command` is a shell command and Postgres trusts its exit code
  completely. A command that exits 0 without producing a file ends recovery
  early and quietly, and the result is a database that starts, serves, and is
  missing the tail of its history.
- `summarize_wal` is off by default, and it is what `pg_basebackup -i` needs
  for incremental backups. It has to be on _before_ the full backup that the
  incrementals will be based on.
