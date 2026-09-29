# Backup and Restore

- [What is inside PGDATA?](#what-is-inside-pgdata)
- [What is the difference between pg_dump and pg_basebackup?](#what-is-the-difference-between-pg_dump-and-pg_basebackup)
- [What is the difference between crash recovery and PITR?](#what-is-the-difference-between-crash-recovery-and-pitr)
- [What do you need to configure for PITR?](#what-do-you-need-to-configure-for-pitr)
- [Why is a base backup useless without WAL?](#why-is-a-base-backup-useless-without-wal)
- [What files does pg_basebackup produce?](#what-files-does-pg_basebackup-produce)
- [How big is the WAL archive?](#how-big-is-the-wal-archive)
- [What is your RPO with WAL archiving?](#what-is-your-rpo-with-wal-archiving)
- [How do you bound the RPO in time?](#how-do-you-bound-the-rpo-in-time)
- [What determines your RTO?](#what-determines-your-rto)
- [Why did the crashed primary lose nothing?](#why-did-the-crashed-primary-lose-nothing)
- [How do you recover from a DELETE with the wrong WHERE?](#how-do-you-recover-from-a-delete-with-the-wrong-where)
- [How do you find the recovery target?](#how-do-you-find-the-recovery-target)
- [How do you stop recovery right before it?](#how-do-you-stop-recovery-right-before-it)
- [Why not promote the PITR instance into production?](#why-not-promote-the-pitr-instance-into-production)
- [What is a restore drill?](#what-is-a-restore-drill)
- [What fails only at restore time?](#what-fails-only-at-restore-time)

## What is inside PGDATA?

- The whole cluster. In Postgres 18: `/var/lib/postgresql/18/docker`.
- `base/`: table and index data, one subdirectory per database.
- `global/`: cluster-wide catalogs: roles, tablespaces, list of databases.
- `pg_wal/`: write-ahead log, 16 MB segments. Every change lands here before `base/`.
- `pg_xact/`: commit status (committed/aborted) per transaction ID.
- `postgresql.conf`: main config.

## What is the difference between pg_dump and pg_basebackup?

- `pg_dump`: **logical**. Connects as a client, writes schema and rows to `.sql`. One point-in-time snapshot, nothing to replay forward from.
- `pg_basebackup`: **physical**. Copies the whole data directory. Restore target must match major version, architecture and page layout.
- `pg_basebackup` is for the two things `pg_dump` can't do: setting up a streaming replica, and PITR.

## What is the difference between crash recovery and PITR?

- **Crash recovery**: on startup after an unclean shutdown, find the last checkpoint and replay local `pg_wal/` to the end. Automatic.
- **PITR** (Point-In-Time Recovery): base backup + archived WAL, replayed up to a chosen target.
- PITR covers two cases: the data directory is gone, or it is perfectly healthy and that's the problem (bad `DELETE`).

## What do you need to configure for PITR?

- `wal_level = replica`: already the default.
- `archive_mode = on`: **off by default**. Stock Postgres keeps no WAL history; the best recovery is your last `pg_dump`.
- `archive_command`: must **never overwrite** an existing segment. A retried archive after a crash can replace a good segment with a different one of the same name, and you find out at restore time.

```bash
wal_level = replica
archive_mode = on
archive_command = 'test ! -f /archive/%f && cp %p /archive/%f'
```

## Why is a base backup useless without WAL?

- It is copied **while the database is running and changing**. Not a snapshot of a moment.
- Files are copied over several seconds, so the copy is internally inconsistent.
- WAL replay from the backup start LSN (Log Sequence Number) is what makes it consistent.
- `pg_basebackup` takes no lock that blocks writes: 120 000 rows committed during a 3.9s backup, none blocked.

## What files does pg_basebackup produce?

| file              |     size |
| ----------------- | -------: |
| `base.tar.gz`     |  24.4 MB |
| `pg_wal.tar.gz`   |   9.2 MB |
| `backup_manifest` |   0.2 MB |
| (live database)   | 108.3 MB |

- `base.tar.gz`: the data directory. 3.2x compression.
- `pg_wal.tar.gz`: the WAL generated **during** the backup, streamed with `-Xs`. Without it, the backup depends on those segments still being in the archive.
- `backup_manifest`: checksum per file. `pg_verifybackup` catches truncated uploads and half-written files **without a restore**. Cheapest check there is.

```bash
pg_basebackup -U postgres -D /backups/base -Ft -z -Xs -c fast
pg_verifybackup -n /backups/base
```

## How big is the WAL archive?

- Usually bigger than the backups: **224 MB of WAL for a 99 MB database**.
- Loading a table writes far more WAL than the table occupies, and all of it is stored until retention drops it.
- Archive retention and the archive's own durability are part of "do we have a backup".

## What is your RPO with WAL archiving?

- RPO (Recovery Point Objective): how much acknowledged data you lose.
- A segment is archived only when it fills (16 MB) or something forces a switch. `archive_timeout = 0` by default, so nothing does.
- **The open segment is not in the archive.** Lose the host and you lose it.
- Lab: SIGKILL the host after 20s of writes → **22 000 committed rows lost** (0.8s).
- The 0.8s is a property of the workload (~8 MB/s of WAL), not of Postgres. **RPO is bounded in bytes, not in time**: a quiet database writing 16 MB/hour has an RPO of an hour.

## How do you bound the RPO in time?

- `archive_timeout`: forces a switch every N seconds. Cost: a padded 16 MB segment per interval, even if empty.
- `pg_receivewal`: streams WAL continuously to the archive host. Tail near zero.
- Synchronous replication: no commit is acknowledged until a second machine has it. Tail zero.
- A more frequent base backup **doesn't** help RPO.

## What determines your RTO?

- RTO (Recovery Time Objective): how long until the database is back.
- Lab: 3.2s total. ~Half unpacking the base backup, ~half replaying WAL.
- Unpacking scales with database size. **Replay scales with WAL accumulated since the base backup**.
- Backup on Sunday, incident on Friday: five days of single-threaded replay, however small the database.
- **Backup frequency is an RTO decision** at least as much as an RPO one.

| step                                     |    time |
| ---------------------------------------- | ------: |
| lay down data dir + reach consistency    | 1717 ms |
| replay archive to end + promote          | 1517 ms |
| **RTO**                                  | 3234 ms |

## Why did the crashed primary lose nothing?

- Same SIGKILL, but the primary's volume survived: local crash recovery replays its own `pg_wal/` and returns all 1 736 000 rows.
- The **process** died: crash recovery, no loss. The **hardware** died: archive restore, lose the open segment.
- Both look identical from the application, and have completely different recovery stories.

## How do you recover from a DELETE with the wrong WHERE?

- Nothing is broken: the database is healthy and 177 600 rows are gone. Restoring "the backup" is not the answer.
- Runbook:
  1. Stop whatever is still writing to the damaged table.
  2. Force a WAL switch so the mistake reaches the archive.
  3. Find the bad transaction.
  4. Restore base backup + archived WAL into a **side instance**, stopped just before it.
  5. `pg_dump -t accounts` from the side instance, load into production under a temp name, `INSERT ... SELECT` the missing rows back.
  6. Delete the side instance.

## How do you find the recovery target?

- The hard part. Nobody hands you a target in a real incident.
- **First move is a WAL switch**, not a restore: the mistake is in the open segment, and nothing past the last archived segment can be recovered.
- Then search the WAL around the incident time for the `DELETE` on that relation.
- `pg_waldump` takes a **start and end segment**, not a list. Given a glob, it reads the first two and reports "no DELETEs found".
- An empty target is not an error: it is a restore to end of archive that faithfully replays the mistake.
- Prefer `recovery_target_xid` over `recovery_target_time`: exact, not a guess.

```sql
SELECT pg_switch_wal();
```

```bash
pg_waldump -p /archive 00000001000000000000001C 00000001000000000000001F \
  | grep 'desc: DELETE' | grep 'rel 1663/16384/16397'
```

## How do you stop recovery right before it?

- `recovery_target_inclusive` **defaults to `true`**: recovery stops **after** the target commits, and you get an exact copy of the database you're escaping.
- `recovery_target_action = 'pause'`: holds at the target, still in recovery, so you can check it.
- Wrong target? Shut down and restart with another one. Only possible because it hasn't promoted.
- Right target? `pg_wal_replay_resume()` ends recovery and promotes.
- `promote` is the default action and a **one-way door**.

```bash
recovery_target_xid = '1150'
recovery_target_inclusive = false
recovery_target_action = 'pause'
```

```
LOG:  starting point-in-time recovery to XID 1150
LOG:  recovery stopping before commit of transaction 1150
```

## Why not promote the PITR instance into production?

- **PITR recovers the cluster, not the rows.** It can't replay the good transactions and skip the bad one.
- Lab: all 177 600 victim rows back, and **20 000 good writes lost** with them (everything after the target).
- Restore-aside, extract, copy back keeps the good writes. Slower, manual, correct.
- After promotion the instance is on **timeline 2**. A later recovery must be told which history to follow.
- A promoted PITR instance must **never write into the primary's archive**.

| state             |      rows |
| ----------------- | --------: |
| before the DELETE | 1 776 000 |
| after the DELETE  | 1 598 400 |
| live now          | 1 618 400 |
| restored          | 1 776 000 |

## What is a restore drill?

- A backup you have never restored has an **unknown** state, not a good one.
- Scheduled job: restore the latest backup into a throwaway instance, check it, tear it down, exit non-zero on failure.

| check                                   | why                                                                  |
| --------------------------------------- | -------------------------------------------------------------------- |
| 1. `pg_verifybackup` against manifest   | Catches a broken backup without paying for a restore.                |
| 2. restores into a clean data directory | Proves it is a database, not a tarball.                              |
| 3. reaches end of recovery and promotes | `pg_is_in_recovery()` false. Accepting connections is not enough.    |
| 4. RTO within budget                    | Fails on a **working** system: the database grew.                    |
| 5. `pg_amcheck --heapallindexed`        | Every index entry has its heap tuple. A row count can't do this.     |
| 6. freshness within budget              | Fails on a **working** system: the base backup got old.              |

- Checks 2 and 3 are separate because `hot_standby` is on by default: the server accepts read-only connections **while still replaying**.
- `amcheck` must be installed **before the backup was taken**. Verification tooling lives inside what you back up.
- No "matches production" check: live is a moving target. Measure **freshness against the clock** instead. A drill that needs an outage is a drill nobody runs.
- The restore host must **not** mount the primary's disk. A drill that can see it proves nothing.

## What fails only at restore time?

- **File permissions**: `pg_basebackup` writes `0600`. Taken as root, the `postgres` user on the restore host can't read it.
- **`restore_command` exit codes**: Postgres trusts them completely. Exit 0 without producing a file ends recovery early and quietly: a database that starts, serves, and is missing the tail of its history.
- **Empty recovery target**: replays the mistake (see above).
- **Mid-replay database**: accepts connections before recovery is done.
- **Readiness**: `pg_isready` inside the container answers on the unix socket during initdb's temporary server, before TCP is up. Poll over TCP.
- **Incremental backups**: `pg_basebackup -i` needs `summarize_wal = on` (off by default) **before** the full backup they're based on.
