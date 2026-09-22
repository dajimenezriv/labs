1. [Backup recovery + WAL](backup-and-restore/backup-and-restore.md) — base backup + WAL archive, total-loss restore with RTO/RPO measured, PITR to the transaction before a bad DELETE, and an automated restore drill
2. Migrations
3. [Zero-downtime database split](zero-downtime-split/zero-downtime-split.md) — logical replication backfill of a live table, shadow reads, a 571 ms freeze/drain/flip cutover with zero lost writes, reverse-replication rollback, and the constraint and transaction that do not come back
4. [Feature flags with LISTEN/NOTIFY](feature-flags/feature-flags.md) — in-memory flag caches invalidated by a trigger, 9 ms propagation against polling's seconds, and the reconnect loop that leaves all 20 instances serving a stale kill switch forever without erroring
