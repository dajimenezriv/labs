-- A workload that is CPU-bound on the server and small enough to stay in
-- shared_buffers. Both properties matter:
--
--   in cache   -- so the pool-size sweep measures scheduling, not disk. A
--                 disk-bound query hides the knee, because the backend spends
--                 its time blocked rather than on a core.
--   CPU-bound  -- so the ceiling is "how many cores does Postgres have", which
--                 is the number pool sizing is actually about.
DROP SCHEMA IF EXISTS lab CASCADE;

CREATE SCHEMA lab;

CREATE TABLE lab.accounts (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner int NOT NULL,
  balance numeric(12, 2) NOT NULL
);

INSERT INTO
  lab.accounts (owner, balance)
SELECT
  g % 10000, (g % 10000)::numeric / 10
FROM
  generate_series(1, 50000) AS g;

-- No index on balance, on purpose. The sequential scan is the point: one
-- backend, one core, a few milliseconds of pure CPU per call.
ANALYZE lab.accounts;