-- 5M rows in a schema of its own, so the lab can be dropped without touching
-- the service tables sharing this database.
--
-- created_at is deliberately coarse: 50 rows share every second. Ties are what
-- make the correctness half of this lab real -- ORDER BY created_at alone does
-- not determine an order, so the same row can appear on two pages or on none.
DROP SCHEMA IF EXISTS lab CASCADE;

CREATE SCHEMA lab;

CREATE TABLE lab.events (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at timestamptz NOT NULL,
  device_id int NOT NULL,
  payload text NOT NULL
);

INSERT INTO
  lab.events (created_at, device_id, payload)
SELECT
  timestamptz '2025-01-01 00:00:00+00' + make_interval(secs => g / 50),
  g % 1000,
  repeat('x', 40)
FROM
  generate_series(1, 5000000) AS g;

-- The index both strategies get to use. Same columns as the ORDER BY, same
-- direction: without this, neither strategy has anything to walk and both
-- degenerate to a sort of the whole table.
CREATE INDEX events_created_at_id_desc ON lab.events (created_at DESC, id DESC);

-- Samples the table and writes statistics the query planner uses. This is run inside
-- autovacuum, but since we start benchmarking as soon as the bulk is done, the
-- autovacuum has not run yet.
ANALYZE lab.events;