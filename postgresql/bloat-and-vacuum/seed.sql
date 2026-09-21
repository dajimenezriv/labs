-- 1M rows in a schema of its own, so the lab can be dropped without touching
-- the service tables sharing this database.
--
-- Two columns exist only to be updated, and the difference between them is
-- the whole lab:
--
--   value    not indexed -- an update to it can be a HOT update
--   status   indexed     -- an update to it never can
--
-- MVCC means neither one is edited in place. UPDATE writes a whole new row
-- version and marks the old one dead; the row count does not move but the
-- file does.
DROP SCHEMA IF EXISTS lab CASCADE;

CREATE SCHEMA lab;

CREATE TABLE lab.readings (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sensor_id int NOT NULL,
  status text NOT NULL,
  value numeric(10, 2) NOT NULL,
  updated_at timestamptz NOT NULL
);

INSERT INTO
  lab.readings (sensor_id, status, value, updated_at)
SELECT
  g % 5000,
  'ok',
  (g % 1000)::numeric,
  timestamptz '2025-01-01 00:00:00+00' + make_interval(secs => g)
FROM
  generate_series(1, 1000000) AS g;

-- The second index is what makes a status update expensive. Every non-HOT
-- update has to add an entry to every index on the table, whether or not the
-- indexed column changed -- the new row version is at a new address, so every
-- index has to learn about it.
CREATE INDEX readings_status ON lab.readings (status);

VACUUM ANALYZE lab.readings;