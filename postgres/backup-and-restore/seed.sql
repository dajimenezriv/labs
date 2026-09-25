-- An orders table in a schema of its own, so the lab can be dropped without
-- touching anything else sharing this database.
--
-- Nothing here is about the schema. What matters for a restore lab is that
-- the table is big enough that copying it takes measurable time, and that it
-- carries a column a careless WHERE clause can be written against -- which is
-- what customer_id is for in part 3.
-- Structural verification for the drill in part 4. It has to be installed
-- here, before the backup is taken, or the restored database cannot be
-- checked -- the verification tooling has to be inside the thing you backed
-- up.
CREATE EXTENSION IF NOT EXISTS amcheck;

DROP SCHEMA IF EXISTS lab CASCADE;

CREATE SCHEMA lab;

CREATE TABLE lab.orders (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_id int NOT NULL,
  status text NOT NULL,
  amount numeric(10, 2) NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO
  lab.orders (customer_id, status, amount, created_at)
SELECT
  g % 1000,
  (ARRAY['pending', 'paid', 'shipped'])[1 + g % 3],
  (g % 10000)::numeric / 100,
  timestamptz '2025-01-01 00:00:00+00' + make_interval(secs => g)
FROM
  generate_series(1, 1000000) AS g;

CREATE INDEX orders_customer ON lab.orders (customer_id);

-- Every script in this lab compares this between the live database and the
-- restored one. A restore that starts is not a restore that is correct, and
-- "the service came up" is not a verification.
--
-- count + sum + max is cheap and catches truncation, a missed WAL segment and
-- a partial replay, which is what these drills produce. It would not catch a
-- single flipped byte in an untouched row; a real drill wants a full digest
-- or a page-checksum scan on top.
CREATE VIEW lab.fingerprint AS
SELECT
  count(*) AS rows,
  coalesce(sum(amount), 0) AS total,
  coalesce(max(id), 0) AS max_id
FROM
  lab.orders;

VACUUM ANALYZE lab.orders;
