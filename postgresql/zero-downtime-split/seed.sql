-- The monolith. Orders stay, payments leave.
--
-- The two tables are joined by a foreign key and written by one transaction,
-- which is the whole difficulty: the seam runs through a constraint and
-- through a commit, and neither survives the split.

DROP SCHEMA IF EXISTS lab CASCADE;
CREATE SCHEMA lab;

CREATE TABLE lab.orders (
  id           bigserial PRIMARY KEY,
  customer_id  bigint      NOT NULL,
  amount_cents bigint      NOT NULL,
  status       text        NOT NULL DEFAULT 'pending',
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE lab.payments (
  id           bigserial PRIMARY KEY,
  order_id     bigint      NOT NULL REFERENCES lab.orders (id),
  amount_cents bigint      NOT NULL,
  provider_ref text        NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX ON lab.payments (order_id);

-- 500k orders. The first 200k are already paid, which is the history the
-- migration has to carry across; the rest are what the load generator pays
-- while the migration is running. Contiguous on purpose: the order id is the
-- identity of the write, so "acked but absent" is a range query.
INSERT INTO lab.orders (customer_id, amount_cents, status, created_at)
SELECT i,
       (i % 900 + 100) * 10,
       CASE WHEN i <= 200000 THEN 'paid' ELSE 'pending' END,
       now() - (i % 86400) * interval '1 second'
FROM generate_series(1, 500000) AS i;

INSERT INTO lab.payments (order_id, amount_cents, provider_ref, created_at)
SELECT id, amount_cents, 'seed-' || id, created_at
FROM lab.orders
WHERE status = 'paid';

ANALYZE lab.orders;
ANALYZE lab.payments;

SELECT (SELECT count(*) FROM lab.orders)   AS orders,
       (SELECT count(*) FROM lab.payments) AS payments,
       pg_size_pretty(pg_database_size(current_database())) AS size;
