-- The monolith. lab.payments is the table that leaves.

DROP SCHEMA IF EXISTS lab CASCADE;
CREATE SCHEMA lab;

CREATE TABLE lab.payments (
  id           bigserial PRIMARY KEY,
  order_id     bigint      NOT NULL,
  amount_cents bigint      NOT NULL,
  provider_ref text        NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX ON lab.payments (order_id);

-- 200k payments, for order ids 1..200000. That is the history the migration
-- has to carry across; the load generator pays 200001..500000 while it runs.
-- Contiguous on purpose: the order id is the identity of the write, so
-- "acked but absent" is a range query.
INSERT INTO lab.payments (order_id, amount_cents, provider_ref, created_at)
SELECT i,
       (i % 900 + 100) * 10,
       'seed-' || i,
       now() - (i % 86400) * interval '1 second'
FROM generate_series(1, 200000) AS i;

ANALYZE lab.payments;

SELECT count(*) AS payments,
       pg_size_pretty(pg_database_size(current_database())) AS size
FROM lab.payments;
