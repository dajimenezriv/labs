-- Two tables, one foreign key, and one transaction that writes both. They
-- move together, which is the only way a foreign key survives a migration:
-- a constraint cannot span two databases, so either both ends go or the
-- constraint does.
--
-- Moving them as a pair is also what keeps the write a single commit on
-- both sides of the cutover. Nothing in the application has to change.

DROP SCHEMA IF EXISTS lab CASCADE;

CREATE SCHEMA lab;

CREATE TABLE lab.orders (
  id         bigserial   PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE lab.payments (
  id         bigserial   PRIMARY KEY,
  order_id   bigint      NOT NULL REFERENCES lab.orders (id),
  created_at timestamptz NOT NULL DEFAULT now()
);

-- The index the read path uses. Section 1 is about what happens when it is
-- on one side of the migration and not the other.
CREATE INDEX ON lab.payments (order_id);

-- 200k orders, one payment each, spread over the last day. The ids come
-- from the sequences rather than from generate_series, so the sequences end
-- up where the data does.
INSERT INTO lab.orders (created_at)
SELECT now() - (i % 86400) * interval '1 second'
FROM generate_series(1, 200000) AS i;

INSERT INTO lab.payments (order_id, created_at)
SELECT id, created_at FROM lab.orders;

ANALYZE lab.orders;
ANALYZE lab.payments;

SELECT (SELECT count(*) FROM lab.orders)   AS orders,
       (SELECT count(*) FROM lab.payments) AS payments,
       (SELECT last_value FROM lab.orders_id_seq)   AS orders_seq,
       (SELECT last_value FROM lab.payments_id_seq) AS payments_seq;
