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

CREATE INDEX ON lab.payments (order_id);

-- 200k orders.
INSERT INTO lab.orders (created_at)
SELECT now() - (i % 86400) * interval '1 second'
FROM generate_series(1, 200000) AS i;

INSERT INTO lab.payments (order_id, created_at)
SELECT id, created_at FROM lab.orders;

ANALYZE lab.orders;
ANALYZE lab.payments;
