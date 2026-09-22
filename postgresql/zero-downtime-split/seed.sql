DROP SCHEMA IF EXISTS lab CASCADE;

CREATE SCHEMA lab;

CREATE TABLE lab.payments (
  id         bigserial   PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX ON lab.payments (created_at);

-- 200k rows.
INSERT INTO lab.payments (created_at)
SELECT now() - (i % 86400) * interval '1 second'
FROM generate_series(1, 200000) AS i;

ANALYZE lab.payments;
