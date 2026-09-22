DROP SCHEMA IF EXISTS lab CASCADE;

CREATE SCHEMA lab;

CREATE TABLE lab.flags (
  key text PRIMARY KEY,
  enabled boolean NOT NULL DEFAULT false,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION lab.flag_changed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := clock_timestamp();
  PERFORM pg_notify('flags', NEW.key);
  RETURN NEW;
END $$;

CREATE TRIGGER flag_changed
BEFORE INSERT OR UPDATE ON lab.flags
FOR EACH ROW EXECUTE FUNCTION lab.flag_changed();

INSERT INTO lab.flags (key, enabled)
SELECT 'flag_' || to_char(g, 'FM000'), g % 3 = 0
FROM generate_series(1, 200) AS g;

ANALYZE lab.flags;
