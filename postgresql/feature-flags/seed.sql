-- A flag table shaped like one a service would actually read: every instance
-- holds the whole thing in memory and evaluates from the map, so the read
-- path never touches Postgres. That is the only reason any of this works --
-- a flag check is on every request, and a query per check is not a budget
-- anyone has.
--
-- Which leaves one problem, and it is the whole lab: getting a change out to
-- every instance's map.
DROP SCHEMA IF EXISTS lab CASCADE;

CREATE SCHEMA lab;

-- One monotonic counter for the whole table, not a per-row one. A listener
-- that knows its own highest version can ask "is there anything newer than
-- me?" in a single indexed lookup, which is what the watchdog in §2 does.
CREATE SEQUENCE lab.flags_version;

CREATE TABLE lab.flags (
  key text PRIMARY KEY,
  enabled boolean NOT NULL DEFAULT false,
  rollout int NOT NULL DEFAULT 0,
  -- The rules a real flag carries: allowlists, segment predicates, variant
  -- weights. Present because its size is the subject of §the payload.
  rules jsonb NOT NULL DEFAULT '{}'::jsonb,
  version bigint NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX ON lab.flags (version);

-- The notification carries the key. It does NOT carry the flag, and the
-- listener reacts by re-reading that one row.
--
-- Sending the row instead is the obvious optimisation -- the listener applies
-- it to its map with no round trip -- and it is wrong for a reason that has
-- nothing to do with speed: a payload is a statement about the past.
--
-- Delivery here is at-most-once and unordered with respect to everything
-- else the listener is doing. By the time a payload is applied, the row it
-- describes may have been updated twice more, and the listener may already
-- hold a newer version from a reconnect snapshot. Applying it then walks the
-- flag BACKWARDS -- to a value that was briefly true and is now wrong -- and
-- the instance stays there until the next change to that key, because
-- nothing will contradict it. Guarding with a version check makes that safe
-- but not useful: the correct outcome of a stale payload is to ignore it,
-- which is an announcement that carried nothing.
--
-- Re-reading has no such state. It converges on the current row no matter
-- how many notifications were missed, duplicated or delivered late, because
-- the answer comes from the table rather than from the message. The
-- notification is reduced to what it is actually good for -- a hint that
-- something changed, and a fast one -- and the code that handles it is the
-- same code that handles a resync.
--
-- It also sidesteps a ceiling. pg_notify caps the payload at 8000 bytes and
-- raises 22023 "payload string too long" above it, and since the call is
-- inside this trigger that error fails the UPDATE that fired it: a flag whose
-- rules outgrow the cap would stop being writable, not merely undeliverable.
-- A key is never going to approach 8000 bytes.
--
-- The cost is one indexed lookup per flag change per instance.
CREATE FUNCTION lab.flag_changed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.version := nextval('lab.flags_version');
  NEW.updated_at := clock_timestamp();
  PERFORM pg_notify('flags', NEW.key);
  RETURN NEW;
END $$;

-- BEFORE, so the assignments to NEW stick: the version the listener will
-- read back is the one set here.
--
-- What LISTEN/NOTIFY is, stated plainly, because both halves matter:
--
--   transactional.  The notification is queued here and delivered at COMMIT.
--                   Roll the transaction back and nothing is sent, ever.
--                   That is atomicity between the data change and its
--                   announcement, and it is the thing an outbox table exists
--                   to fake when the broker lives outside the database.
--
--   not durable.    At-most-once, with no retention, no replay and no
--                   offset. A notification published while a listener is
--                   between connections is not queued for it and not
--                   redelivered -- it is gone, and nothing on either side
--                   records that it existed. This is Redis pub/sub, not
--                   Kafka: there is no committed position to resume from,
--                   which is exactly what section 2 measures.
--
-- So NOTIFY is a latency optimisation over polling, and never the thing that
-- makes a listener correct.
CREATE TRIGGER flag_changed
BEFORE INSERT OR UPDATE ON lab.flags
FOR EACH ROW EXECUTE FUNCTION lab.flag_changed();

-- 200 flags, which is a boring number for a service that has been alive a
-- few years.
INSERT INTO lab.flags (key, enabled, rollout)
SELECT 'flag_' || to_char(g, 'FM000'), g % 3 = 0, (g * 7) % 101
FROM generate_series(1, 200) AS g;

ANALYZE lab.flags;
