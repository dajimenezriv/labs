-- The keyset predicate has to be a row comparison over exactly the ORDER BY columns:
--
-- (created_at, id) < (:ts, :id)    correct
-- created_at < :ts AND id < :id    wrong
-- id < :id                         wrong
--
-- Correlation is what hides these bugs. Inserted in order, at increasing timestamps,
-- with a serial id. This issue happens when a bigger id is commited with an older
-- timestamp.

DROP TABLE IF EXISTS lab.skew;

CREATE TABLE lab.skew (id bigint PRIMARY KEY, created_at timestamptz);

-- Correct order: 3 5 1 2 4 6
-- Get page 2 (id 5 and day 05)
INSERT INTO lab.skew VALUES
  (1, '2000-01-04'), (2, '2000-01-03'), (3, '2000-01-06'),
  (4, '2000-01-02'), (5, '2000-01-05'), (6, '2000-01-01');

-- Offset: 1 2 4 6 (correct)
SELECT id FROM lab.skew ORDER BY created_at DESC OFFSET 2;

-- Row comparison: 1 2 4 6 (correct)
-- Same as WHERE created_at < '2000-01-05' OR (created_at = '2000-01-05' AND id < 5)
SELECT id FROM lab.skew
WHERE (created_at, id) < ('2000-01-05', 5)
ORDER BY created_at DESC, id DESC;

-- AND: 1 2 4 (6 is lost: behind the cursor in time, ahead of it in id)
SELECT id FROM lab.skew
WHERE created_at < '2000-01-05' AND id < 5
ORDER BY created_at DESC, id DESC;

-- Id only: 3 1 2 4 (3 was already served on page 1, and 6 is lost)
SELECT id FROM lab.skew
WHERE id < 5
ORDER BY created_at DESC, id DESC;