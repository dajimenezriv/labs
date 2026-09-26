-- name: CreateOutboxEvent :one
INSERT INTO
  outbox (
    key,
    payload,
    trace_context
  )
VALUES
  (
    @key,
    @payload,
    @trace_context
  ) RETURNING *;

-- name: GetOutboxEvents :many
SELECT
  *
FROM
  outbox
WHERE
  published_at IS NULL
ORDER BY
  id
LIMIT
  $1 FOR
UPDATE
  SKIP LOCKED;