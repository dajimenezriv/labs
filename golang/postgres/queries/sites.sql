-- name: CreateSite :one
INSERT INTO
  sites (name)
VALUES
  (@name) RETURNING *;

-- name: GetSites :many
SELECT
  *
FROM
  sites;