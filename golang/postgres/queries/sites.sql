-- name: CreateSite :one
INSERT INTO
  sites (name)
VALUES
  (@name) RETURNING *;

-- name: GetSites :one
SELECT
  *
FROM
  sites;