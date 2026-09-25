package main

import (
	"context"
	"postgres/db"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestQueries(t *testing.T) {
	ctx := context.Background()

	ctr, err := postgres.Run(ctx, "postgres:18-alpine",
		postgres.WithDatabase("db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithInitScripts("./migrations/000001_init.up.sql"),
		postgres.BasicWaitStrategies())
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("postgres run: %v", err)
	}

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	defer pool.Close()

	queries := db.New(pool)
	site, err := queries.CreateSite(t.Context(), "Site")
	if err != nil {
		t.Fatalf("create site error: %v", err)
	}
	if got := site.ID; got != 1 {
		t.Errorf("siteID = %d, want %d", got, 1)
	}

	sites, err := queries.GetSites(t.Context())
	if err != nil {
		t.Fatalf("get sites error: %v", err)
	}
	if got, want := len(sites), 1; got != want {
		t.Errorf("len sites = %d, want %d", got, want)
	}
}
