package main

import (
	"context"
	"fmt"
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
		t.Fatal(err)
	}

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	defer pool.Close()

	queries := db.New(pool)
	site, err := queries.CreateSite(t.Context(), "Site")
	fmt.Println(site, err)
}
