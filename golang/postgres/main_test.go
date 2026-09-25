package main

import (
	"context"
	"log"
	"postgres/db"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()

	ctr, err := postgres.Run(ctx, "postgres:18-alpine",
		postgres.WithDatabase("db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithInitScripts("./migrations/000001_init.up.sql"),
		postgres.BasicWaitStrategies())
	if err != nil {
		log.Fatalf("postgres run: %v", err)
	}
	defer testcontainers.TerminateContainer(ctr)

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("new pool: %v", err)
	}
	defer pool.Close()

	m.Run()
}

func setup(t *testing.T) *db.Queries {
	t.Cleanup(func() {
		_, err := pool.Exec(context.Background(), "TRUNCATE sites RESTART IDENTITY CASCADE")
		if err != nil {
			t.Fatalf("truncate: %v", err)
		}
	})
	return db.New(pool)
}

func TestGetSites(t *testing.T) {
	queries := setup(t)
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

func TestGetSiteByID(t *testing.T) {
	queries := setup(t)
	site, err := queries.CreateSite(t.Context(), "Site")
	if err != nil {
		t.Fatalf("create site error: %v", err)
	}
	if got := site.ID; got != 1 {
		t.Errorf("siteID = %d, want %d", got, 1)
	}

	site, err = queries.GetSiteById(t.Context(), 1)
	if err != nil {
		t.Fatalf("get site by id error: %v", err)
	}
	if got, want := site.Name, "Site"; got != want {
		t.Errorf("len sites = %s, want %s", got, want)
	}
}
