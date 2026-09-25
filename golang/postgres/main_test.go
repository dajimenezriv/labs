package main

import (
	"context"
	"errors"
	"log"
	"os"
	"postgres/db"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()

	ctr, err := postgres.Run(ctx, "postgres:18-alpine",
		postgres.WithDatabase("db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithInitScripts("./migrations/000001_init.up.sql"),
		postgres.BasicWaitStrategies())
	defer testcontainers.TerminateContainer(ctr)
	if err != nil {
		log.Printf("postgres run: %v", err)
		return 1
	}

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Printf("connection string: %v", err)
		return 1
	}

	pool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		log.Printf("new pool: %v", err)
		return 1
	}
	defer pool.Close()

	return m.Run()
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
	if _, err := queries.CreateSite(t.Context(), "Site"); err != nil {
		t.Fatalf("CreateSite: %v", err)
	}

	sites, err := queries.GetSites(t.Context())
	if err != nil {
		t.Fatalf("GetSites: %v", err)
	}
	if got, want := len(sites), 1; got != want {
		t.Errorf("len(GetSites()) = %d, want %d", got, want)
	}
}

func TestGetSiteByID(t *testing.T) {
	queries := setup(t)
	created, err := queries.CreateSite(t.Context(), "Site")
	if err != nil {
		t.Fatalf("CreateSite: %v", err)
	}

	got, err := queries.GetSiteById(t.Context(), created.ID)
	if err != nil {
		t.Fatalf("GetSiteById: %v", err)
	}
	if got != created {
		t.Errorf("GetSiteById() = %+v, want %+v", got, created)
	}
}

func TestGetSiteByID_NotFound(t *testing.T) {
	queries := setup(t)

	_, err := queries.GetSiteById(t.Context(), 1)
	if got, want := err, pgx.ErrNoRows; !errors.Is(got, want) {
		t.Errorf("GetSiteById() error = %v, want %v", got, want)
	}
}
