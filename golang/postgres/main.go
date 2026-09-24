package main

import (
	"context"
	"fmt"
	"postgres/db"

	"github.com/jackc/pgx/v5/pgxpool"
)

const databaseURL = "postgresql://postgres:postgres@localhost:5555/db?sslmode=disable"

func main() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		panic("new pool: " + err.Error())
	}
	if err := pool.Ping(ctx); err != nil {
		panic("pool ping: " + err.Error())
	}
	defer pool.Close()

	queries := db.New(pool)
	queries.CreateSite(ctx, "Site")

	fmt.Println(queries.GetSites(ctx))
}
