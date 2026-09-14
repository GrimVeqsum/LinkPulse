package main

import (
	"context"
	"log"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	"linkpulse/internal/config"
	"linkpulse/internal/link/handler"
	"linkpulse/internal/link/repository"
	"linkpulse/internal/link/service"
	"linkpulse/migrations"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, cfg.PostgresURL)
	if err != nil {
		log.Fatal("failed to create postgres pool: ", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatal("failed to connect to postgres: ", err)
	}

	if err := migrations.Up(ctx, pool); err != nil {
		log.Fatal("failed to run migrations: ", err)
	}

	linkRepository := repository.NewPostgresRepository(pool)
	linkService := service.New(linkRepository)
	linkHandler := handler.New(linkService)

	mux := http.NewServeMux()

	linkHandler.RegisterRoutes(mux)

	server := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: mux,
	}

	log.Printf("server started on :%s", cfg.HTTPPort)

	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
