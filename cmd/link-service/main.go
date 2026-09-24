package main

import (
	"context"
	"log"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	analyticsclient "linkpulse/internal/analytics/client"
	"linkpulse/internal/config"
	"linkpulse/internal/link/handler"
	"linkpulse/internal/link/repository"
	"linkpulse/internal/link/service"
	"linkpulse/migrations"
)

func main() {
	cfg, err := config.LoadLink()
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	pool, err := pgxpool.New(
		ctx,
		cfg.PostgresURL,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatal(err)
	}

	if err := migrations.UpPostgres(
		ctx,
		pool,
	); err != nil {
		log.Fatal(err)
	}

	grpcConnection, err := grpc.NewClient(
		cfg.AnalyticsGRPCAddr,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer grpcConnection.Close()

	analyticsClient := analyticsclient.New(
		grpcConnection,
	)

	linkRepository :=
		repository.NewPostgresRepository(pool)

	linkService :=
		service.New(linkRepository)

	linkHandler := handler.New(
		linkService,
		analyticsClient,
	)

	mux := http.NewServeMux()

	linkHandler.RegisterRoutes(mux)

	server := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: mux,
	}

	log.Printf(
		"link service started on :%s",
		cfg.HTTPPort,
	)

	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
