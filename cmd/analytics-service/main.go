package main

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"google.golang.org/grpc"

	analyticsv1 "linkpulse/gen/analytics/v1"
	grpcapi "linkpulse/internal/analytics/grpc"
	"linkpulse/internal/analytics/repository"
	"linkpulse/internal/config"
	"linkpulse/migrations"
)

func main() {
	cfg := config.LoadAnalytics()

	ctx := context.Background()

	db := clickhouse.OpenDB(
		&clickhouse.Options{
			Addr: []string{
				cfg.ClickHouseAddr,
			},

			Auth: clickhouse.Auth{
				Database: cfg.ClickHouseDatabase,
				Username: cfg.ClickHouseUser,
				Password: cfg.ClickHousePassword,
			},

			DialTimeout: 5 * time.Second,

			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
		},
	)

	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		log.Fatal(
			"failed to connect to clickhouse: ",
			err,
		)
	}

	if err := migrations.UpClickHouse(
		ctx,
		db,
	); err != nil {
		log.Fatal(err)
	}

	clickRepository := repository.New(db)

	analyticsServer :=
		grpcapi.New(clickRepository)

	grpcServer := grpc.NewServer()

	analyticsv1.RegisterAnalyticsServiceServer(
		grpcServer,
		analyticsServer,
	)

	listener, err := net.Listen(
		"tcp",
		":"+cfg.GRPCPort,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf(
		"analytics grpc service started on :%s",
		cfg.GRPCPort,
	)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
