package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	analyticsv1 "linkpulse/gen/analytics/v1"
	grpcapi "linkpulse/internal/analytics/grpc"
	"linkpulse/internal/analytics/repository"
	"linkpulse/internal/config"
	"linkpulse/migrations"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("analytics service stopped", "error", err)
		os.Exit(1)
	}
}

// dependencyHealth checks the current database state instead of a stale startup flag.
type dependencyHealth struct {
	healthv1.UnimplementedHealthServer
	ping     func(context.Context) error
	draining atomic.Bool
}

func (h *dependencyHealth) Check(ctx context.Context, request *healthv1.HealthCheckRequest) (*healthv1.HealthCheckResponse, error) {
	if request.GetService() != "" && request.GetService() != "analytics.v1.AnalyticsService" {
		return nil, status.Error(codes.NotFound, "unknown service")
	}
	if h.draining.Load() || h.ping(ctx) != nil {
		return &healthv1.HealthCheckResponse{Status: healthv1.HealthCheckResponse_NOT_SERVING}, nil
	}
	return &healthv1.HealthCheckResponse{Status: healthv1.HealthCheckResponse_SERVING}, nil
}

func run(ctx context.Context) error {
	cfg, err := config.LoadAnalytics()
	if err != nil {
		return err
	}
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		connection, err := grpc.NewClient("127.0.0.1:"+cfg.GRPCPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer connection.Close()
		checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		response, err := healthv1.NewHealthClient(connection).Check(checkCtx, &healthv1.HealthCheckRequest{})
		if err != nil {
			return err
		}
		if response.GetStatus() != healthv1.HealthCheckResponse_SERVING {
			return errors.New("analytics dependency is not serving")
		}
		return nil
	}
	startupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr:        []string{cfg.ClickHouseAddr},
		Auth:        clickhouse.Auth{Database: cfg.ClickHouseDatabase, Username: cfg.ClickHouseUser, Password: cfg.ClickHousePassword},
		DialTimeout: 3 * time.Second, ReadTimeout: 3 * time.Second,
		Settings:    clickhouse.Settings{"max_execution_time": 2},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	defer db.Close()
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.PingContext(startupCtx); err != nil {
		return fmt.Errorf("connect clickhouse: %w", err)
	}
	if err := migrations.UpClickHouse(startupCtx, db); err != nil {
		return err
	}
	clickRepository := repository.New(db)
	analyticsServer := grpcapi.New(clickRepository)
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(32<<10), grpc.MaxConcurrentStreams(128))
	analyticsv1.RegisterAnalyticsServiceServer(grpcServer, analyticsServer)
	health := &dependencyHealth{ping: clickRepository.Ping}
	healthv1.RegisterHealthServer(grpcServer, health)
	listener, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		return fmt.Errorf("listen gRPC: %w", err)
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	go func() { serverDone <- grpcServer.Serve(listener) }()
	slog.Info("analytics gRPC service started", "address", listener.Addr())
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-serverDone:
	}
	health.draining.Store(true)
	done := make(chan struct{})
	go func() { grpcServer.GracefulStop(); close(done) }()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		grpcServer.Stop()
		<-done
	}
	if errors.Is(serveErr, grpc.ErrServerStopped) {
		return nil
	}
	return serveErr
}
