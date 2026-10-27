package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	analyticsclient "linkpulse/internal/analytics/client"
	"linkpulse/internal/config"
	"linkpulse/internal/link/handler"
	"linkpulse/internal/link/repository"
	"linkpulse/internal/link/service"
	"linkpulse/internal/outbox"
	"linkpulse/migrations"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("link service stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := config.LoadLink()
	if err != nil {
		return err
	}
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		client := &http.Client{Timeout: 2 * time.Second}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:"+cfg.HTTPPort+"/ready", nil)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("readiness status %d", response.StatusCode)
		}
		return nil
	}
	startupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	poolConfig, err := pgxpool.ParseConfig(cfg.PostgresURL)
	if err != nil {
		return fmt.Errorf("configure postgres: %w", err)
	}
	// pgx defaults to CPU-count connections, which can exceed PostgreSQL's
	// connection budget on large hosts. Keep each service instance bounded.
	poolConfig.MaxConns = min(poolConfig.MaxConns, 16)
	poolConfig.MinConns = min(poolConfig.MinConns, poolConfig.MaxConns)
	poolConfig.MinIdleConns = min(poolConfig.MinIdleConns, poolConfig.MaxConns)
	poolConfig.ConnConfig.ConnectTimeout = 3 * time.Second
	pool, err := pgxpool.NewWithConfig(startupCtx, poolConfig)
	if err != nil {
		return fmt.Errorf("open postgres pool: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(startupCtx); err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	if err := migrations.UpPostgres(startupCtx, pool); err != nil {
		return err
	}
	grpcConnection, err := grpc.NewClient(cfg.AnalyticsGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("configure analytics client: %w", err)
	}
	defer grpcConnection.Close()
	analyticsClient := analyticsclient.New(grpcConnection)
	linkRepository := repository.NewPostgresRepository(pool)
	linkService := service.New(linkRepository)
	var draining atomic.Bool
	ready := func(ctx context.Context) error {
		if draining.Load() {
			return errors.New("service is draining")
		}
		// Redirects remain available while analytics is down: PostgreSQL buffers events.
		return linkRepository.Ping(ctx)
	}
	linkHandler := handler.New(linkService, analyticsClient, linkRepository, ready)
	mux := http.NewServeMux()
	linkHandler.RegisterRoutes(mux)
	server := &http.Server{
		Addr: ":" + cfg.HTTPPort, Handler: mux,
		ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("listen HTTP: %w", err)
	}
	defer listener.Close()
	workerCtx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()
	worker := outbox.New(linkRepository, analyticsClient, outbox.Options{})
	workerDone := make(chan error, 1)
	go func() { workerDone <- worker.Run(workerCtx) }()
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(listener) }()
	slog.Info("link HTTP service started", "address", server.Addr)
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-serverDone:
	}
	draining.Store(true)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	shutdownErr := server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		_ = server.Close()
	}
	// Finish accepted HTTP writes first, then stop and join the single delivery worker.
	stopWorker()
	workerErr := <-workerDone
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return errors.Join(serveErr, shutdownErr, workerErr)
}
