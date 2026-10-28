package main

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestDependencyHealthReflectsLiveDatabaseState(t *testing.T) {
	var pingErr error
	calls := 0
	health := &dependencyHealth{ping: func(context.Context) error { calls++; return pingErr }}
	for _, service := range []string{"", "analytics.v1.AnalyticsService"} {
		response, err := health.Check(context.Background(), &healthv1.HealthCheckRequest{Service: service})
		if err != nil || response.GetStatus() != healthv1.HealthCheckResponse_SERVING {
			t.Fatalf("healthy dependency: response=%v err=%v", response, err)
		}
	}
	pingErr = errors.New("database temporarily unavailable")
	response, err := health.Check(context.Background(), &healthv1.HealthCheckRequest{})
	if err != nil || response.GetStatus() != healthv1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("dependency outage: response=%v err=%v", response, err)
	}
	pingErr = nil
	response, err = health.Check(context.Background(), &healthv1.HealthCheckRequest{})
	if err != nil || response.GetStatus() != healthv1.HealthCheckResponse_SERVING || calls != 4 {
		t.Fatalf("dependency recovery: response=%v err=%v pings=%d", response, err, calls)
	}
}

func TestDependencyHealthUnknownService(t *testing.T) {
	health := &dependencyHealth{ping: func(context.Context) error { t.Fatal("unknown service must not ping database"); return nil }}
	response, err := health.Check(context.Background(), &healthv1.HealthCheckRequest{Service: "unknown.Service"})
	if status.Code(err) != codes.NotFound || response != nil {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestDependencyHealthDraining(t *testing.T) {
	health := &dependencyHealth{ping: func(context.Context) error {
		t.Fatal("draining service must not report dependency readiness")
		return nil
	}}
	health.draining.Store(true)
	response, err := health.Check(context.Background(), &healthv1.HealthCheckRequest{})
	if err != nil || response.GetStatus() != healthv1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestDependencyHealthPropagatesCheckContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	health := &dependencyHealth{ping: func(pingCtx context.Context) error {
		if pingCtx != ctx || !errors.Is(pingCtx.Err(), context.Canceled) {
			t.Fatal("caller cancellation was not propagated")
		}
		return pingCtx.Err()
	}}
	response, err := health.Check(ctx, &healthv1.HealthCheckRequest{})
	if err != nil || response.GetStatus() != healthv1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("response=%v err=%v", response, err)
	}
}
