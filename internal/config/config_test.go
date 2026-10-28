package config

import (
	"strings"
	"testing"
)

func clearConfigEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{"HTTP_PORT", "POSTGRES_URL", "ANALYTICS_GRPC_ADDR", "GRPC_PORT", "CLICKHOUSE_ADDR", "CLICKHOUSE_DATABASE", "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD"} {
		t.Setenv(name, "")
	}
}

func TestLoadLinkRequiresPostgres(t *testing.T) {
	clearConfigEnvironment(t)
	if _, err := LoadLink(); err == nil || !strings.Contains(err.Error(), "POSTGRES_URL") {
		t.Fatalf("expected actionable missing POSTGRES_URL error, got %v", err)
	}
}

func TestLoadLinkDefaultsAndOverrides(t *testing.T) {
	clearConfigEnvironment(t)
	const postgresURL = "postgres://demo:demo@postgres:5432/linkpulse?sslmode=disable"
	t.Setenv("POSTGRES_URL", postgresURL)
	cfg, err := LoadLink()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPPort != "8080" || cfg.AnalyticsGRPCAddr != "localhost:9090" || cfg.PostgresURL != postgresURL {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	t.Setenv("HTTP_PORT", "8181")
	t.Setenv("ANALYTICS_GRPC_ADDR", "[::1]:9191")
	cfg, err = LoadLink()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPPort != "8181" || cfg.AnalyticsGRPCAddr != "[::1]:9191" || cfg.PostgresURL != postgresURL {
		t.Fatalf("unexpected overrides: %+v", cfg)
	}
}

func TestLoadAnalyticsDefaultsAndOverrides(t *testing.T) {
	clearConfigEnvironment(t)
	cfg, err := LoadAnalytics()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GRPCPort != "9090" || cfg.ClickHouseAddr != "localhost:9000" || cfg.ClickHouseDatabase != "linkpulse" || cfg.ClickHouseUser != "default" || cfg.ClickHousePassword != "" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	for name, value := range map[string]string{"GRPC_PORT": "9191", "CLICKHOUSE_ADDR": "clickhouse:9001", "CLICKHOUSE_DATABASE": "analytics_test", "CLICKHOUSE_USER": "demo", "CLICKHOUSE_PASSWORD": "demo-password"} {
		t.Setenv(name, value)
	}
	cfg, err = LoadAnalytics()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GRPCPort != "9191" || cfg.ClickHouseAddr != "clickhouse:9001" || cfg.ClickHouseDatabase != "analytics_test" || cfg.ClickHouseUser != "demo" || cfg.ClickHousePassword != "demo-password" {
		t.Fatal("configuration overrides were not preserved")
	}
}

func TestConfigurationRejectsInvalidPorts(t *testing.T) {
	for _, variable := range []string{"HTTP_PORT", "GRPC_PORT"} {
		for _, value := range []string{"0", "65536", "-1", "+8080", "http", " 8080", "8080 "} {
			t.Run(variable+"/"+value, func(t *testing.T) {
				clearConfigEnvironment(t)
				t.Setenv("POSTGRES_URL", "postgres://postgres:5432/test")
				t.Setenv(variable, value)
				var err error
				if variable == "HTTP_PORT" {
					_, err = LoadLink()
				} else {
					_, err = LoadAnalytics()
				}
				if err == nil || !strings.Contains(err.Error(), variable) {
					t.Fatalf("expected %s validation error, got %v", variable, err)
				}
			})
		}
	}
	for _, value := range []string{"1", "65535"} {
		if err := validatePort(value); err != nil {
			t.Fatalf("valid boundary port %q rejected: %v", value, err)
		}
	}
}

func TestConfigurationRejectsInvalidDependencyAddresses(t *testing.T) {
	for _, variable := range []string{"ANALYTICS_GRPC_ADDR", "CLICKHOUSE_ADDR"} {
		for _, value := range []string{"localhost", ":9090", "localhost:", "localhost:0", "localhost:65536", "::1:9090", "localhost:grpc"} {
			t.Run(variable+"/"+value, func(t *testing.T) {
				clearConfigEnvironment(t)
				t.Setenv("POSTGRES_URL", "postgres://postgres:5432/test")
				t.Setenv(variable, value)
				var err error
				if variable == "ANALYTICS_GRPC_ADDR" {
					_, err = LoadLink()
				} else {
					_, err = LoadAnalytics()
				}
				if err == nil || !strings.Contains(err.Error(), variable) {
					t.Fatalf("expected %s validation error, got %v", variable, err)
				}
			})
		}
	}
}
