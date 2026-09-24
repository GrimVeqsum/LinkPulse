package config

import (
	"errors"
	"os"
)

type LinkConfig struct {
	HTTPPort          string
	PostgresURL       string
	AnalyticsGRPCAddr string
}

type AnalyticsConfig struct {
	GRPCPort           string
	ClickHouseAddr     string
	ClickHouseDatabase string
	ClickHouseUser     string
	ClickHousePassword string
}

func LoadLink() (LinkConfig, error) {
	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		httpPort = "8080"
	}

	postgresURL := os.Getenv("POSTGRES_URL")
	if postgresURL == "" {
		return LinkConfig{}, errors.New("POSTGRES_URL is required")
	}

	analyticsAddr := os.Getenv("ANALYTICS_GRPC_ADDR")
	if analyticsAddr == "" {
		analyticsAddr = "localhost:9090"
	}

	return LinkConfig{
		HTTPPort:          httpPort,
		PostgresURL:       postgresURL,
		AnalyticsGRPCAddr: analyticsAddr,
	}, nil
}

func LoadAnalytics() AnalyticsConfig {
	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "9090"
	}

	clickHouseAddr := os.Getenv("CLICKHOUSE_ADDR")
	if clickHouseAddr == "" {
		clickHouseAddr = "localhost:9000"
	}

	database := os.Getenv("CLICKHOUSE_DATABASE")
	if database == "" {
		database = "linkpulse"
	}

	user := os.Getenv("CLICKHOUSE_USER")
	if user == "" {
		user = "default"
	}

	return AnalyticsConfig{
		GRPCPort:           grpcPort,
		ClickHouseAddr:     clickHouseAddr,
		ClickHouseDatabase: database,
		ClickHouseUser:     user,
		ClickHousePassword: os.Getenv("CLICKHOUSE_PASSWORD"),
	}
}
