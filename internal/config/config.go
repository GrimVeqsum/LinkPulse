package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
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
	if err := validatePort(httpPort); err != nil {
		return LinkConfig{}, fmt.Errorf("HTTP_PORT: %w", err)
	}

	postgresURL := os.Getenv("POSTGRES_URL")
	if postgresURL == "" {
		return LinkConfig{}, errors.New("POSTGRES_URL is required")
	}

	analyticsAddr := os.Getenv("ANALYTICS_GRPC_ADDR")
	if analyticsAddr == "" {
		analyticsAddr = "localhost:9090"
	}
	if err := validateAddress(analyticsAddr); err != nil {
		return LinkConfig{}, fmt.Errorf("ANALYTICS_GRPC_ADDR: %w", err)
	}

	return LinkConfig{
		HTTPPort:          httpPort,
		PostgresURL:       postgresURL,
		AnalyticsGRPCAddr: analyticsAddr,
	}, nil
}

func LoadAnalytics() (AnalyticsConfig, error) {
	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "9090"
	}
	if err := validatePort(grpcPort); err != nil {
		return AnalyticsConfig{}, fmt.Errorf("GRPC_PORT: %w", err)
	}

	clickHouseAddr := os.Getenv("CLICKHOUSE_ADDR")
	if clickHouseAddr == "" {
		clickHouseAddr = "localhost:9000"
	}
	if err := validateAddress(clickHouseAddr); err != nil {
		return AnalyticsConfig{}, fmt.Errorf("CLICKHOUSE_ADDR: %w", err)
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
	}, nil
}

func validatePort(value string) error {
	for _, char := range value {
		if char < '0' || char > '9' {
			return errors.New("port must be an integer between 1 and 65535")
		}
	}
	port, err := strconv.Atoi(value)
	if err != nil || port < 1 || port > 65535 {
		return errors.New("port must be an integer between 1 and 65535")
	}
	return nil
}

func validateAddress(value string) error {
	host, port, err := net.SplitHostPort(value)
	if err != nil || host == "" {
		return errors.New("address must contain a host and port")
	}
	return validatePort(port)
}
