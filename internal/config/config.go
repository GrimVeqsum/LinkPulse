package config

import (
	"errors"
	"os"
)

type Config struct {
	HTTPPort    string
	PostgresURL string
}

func Load() (Config, error) {
	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		httpPort = "8080"
	}

	postgresURL := os.Getenv("POSTGRES_URL")
	if postgresURL == "" {
		return Config{}, errors.New("POSTGRES_URL is required")
	}

	return Config{
		HTTPPort:    httpPort,
		PostgresURL: postgresURL,
	}, nil
}
