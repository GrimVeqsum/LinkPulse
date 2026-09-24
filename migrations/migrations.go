package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed postgres/*.sql
var postgresFiles embed.FS

//go:embed clickhouse/*.sql
var clickHouseFiles embed.FS

func UpPostgres(
	ctx context.Context,
	pool *pgxpool.Pool,
) error {
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()

	goose.SetBaseFS(postgresFiles)

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set postgres dialect: %w", err)
	}

	if err := goose.UpContext(
		ctx,
		db,
		"postgres",
	); err != nil {
		return fmt.Errorf("postgres migrations: %w", err)
	}

	return nil
}

func UpClickHouse(
	ctx context.Context,
	db *sql.DB,
) error {
	goose.SetBaseFS(clickHouseFiles)

	if err := goose.SetDialect("clickhouse"); err != nil {
		return fmt.Errorf("set clickhouse dialect: %w", err)
	}

	if err := goose.UpContext(
		ctx,
		db,
		"clickhouse",
	); err != nil {
		return fmt.Errorf("clickhouse migrations: %w", err)
	}

	return nil
}
