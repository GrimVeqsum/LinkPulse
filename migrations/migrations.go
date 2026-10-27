package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed postgres/*.sql
var postgresFiles embed.FS

//go:embed clickhouse/*.sql
var clickHouseFiles embed.FS

func UpPostgres(
	ctx context.Context,
	pool *pgxpool.Pool,
) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	files, err := fs.Sub(postgresFiles, "postgres")
	if err != nil {
		return err
	}
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("postgres migration lock: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, files,
		goose.WithDisableGlobalRegistry(true), goose.WithSessionLocker(locker))
	if err != nil {
		return fmt.Errorf("postgres migration provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("postgres migrations: %w", err)
	}

	return nil
}

func UpClickHouse(
	ctx context.Context,
	db *sql.DB,
) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	files, err := fs.Sub(clickHouseFiles, "clickhouse")
	if err != nil {
		return err
	}
	provider, err := goose.NewProvider(goose.DialectClickHouse, db, files, goose.WithDisableGlobalRegistry(true))
	if err != nil {
		return fmt.Errorf("clickhouse migration provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("clickhouse migrations: %w", err)
	}

	return nil
}
