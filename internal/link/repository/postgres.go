package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"linkpulse/internal/link/model"
)

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{
		pool: pool,
	}
}

func (r *PostgresRepository) Create(
	ctx context.Context,
	link *model.Link,
) error {
	query := `
		INSERT INTO links (
			code,
			target_url
		)
		VALUES ($1, $2)
		RETURNING id, created_at
	`

	err := r.pool.QueryRow(
		ctx,
		query,
		link.Code,
		link.TargetURL,
	).Scan(
		&link.ID,
		&link.CreatedAt,
	)

	if err != nil {
		var pgErr *pgconn.PgError

		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrCodeAlreadyExists
		}

		return fmt.Errorf("create link: %w", err)
	}

	return nil
}

func (r *PostgresRepository) GetByCode(
	ctx context.Context,
	code string,
) (model.Link, error) {
	query := `
		SELECT
			id,
			code,
			target_url,
			created_at
		FROM links
		WHERE code = $1
	`

	var link model.Link

	err := r.pool.QueryRow(
		ctx,
		query,
		code,
	).Scan(
		&link.ID,
		&link.Code,
		&link.TargetURL,
		&link.CreatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.Link{}, ErrLinkNotFound
		}

		return model.Link{}, fmt.Errorf("get link by code: %w", err)
	}

	return link, nil
}
