package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"linkpulse/internal/event"
	"linkpulse/internal/link/model"
	"linkpulse/internal/outbox"
)

const queryTimeout = 2 * time.Second

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
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
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

		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "links_code_key" {
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
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
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

func (r *PostgresRepository) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	return r.pool.Ping(ctx)
}

// Enqueue commits independently of the HTTP response. Default PostgreSQL
// synchronous_commit makes a successful return the durability boundary.
func (r *PostgresRepository) Enqueue(ctx context.Context, event event.Click) error {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	_, err := r.pool.Exec(ctx, `
		INSERT INTO click_outbox (event_id, link_id, code, user_agent, referer, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		event.ID, event.LinkID, event.Code, event.UserAgent, event.Referer, event.OccurredAt.UTC())
	if err != nil {
		return fmt.Errorf("enqueue click: %w", err)
	}
	return nil
}

// Claim atomically locks due rows, skips other workers' locks, and grants a lease.
// No transaction or database connection is held across the network RPC.
func (r *PostgresRepository) Claim(ctx context.Context, limit int, lease time.Duration) ([]outbox.Item, error) {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	if limit <= 0 || lease <= 0 {
		return nil, errors.New("claim requires positive limit and lease")
	}
	token, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("generate lease token: %w", err)
	}
	rows, err := r.pool.Query(ctx, `
		WITH due AS (
			SELECT event_id FROM click_outbox
			WHERE available_at <= NOW() AND (locked_until IS NULL OR locked_until <= NOW())
			ORDER BY available_at, created_at
			LIMIT $1 FOR UPDATE SKIP LOCKED
		)
		UPDATE click_outbox AS o
		SET lease_token = $2, locked_until = NOW() + $3 * INTERVAL '1 millisecond',
		    attempts = LEAST(o.attempts::BIGINT + 1, 2147483647)::INTEGER
		FROM due WHERE o.event_id = due.event_id
		RETURNING o.event_id::TEXT, o.link_id, o.code, o.user_agent, o.referer,
		          o.occurred_at, o.attempts, o.lease_token::TEXT`, limit, token.String(), lease.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("claim clicks: %w", err)
	}
	defer rows.Close()
	items := make([]outbox.Item, 0, limit)
	for rows.Next() {
		var item outbox.Item
		if err := rows.Scan(&item.Event.ID, &item.Event.LinkID, &item.Event.Code,
			&item.Event.UserAgent, &item.Event.Referer, &item.Event.OccurredAt,
			&item.Attempts, &item.LeaseToken); err != nil {
			return nil, fmt.Errorf("scan click: %w", err)
		}
		item.Event.OccurredAt = item.Event.OccurredAt.UTC()
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *PostgresRepository) Ack(ctx context.Context, item outbox.Item) error {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	result, err := r.pool.Exec(ctx,
		`DELETE FROM click_outbox WHERE event_id = $1 AND lease_token = $2`, item.Event.ID, item.LeaseToken)
	if err != nil {
		return fmt.Errorf("delete delivered click: %w", err)
	}
	if result.RowsAffected() != 1 {
		return outbox.ErrLeaseLost
	}
	return nil
}

func (r *PostgresRepository) Retry(ctx context.Context, item outbox.Item, next time.Time, lastError string) error {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	message := []rune(strings.ToValidUTF8(lastError, "?"))
	if len(message) > 1024 {
		message = message[:1024]
	}
	result, err := r.pool.Exec(ctx, `
		UPDATE click_outbox SET available_at = $3, last_error = $4,
		lease_token = NULL, locked_until = NULL
		WHERE event_id = $1 AND lease_token = $2`, item.Event.ID, item.LeaseToken, next.UTC(), string(message))
	if err != nil {
		return fmt.Errorf("schedule click retry: %w", err)
	}
	if result.RowsAffected() != 1 {
		return outbox.ErrLeaseLost
	}
	return nil
}
