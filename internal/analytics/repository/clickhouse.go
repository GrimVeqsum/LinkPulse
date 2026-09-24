package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Repository struct {
	db *sql.DB
}

type ClickEvent struct {
	LinkID     int64
	Code       string
	UserAgent  string
	Referer    string
	OccurredAt time.Time
}

type LinkStats struct {
	TotalClicks uint64
	TodayClicks uint64
}

func New(db *sql.DB) *Repository {
	return &Repository{
		db: db,
	}
}

func (r *Repository) SaveClick(
	ctx context.Context,
	event ClickEvent,
) error {
	query := `
		INSERT INTO click_events (
			link_id,
			code,
			user_agent,
			referer,
			occurred_at
		)
		VALUES (?, ?, ?, ?, ?)
	`

	_, err := r.db.ExecContext(
		ctx,
		query,
		event.LinkID,
		event.Code,
		event.UserAgent,
		event.Referer,
		event.OccurredAt,
	)

	if err != nil {
		return fmt.Errorf("save click: %w", err)
	}

	return nil
}

func (r *Repository) GetLinkStats(
	ctx context.Context,
	linkID int64,
) (LinkStats, error) {
	query := `
		SELECT
			count(),
			countIf(
				occurred_at >= toStartOfDay(now())
			)
		FROM click_events
		WHERE link_id = ?
	`

	var stats LinkStats

	err := r.db.QueryRowContext(
		ctx,
		query,
		linkID,
	).Scan(
		&stats.TotalClicks,
		&stats.TodayClicks,
	)

	if err != nil {
		return LinkStats{}, fmt.Errorf(
			"get link stats: %w",
			err,
		)
	}

	return stats, nil
}
