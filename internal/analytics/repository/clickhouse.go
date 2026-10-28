package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"linkpulse/internal/event"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const queryTimeout = 2 * time.Second

type Repository struct {
	db *sql.DB
}

// ClickEvent is kept as an alias for callers of the original repository API.
type ClickEvent = event.Click

type LinkStats struct {
	TotalClicks uint64
	TodayClicks uint64
}

func New(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	return r.db.PingContext(ctx)
}

func (r *Repository) SaveClick(ctx context.Context, event event.Click) error {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"async_insert": 0}))

	// Synchronous INSERT acknowledgement is required before the outbox can be
	// acknowledged. Duplicate physical rows are intentional and deduplicated
	// by event identity in every supported statistics query. Pass an integer epoch
	// rather than time.Time: the driver can omit the timezone from time literals
	// when the Go/server timezone matches, making session_timezone affect the
	// stored instant. Explicit millisecond conversion also preserves precision.
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO click_events_v2
			(event_id, link_id, code, user_agent, referer, occurred_at)
		VALUES (?, ?, ?, ?, ?, fromUnixTimestamp64Milli(?, 'UTC'))
	`, event.ID, event.LinkID, event.Code, event.UserAgent, event.Referer, event.OccurredAt.UnixMilli())
	if err != nil {
		return fmt.Errorf("save click: %w", err)
	}
	return nil
}

func (r *Repository) GetLinkStats(ctx context.Context, linkID int64) (LinkStats, error) {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	// now('UTC') is constant within this query. Both bounds are necessary:
	// future-dated events must not be included in today's count. Legacy rows
	// have no IDs and are preserved as historical counts, without inventing IDs.
	const query = `
		WITH
			toStartOfDay(now('UTC'), 'UTC') AS day_start,
			day_start + INTERVAL 1 DAY AS day_end
		SELECT sum(total_clicks), sum(today_clicks)
		FROM (
			SELECT count() AS total_clicks,
				countIf(occurred_at >= day_start AND occurred_at < day_end) AS today_clicks
			FROM click_events WHERE link_id = ?
			UNION ALL
			SELECT uniqExact(event_id) AS total_clicks,
				uniqExactIf(event_id, occurred_at >= day_start AND occurred_at < day_end) AS today_clicks
			FROM click_events_v2 WHERE link_id = ?
		)
	`
	var stats LinkStats
	if err := r.db.QueryRowContext(ctx, query, linkID, linkID).Scan(&stats.TotalClicks, &stats.TodayClicks); err != nil {
		return LinkStats{}, fmt.Errorf("get link stats: %w", err)
	}
	return stats, nil
}
