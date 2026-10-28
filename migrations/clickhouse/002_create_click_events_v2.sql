-- +goose Up
-- The original click_events table is intentionally retained for legacy data.
-- Read-time uniqExact is required; raw row count is not a logical click count.
CREATE TABLE IF NOT EXISTS click_events_v2
(
    event_id UUID,
    link_id Int64,
    code String,
    user_agent String,
    referer String,
    occurred_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (link_id, event_id);

-- +goose Down
DROP TABLE IF EXISTS click_events_v2;
