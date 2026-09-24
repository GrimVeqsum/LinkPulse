-- +goose Up

CREATE TABLE IF NOT EXISTS click_events
(
    link_id Int64,
    code String,
    user_agent String,
    referer String,
    occurred_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (link_id, occurred_at);

-- +goose Down

DROP TABLE IF EXISTS click_events;