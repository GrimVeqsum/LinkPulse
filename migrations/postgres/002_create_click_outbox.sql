-- +goose Up
CREATE TABLE click_outbox (
    event_id UUID PRIMARY KEY,
    link_id BIGINT NOT NULL REFERENCES links(id),
    code VARCHAR(6) NOT NULL,
    user_agent TEXT NOT NULL,
    referer TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_token UUID,
    locked_until TIMESTAMPTZ,
    last_error VARCHAR(1024) NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX click_outbox_available_idx ON click_outbox (available_at, created_at);

-- +goose Down
DROP TABLE click_outbox;
