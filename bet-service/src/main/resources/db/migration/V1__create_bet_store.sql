-- Schema ownership: bet-service owns ALL migrations for the shared `sportsbook` database,
-- including processed_settlements, which settlement-service reads and writes but never migrates.
-- This keeps a single migration history and avoids two services racing to create the same objects.

CREATE TABLE bet (
    id               UUID           PRIMARY KEY,
    user_id          VARCHAR(64)    NOT NULL,
    event_id         VARCHAR(64)    NOT NULL,
    market_id        VARCHAR(64)    NOT NULL,
    selection_id     VARCHAR(64)    NOT NULL,
    stake            NUMERIC(19, 4) NOT NULL CHECK (stake > 0),
    accepted_odds    NUMERIC(10, 4) NOT NULL CHECK (accepted_odds > 1),
    status           VARCHAR(16)    NOT NULL,
    idempotency_key  VARCHAR(128)   NOT NULL,
    created_at       TIMESTAMPTZ    NOT NULL,
    settled_at       TIMESTAMPTZ,
    payout           NUMERIC(19, 4)
);

-- A client retrying POST /bets with the same key must never create a second bet.
-- Scoped per user so two users cannot collide on a shared key value.
ALTER TABLE bet ADD CONSTRAINT uk_bet_user_idempotency UNIQUE (user_id, idempotency_key);

CREATE INDEX idx_bet_event_id  ON bet (event_id);
CREATE INDEX idx_bet_market_id ON bet (market_id);
CREATE INDEX idx_bet_user_id   ON bet (user_id);
CREATE INDEX idx_bet_status    ON bet (status);

-- Settlement's hot query is "open bets for this market".
CREATE INDEX idx_bet_market_status ON bet (market_id, status);

CREATE TABLE outbox_event (
    id             UUID         PRIMARY KEY,
    aggregate_type VARCHAR(64)  NOT NULL,
    aggregate_id   VARCHAR(64)  NOT NULL,
    event_type     VARCHAR(64)  NOT NULL,
    payload        TEXT         NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL,
    published_at   TIMESTAMPTZ
);

-- The publisher polls for unpublished rows in creation order; a partial index keeps that
-- scan proportional to the backlog rather than to the whole table.
CREATE INDEX idx_outbox_unpublished ON outbox_event (created_at) WHERE published_at IS NULL;

CREATE TABLE processed_settlements (
    id                 UUID        PRIMARY KEY,
    market_id          VARCHAR(64) NOT NULL,
    settlement_version BIGINT      NOT NULL,
    processed_at       TIMESTAMPTZ NOT NULL
);

-- The idempotency fence: a redelivered MARKET_SETTLED collides here and is skipped.
ALTER TABLE processed_settlements
    ADD CONSTRAINT uk_settlement_market_version UNIQUE (market_id, settlement_version);
