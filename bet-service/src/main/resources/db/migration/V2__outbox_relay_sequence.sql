-- Relay order for the outbox.
--
-- Two services insert into outbox_event: bet-service (BET_PLACED) and settlement-service
-- (BET_SETTLED, PAYOUT_REQUIRED). created_at is taken from each writer's own clock, so ordering by it
-- can send a bet's BET_SETTLED before its BET_PLACED when the clocks differ. The database hands out
-- seq, so it is one ordering for every writer.
--
-- seq is assigned at insert, not at commit. That is enough for the events of one bet: settlement only
-- sees a bet once its insert, and with it its BET_PLACED row, has committed.
ALTER TABLE outbox_event ADD COLUMN seq BIGINT GENERATED ALWAYS AS IDENTITY;

DROP INDEX idx_outbox_unpublished;
CREATE INDEX idx_outbox_unpublished ON outbox_event (seq) WHERE published_at IS NULL;
