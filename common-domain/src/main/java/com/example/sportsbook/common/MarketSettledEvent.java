package com.example.sportsbook.common;

import java.time.Instant;

/** Market resolved. {@code version} doubles as the settlement version for idempotency. */
public record MarketSettledEvent(
        String eventId,
        String marketId,
        String winningSelectionId,
        long version,
        Instant timestamp
) implements SportsEvent {

    @Override
    public SportsEventType type() {
        return SportsEventType.MARKET_SETTLED;
    }

    @Override
    public String partitionKey() {
        return marketId;
    }
}
