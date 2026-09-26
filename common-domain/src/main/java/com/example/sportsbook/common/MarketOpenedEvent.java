package com.example.sportsbook.common;

import java.time.Instant;

/** Market reopened for new bets. */
public record MarketOpenedEvent(
        String eventId,
        String marketId,
        long version,
        Instant timestamp
) implements SportsEvent {

    @Override
    public SportsEventType type() {
        return SportsEventType.MARKET_OPENED;
    }

    @Override
    public String partitionKey() {
        return marketId;
    }
}
