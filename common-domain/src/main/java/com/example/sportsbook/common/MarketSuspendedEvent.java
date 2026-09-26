package com.example.sportsbook.common;

import java.time.Instant;

/** Market closed for new bets, typically mid-incident. */
public record MarketSuspendedEvent(
        String eventId,
        String marketId,
        long version,
        Instant timestamp
) implements SportsEvent {

    @Override
    public SportsEventType type() {
        return SportsEventType.MARKET_SUSPENDED;
    }

    @Override
    public String partitionKey() {
        return marketId;
    }
}
