package com.example.sportsbook.common;

import java.math.BigDecimal;
import java.time.Instant;

/** Decimal odds for one selection changed. */
public record OddsUpdatedEvent(
        String eventId,
        String marketId,
        String selectionId,
        BigDecimal odds,
        long version,
        Instant timestamp
) implements SportsEvent {

    @Override
    public SportsEventType type() {
        return SportsEventType.ODDS_UPDATED;
    }

    @Override
    public String partitionKey() {
        return marketId;
    }
}
