package com.example.sportsbook.common;

import java.math.BigDecimal;
import java.time.Instant;

/** A bet was durably accepted. Published via the transactional outbox. */
public record BetPlacedEvent(
        String betId,
        String userId,
        String eventId,
        String marketId,
        String selectionId,
        BigDecimal stake,
        BigDecimal odds,
        Instant timestamp
) implements BetEvent {

    @Override
    public BetEventType type() {
        return BetEventType.BET_PLACED;
    }
}
