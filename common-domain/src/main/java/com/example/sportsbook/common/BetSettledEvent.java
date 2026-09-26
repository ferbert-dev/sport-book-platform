package com.example.sportsbook.common;

import java.math.BigDecimal;
import java.time.Instant;

/** A bet was resolved to WON or LOST. */
public record BetSettledEvent(
        String betId,
        BetStatus status,
        BigDecimal payout,
        Instant timestamp
) implements BetEvent {

    @Override
    public BetEventType type() {
        return BetEventType.BET_SETTLED;
    }
}
