package com.example.sportsbook.common;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Hand-off to the Payments/Wallet bounded context, which we deliberately do not own.
 *
 * <p>{@code paymentId} is deterministic ({@code betId + "-settlement-" + settlementVersion}) so a
 * redelivered settlement produces the same id and the payments side can dedupe.
 */
public record PayoutRequiredEvent(
        String paymentId,
        String betId,
        String userId,
        BigDecimal amount,
        String currency,
        Instant timestamp
) implements BetEvent {

    @Override
    public BetEventType type() {
        return BetEventType.PAYOUT_REQUIRED;
    }

    /** Builds the deterministic payment id used for cross-context deduplication. */
    public static String paymentId(String betId, long settlementVersion) {
        return betId + "-settlement-" + settlementVersion;
    }
}
