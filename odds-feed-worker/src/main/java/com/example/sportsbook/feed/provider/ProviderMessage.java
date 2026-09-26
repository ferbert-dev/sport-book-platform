package com.example.sportsbook.feed.provider;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Raw message as the external provider sends it.
 *
 * <p>Deliberately uses provider-specific vocabulary ({@code matchId}, {@code marketRef},
 * {@code price}, {@code PRICE_CHANGE}) rather than our domain language. Normalization into
 * {@link com.example.sportsbook.common.SportsEvent} is an explicit pipeline stage.
 */
public record ProviderMessage(
        long sequenceNumber,
        String messageType,
        String matchId,
        String marketRef,
        String outcomeRef,
        BigDecimal price,
        String winnerRef,
        Instant sentAt
) {
}
