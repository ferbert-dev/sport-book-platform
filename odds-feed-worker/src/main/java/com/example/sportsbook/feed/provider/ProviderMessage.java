package com.example.sportsbook.feed.provider;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Raw message as the external provider sends it.
 *
 * <p>Deliberately uses provider-specific vocabulary ({@code matchId}, {@code marketRef},
 * {@code price}, {@code PRICE_CHANGE}) rather than our domain language. Normalization into
 * {@link com.example.sportsbook.common.SportsEvent} is an explicit pipeline stage.
 *
 * <p>FIX 4: {@code sessionEpoch} grows each time the provider restarts its numbering; together with
 * {@code sequenceNumber} it orders the whole stream (see {@link ProviderVersion}). A provider that
 * does not send the field reads as epoch {@code 0} — Jackson's default for a missing {@code long}.
 * Once epoch 1 or later has been processed, an epoch-0 message compares as a stale session, so how
 * the pipeline treats 0 ("no session support") is decided where the epoch is wired in (step 5).
 */
public record ProviderMessage(
        long sessionEpoch,
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
