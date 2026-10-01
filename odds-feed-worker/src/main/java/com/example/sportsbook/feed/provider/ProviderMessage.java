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
 * A provider that never sends an epoch stays on epoch 0 and works as before. Mixing the two — an
 * epoch-0 message after epoch 1 or later — is treated as a stale session: dropped and logged
 * ({@code PROVIDER_STALE_SESSION_DROPPED}). Silently mixing two numbering schemes would be worse
 * than a loud drop.
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
