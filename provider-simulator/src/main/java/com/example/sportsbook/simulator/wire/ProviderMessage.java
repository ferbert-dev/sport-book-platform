package com.example.sportsbook.simulator.wire;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * One message on the provider's wire, in the provider's vocabulary ({@code matchId},
 * {@code marketRef}, {@code price}, {@code PRICE_CHANGE}).
 *
 * <p>odds-feed-worker keeps its own copy of this shape: the JSON is the contract, not a shared
 * class, exactly as it would be with a real vendor.
 *
 * <p>Built as a draft with {@code sequenceNumber = 0}; {@link com.example.sportsbook.simulator.feed.ProviderFeed}
 * stamps the real sequence when the message is emitted.
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

    public static ProviderMessage matchStart(String matchId) {
        return draft("MATCH_START", matchId, null, null, null, null);
    }

    public static ProviderMessage matchEnd(String matchId) {
        return draft("MATCH_END", matchId, null, null, null, null);
    }

    public static ProviderMessage priceChange(String matchId, String marketRef, String outcomeRef, BigDecimal price) {
        return draft("PRICE_CHANGE", matchId, marketRef, outcomeRef, price, null);
    }

    public static ProviderMessage marketLock(String matchId, String marketRef) {
        return draft("MARKET_LOCK", matchId, marketRef, null, null, null);
    }

    public static ProviderMessage marketUnlock(String matchId, String marketRef) {
        return draft("MARKET_UNLOCK", matchId, marketRef, null, null, null);
    }

    public static ProviderMessage marketResult(String matchId, String marketRef, String winnerRef) {
        return draft("MARKET_RESULT", matchId, marketRef, null, null, winnerRef);
    }

    public ProviderMessage stamped(long epoch, long sequence, Instant at) {
        return new ProviderMessage(epoch, sequence, messageType, matchId, marketRef, outcomeRef, price, winnerRef, at);
    }

    private static ProviderMessage draft(String type, String matchId, String marketRef, String outcomeRef,
                                         BigDecimal price, String winnerRef) {
        return new ProviderMessage(0L, 0L, type, matchId, marketRef, outcomeRef, price, winnerRef, null);
    }
}
