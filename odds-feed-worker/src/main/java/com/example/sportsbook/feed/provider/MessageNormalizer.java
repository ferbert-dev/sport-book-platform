package com.example.sportsbook.feed.provider;

import com.example.sportsbook.common.MarketOpenedEvent;
import com.example.sportsbook.common.MarketSettledEvent;
import com.example.sportsbook.common.MarketSuspendedEvent;
import com.example.sportsbook.common.MatchFinishedEvent;
import com.example.sportsbook.common.MatchStartedEvent;
import com.example.sportsbook.common.OddsUpdatedEvent;
import com.example.sportsbook.common.SportsEvent;

import java.util.Optional;

/**
 * Translates provider vocabulary into our domain events.
 *
 * <p>The domain {@code version} is {@link ProviderVersion#compose}{@code (sessionEpoch, sequenceNumber)}:
 * the provider's ordering signal, kept growing across provider sessions (FIX 4) so downstream
 * consumers can still discard out-of-order updates after the provider restarts its numbering.
 * Epoch 0 — a provider that sends no epoch — gives the plain sequence, as before.
 */
public final class MessageNormalizer {

    private MessageNormalizer() {
    }

    /** Returns empty for message types we do not model, rather than throwing. */
    public static Optional<SportsEvent> normalize(ProviderMessage message) {
        long version = ProviderVersion.compose(message.sessionEpoch(), message.sequenceNumber());
        return switch (message.messageType()) {
            case "MATCH_START" -> Optional.of(
                    new MatchStartedEvent(message.matchId(), version, message.sentAt()));
            case "MATCH_END" -> Optional.of(
                    new MatchFinishedEvent(message.matchId(), version, message.sentAt()));
            case "PRICE_CHANGE" -> Optional.of(new OddsUpdatedEvent(
                    message.matchId(), message.marketRef(), message.outcomeRef(),
                    message.price(), version, message.sentAt()));
            case "MARKET_LOCK" -> Optional.of(new MarketSuspendedEvent(
                    message.matchId(), message.marketRef(), version, message.sentAt()));
            case "MARKET_UNLOCK" -> Optional.of(new MarketOpenedEvent(
                    message.matchId(), message.marketRef(), version, message.sentAt()));
            case "MARKET_RESULT" -> Optional.of(new MarketSettledEvent(
                    message.matchId(), message.marketRef(), message.winnerRef(),
                    version, message.sentAt()));
            default -> Optional.empty();
        };
    }
}
