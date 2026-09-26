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
 * <p>The provider's {@code sequenceNumber} doubles as the domain {@code version}: it is the only
 * monotonic ordering signal the provider gives us, and downstream consumers use it to discard
 * out-of-order updates.
 */
public final class MessageNormalizer {

    private MessageNormalizer() {
    }

    /** Returns empty for message types we do not model, rather than throwing. */
    public static Optional<SportsEvent> normalize(ProviderMessage message) {
        long version = message.sequenceNumber();
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
