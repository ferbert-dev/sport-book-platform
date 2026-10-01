package com.example.sportsbook.feed.provider;

import com.example.sportsbook.common.MarketOpenedEvent;
import com.example.sportsbook.common.MarketSettledEvent;
import com.example.sportsbook.common.MarketSuspendedEvent;
import com.example.sportsbook.common.MatchFinishedEvent;
import com.example.sportsbook.common.MatchStartedEvent;
import com.example.sportsbook.common.OddsUpdatedEvent;
import com.example.sportsbook.common.SportsEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class MessageNormalizerTest {

    private final Instant sentAt = Instant.parse("2026-09-26T10:00:00Z");

    @Test
    void priceChangeBecomesOddsUpdatedAndCarriesSequenceAsVersion() {
        ProviderMessage message = new ProviderMessage(1, 1001, "PRICE_CHANGE", "event-123",
                "market-456", "real-madrid", new BigDecimal("2.10"), null, sentAt);

        SportsEvent event = MessageNormalizer.normalize(message).orElseThrow();

        assertThat(event).isInstanceOf(OddsUpdatedEvent.class);
        OddsUpdatedEvent odds = (OddsUpdatedEvent) event;
        assertThat(odds.eventId()).isEqualTo("event-123");
        assertThat(odds.marketId()).isEqualTo("market-456");
        assertThat(odds.selectionId()).isEqualTo("real-madrid");
        assertThat(odds.odds()).isEqualByComparingTo("2.10");
        assertThat(odds.version()).isEqualTo(1001);
        assertThat(odds.timestamp()).isEqualTo(sentAt);
    }

    @Test
    void providerVocabularyMapsOntoDomainEventTypes() {
        assertThat(normalize("MATCH_START")).isInstanceOf(MatchStartedEvent.class);
        assertThat(normalize("MATCH_END")).isInstanceOf(MatchFinishedEvent.class);
        assertThat(normalize("MARKET_LOCK")).isInstanceOf(MarketSuspendedEvent.class);
        assertThat(normalize("MARKET_UNLOCK")).isInstanceOf(MarketOpenedEvent.class);
    }

    @Test
    void marketResultBecomesMarketSettledWithTheWinningSelection() {
        ProviderMessage message = new ProviderMessage(1, 1006, "MARKET_RESULT", "event-123",
                "market-456", null, null, "real-madrid", sentAt);

        MarketSettledEvent settled = (MarketSettledEvent) MessageNormalizer.normalize(message).orElseThrow();

        assertThat(settled.winningSelectionId()).isEqualTo("real-madrid");
        assertThat(settled.version()).isEqualTo(1006);
    }

    @Test
    void unknownProviderMessageTypeIsSkippedRatherThanThrowing() {
        ProviderMessage message = new ProviderMessage(1, 1, "SOME_FUTURE_TYPE", "event-123",
                "market-456", null, null, null, sentAt);

        Optional<SportsEvent> normalized = MessageNormalizer.normalize(message);

        assertThat(normalized).isEmpty();
    }

    private SportsEvent normalize(String providerType) {
        return MessageNormalizer.normalize(new ProviderMessage(1, 1, providerType, "event-123",
                "market-456", null, null, "real-madrid", sentAt)).orElseThrow();
    }
}
