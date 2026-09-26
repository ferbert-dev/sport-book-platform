package com.example.sportsbook.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EventSerializationTest {

    private final ObjectMapper mapper = SportsbookJson.mapper();
    private final Instant now = Instant.parse("2026-09-26T10:15:30Z");

    @Test
    void sportsEventsRoundTripThroughTheSealedHierarchy() throws Exception {
        List<SportsEvent> events = List.of(
                new OddsUpdatedEvent("event-123", "market-456", "real-madrid", new BigDecimal("2.10"), 1001, now),
                new MarketSuspendedEvent("event-123", "market-456", 1002, now),
                new MarketOpenedEvent("event-123", "market-456", 1003, now),
                new MatchStartedEvent("event-123", 1004, now),
                new MatchFinishedEvent("event-123", 1005, now),
                new MarketSettledEvent("event-123", "market-456", "real-madrid", 1006, now)
        );

        for (SportsEvent original : events) {
            String json = mapper.writeValueAsString(original);
            assertThat(json).contains("\"type\":\"" + original.type().name() + "\"");

            SportsEvent parsed = mapper.readValue(json, SportsEvent.class);
            assertThat(parsed).isEqualTo(original);
        }
    }

    @Test
    void betEventsRoundTripThroughTheSealedHierarchy() throws Exception {
        List<BetEvent> events = List.of(
                new BetPlacedEvent("bet-789", "user-42", "event-123", "market-456", "real-madrid",
                        new BigDecimal("100.00"), new BigDecimal("2.10"), now),
                new BetSettledEvent("bet-789", BetStatus.WON, new BigDecimal("210.00"), now),
                new PayoutRequiredEvent("bet-789-settlement-1001", "bet-789", "user-42",
                        new BigDecimal("210.00"), "EUR", now)
        );

        for (BetEvent original : events) {
            String json = mapper.writeValueAsString(original);
            assertThat(json).contains("\"type\":\"" + original.type().name() + "\"");

            BetEvent parsed = mapper.readValue(json, BetEvent.class);
            assertThat(parsed).isEqualTo(original);
        }
    }

    @Test
    void timestampsSerializeAsIsoStringsNotEpochNumbers() throws Exception {
        String json = mapper.writeValueAsString(new MatchStartedEvent("event-123", 1, now));

        assertThat(json).contains("\"timestamp\":\"2026-09-26T10:15:30Z\"");
    }

    @Test
    void marketScopedEventsKeyOnMarketIdAndMatchLevelEventsKeyOnEventId() {
        assertThat(new OddsUpdatedEvent("event-1", "market-9", "sel", BigDecimal.ONE, 1, now).partitionKey())
                .isEqualTo("market-9");
        assertThat(new MarketSettledEvent("event-1", "market-9", "sel", 1, now).partitionKey())
                .isEqualTo("market-9");
        assertThat(new MatchStartedEvent("event-1", 1, now).partitionKey())
                .isEqualTo("event-1");
        assertThat(new MatchFinishedEvent("event-1", 1, now).partitionKey())
                .isEqualTo("event-1");
    }

    @Test
    void payoutIdIsDeterministicForTheSameSettlementVersion() {
        assertThat(PayoutRequiredEvent.paymentId("bet-789", 1001))
                .isEqualTo("bet-789-settlement-1001")
                .isEqualTo(PayoutRequiredEvent.paymentId("bet-789", 1001));
    }

    @Test
    void oddsPreserveDecimalScaleSoMoneyMathStaysExact() throws Exception {
        OddsUpdatedEvent original =
                new OddsUpdatedEvent("event-1", "market-1", "sel", new BigDecimal("2.10"), 1, now);

        OddsUpdatedEvent parsed = (OddsUpdatedEvent) mapper.readValue(
                mapper.writeValueAsString(original), SportsEvent.class);

        assertThat(parsed.odds()).isEqualByComparingTo("2.10");
        assertThat(parsed.odds().scale()).isEqualTo(2);
    }
}
