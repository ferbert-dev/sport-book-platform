package com.example.sportsbook.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.Instant;

/**
 * A fact originating from the sports/provider domain, carried on {@code sports-events}.
 *
 * <p>Sealed so consumers can switch exhaustively over the event space without a default branch.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = OddsUpdatedEvent.class, name = "ODDS_UPDATED"),
        @JsonSubTypes.Type(value = MarketSuspendedEvent.class, name = "MARKET_SUSPENDED"),
        @JsonSubTypes.Type(value = MarketOpenedEvent.class, name = "MARKET_OPENED"),
        @JsonSubTypes.Type(value = MatchStartedEvent.class, name = "MATCH_STARTED"),
        @JsonSubTypes.Type(value = MatchFinishedEvent.class, name = "MATCH_FINISHED"),
        @JsonSubTypes.Type(value = MarketSettledEvent.class, name = "MARKET_SETTLED")
})
public sealed interface SportsEvent
        permits OddsUpdatedEvent, MarketSuspendedEvent, MarketOpenedEvent,
                MatchStartedEvent, MatchFinishedEvent, MarketSettledEvent {

    SportsEventType type();

    String eventId();

    /** Monotonic provider version used to discard out-of-order and replayed updates. */
    long version();

    Instant timestamp();

    /**
     * Kafka partition key. Market-scoped events key on {@code marketId} so updates for one
     * market stay ordered; match-level events key on {@code eventId}.
     */
    String partitionKey();
}
