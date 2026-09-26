package com.example.sportsbook.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.Instant;

/**
 * A fact produced by our own betting domain, carried on {@code bet-events}.
 *
 * <p>Keyed on {@code betId} so all events for one bet stay ordered within a partition.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = BetPlacedEvent.class, name = "BET_PLACED"),
        @JsonSubTypes.Type(value = BetSettledEvent.class, name = "BET_SETTLED"),
        @JsonSubTypes.Type(value = PayoutRequiredEvent.class, name = "PAYOUT_REQUIRED")
})
public sealed interface BetEvent
        permits BetPlacedEvent, BetSettledEvent, PayoutRequiredEvent {

    BetEventType type();

    String betId();

    Instant timestamp();

    /** Kafka partition key. */
    default String partitionKey() {
        return betId();
    }
}
