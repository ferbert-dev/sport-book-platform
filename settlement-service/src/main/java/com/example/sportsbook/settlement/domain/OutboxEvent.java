package com.example.sportsbook.settlement.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * Write side of the transactional outbox: an event queued for publication in the same transaction as
 * the settlement it describes. Either the bet rows, the fence row and these rows all commit, or none
 * does, so a failed settlement can never leave an event behind and a committed one can never lose it.
 *
 * <p>Same table as bet-service's entity ({@code outbox_event}, migrated by bet-service). This module
 * only inserts; bet-service's {@code OutboxPublisher} is the one relay that sends rows to Kafka and
 * marks them published, which is why there is no {@code publishedAt} here. One table and one relay
 * also means one ordered queue, so a bet's BET_PLACED is sent before its BET_SETTLED.
 */
@Entity
@Table(name = "outbox_event")
public class OutboxEvent {

    @Id
    private UUID id;

    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(nullable = false)
    private String payload;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected OutboxEvent() {
        // for JPA
    }

    public static OutboxEvent pending(String aggregateType, String aggregateId, String eventType,
                                      String payload) {
        OutboxEvent event = new OutboxEvent();
        event.id = UUID.randomUUID();
        event.aggregateType = aggregateType;
        event.aggregateId = aggregateId;
        event.eventType = eventType;
        event.payload = payload;
        event.createdAt = Instant.now();
        return event;
    }

    public UUID getId() {
        return id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
