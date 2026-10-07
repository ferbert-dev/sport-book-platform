package com.example.sportsbook.bet.repository;

import com.example.sportsbook.bet.domain.OutboxEvent;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * In insert order, by the database-assigned {@code seq}. Not by {@code createdAt}: two services
     * write this table, each with its own clock, and a skew between them reordered a bet's
     * BET_SETTLED ahead of its BET_PLACED.
     */
    List<OutboxEvent> findByPublishedAtIsNullOrderBySeqAsc(Limit limit);
}
