package com.example.sportsbook.bet.repository;

import com.example.sportsbook.bet.domain.OutboxEvent;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {

    /** Oldest-first so events are published in the order they were produced. */
    List<OutboxEvent> findByPublishedAtIsNullOrderByCreatedAtAsc(Limit limit);
}
