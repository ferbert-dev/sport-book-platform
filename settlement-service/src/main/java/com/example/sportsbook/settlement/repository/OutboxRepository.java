package com.example.sportsbook.settlement.repository;

import com.example.sportsbook.settlement.domain.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

/** Insert-only here: bet-service's relay is the only reader of the outbox. */
public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {
}
