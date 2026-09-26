package com.example.sportsbook.bet.repository;

import com.example.sportsbook.bet.domain.Bet;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface BetRepository extends JpaRepository<Bet, UUID> {

    /** Backs the idempotency check; served by uk_bet_user_idempotency. */
    Optional<Bet> findByUserIdAndIdempotencyKey(String userId, String idempotencyKey);
}
