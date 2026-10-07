package com.example.sportsbook.bet.repository;

import com.example.sportsbook.bet.domain.Bet;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface BetRepository extends JpaRepository<Bet, UUID> {

    /** Backs the idempotency check; served by uk_bet_user_idempotency. */
    Optional<Bet> findByUserIdAndIdempotencyKey(String userId, String idempotencyKey);

    /**
     * Inserts the bet unless one already exists for the same user and idempotency key.
     *
     * <p>Not {@code save}: when two requests with the same key race, the loser's plain INSERT fails
     * with a unique violation, and in PostgreSQL a failed statement aborts the whole transaction.
     * Catching the Java exception does not undo that, so nothing more can be read in it, not even
     * the winner's bet. {@code ON CONFLICT DO NOTHING} is not an error: it waits for the competing
     * insert to finish and then reports zero rows, leaving the transaction usable.
     *
     * <p>It names the constraint, so only that conflict is absorbed. Any other violation (a CHECK,
     * a NOT NULL) still fails loudly instead of being mistaken for a duplicate.
     *
     * @return 1 if this bet was inserted, 0 if another bet already holds the key
     */
    @Modifying
    @Query(value = """
            INSERT INTO bet (id, user_id, event_id, market_id, selection_id, stake, accepted_odds,
                             status, idempotency_key, created_at)
            VALUES (:#{#bet.id}, :#{#bet.userId}, :#{#bet.eventId}, :#{#bet.marketId},
                    :#{#bet.selectionId}, :#{#bet.stake}, :#{#bet.acceptedOdds},
                    :#{#bet.status.name()}, :#{#bet.idempotencyKey}, :#{#bet.createdAt})
            ON CONFLICT ON CONSTRAINT uk_bet_user_idempotency DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("bet") Bet bet);
}
