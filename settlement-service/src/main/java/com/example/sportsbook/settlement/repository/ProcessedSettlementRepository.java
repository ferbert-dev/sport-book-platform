package com.example.sportsbook.settlement.repository;

import com.example.sportsbook.settlement.domain.ProcessedSettlement;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.UUID;

public interface ProcessedSettlementRepository extends JpaRepository<ProcessedSettlement, UUID> {

    /**
     * Registers the settlement unless this market and version is already registered. The fence.
     *
     * <p>{@code ON CONFLICT DO NOTHING} rather than insert-and-catch: in PostgreSQL a unique
     * violation aborts the transaction, so a caught exception cannot be followed by "return false
     * and commit". Zero rows is not an error and leaves the transaction usable.
     *
     * @return 1 if this delivery registered the settlement, 0 if it was already processed
     */
    @Modifying
    @Query(value = """
            INSERT INTO processed_settlements (id, market_id, settlement_version, processed_at)
            VALUES (:#{#settlement.id}, :#{#settlement.marketId}, :#{#settlement.settlementVersion},
                    :#{#settlement.processedAt})
            ON CONFLICT ON CONSTRAINT uk_settlement_market_version DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("settlement") ProcessedSettlement settlement);

    /**
     * Takes the per-market advisory lock in EXCLUSIVE mode until the transaction ends. It waits for
     * every bet-service transaction that holds the shared lock for this market
     * ({@code pg_advisory_xact_lock_shared}), and makes later bets wait for this settlement.
     *
     * <p>The key expression must stay identical to bet-service's
     * {@code ProcessedSettlementRepository.lockForBetPlacement}.
     */
    @Query(value = "SELECT 1 FROM pg_advisory_xact_lock(hashtextextended(:marketId, 0))",
            nativeQuery = true)
    Integer lockForSettlement(@Param("marketId") String marketId);
}
