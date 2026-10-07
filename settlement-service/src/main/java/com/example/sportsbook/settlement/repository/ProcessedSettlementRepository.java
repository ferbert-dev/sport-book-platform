package com.example.sportsbook.settlement.repository;

import com.example.sportsbook.settlement.domain.ProcessedSettlement;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.UUID;

public interface ProcessedSettlementRepository extends JpaRepository<ProcessedSettlement, UUID> {

    boolean existsByMarketIdAndSettlementVersion(String marketId, long settlementVersion);

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
