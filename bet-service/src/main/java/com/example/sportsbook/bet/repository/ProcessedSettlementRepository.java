package com.example.sportsbook.bet.repository;

import com.example.sportsbook.bet.domain.ProcessedSettlement;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.UUID;

/** Read side of settlement's fence: bet-service only asks "has this market been settled?". */
public interface ProcessedSettlementRepository extends JpaRepository<ProcessedSettlement, UUID> {

    /** Index-backed: {@code uk_settlement_market_version} leads with {@code market_id}. */
    boolean existsByMarketId(String marketId);

    /**
     * Takes the per-market advisory lock in SHARED mode until the transaction ends. It waits while
     * settlement-service holds the EXCLUSIVE lock for the same market
     * ({@code pg_advisory_xact_lock}), and makes a settlement that arrives later wait for this
     * transaction.
     *
     * <p>The key expression must stay identical to settlement-service's, or the two sides lock
     * different things. Two market ids hashing to the same key only wait for each other; that can
     * never let a bet through.
     *
     * <p>The function is in FROM, not in the select list: it returns void, which has no Java type,
     * and a function in FROM is always executed.
     */
    @Query(value = "SELECT 1 FROM pg_advisory_xact_lock_shared(hashtextextended(:marketId, 0))",
            nativeQuery = true)
    Integer lockForBetPlacement(@Param("marketId") String marketId);
}
