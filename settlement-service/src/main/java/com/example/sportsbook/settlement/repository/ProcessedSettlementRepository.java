package com.example.sportsbook.settlement.repository;

import com.example.sportsbook.settlement.domain.ProcessedSettlement;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ProcessedSettlementRepository extends JpaRepository<ProcessedSettlement, UUID> {

    boolean existsByMarketIdAndSettlementVersion(String marketId, long settlementVersion);
}
