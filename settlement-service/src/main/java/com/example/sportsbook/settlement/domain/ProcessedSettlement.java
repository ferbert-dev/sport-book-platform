package com.example.sportsbook.settlement.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * The settlement idempotency fence.
 *
 * <p>Kafka is at-least-once, so MARKET_SETTLED can arrive more than once. Inserting here first means
 * a redelivery collides with {@code uk_settlement_market_version} and is skipped, which is what
 * prevents duplicate payouts.
 *
 * <p>Owned schema-wise by bet-service's migrations; this service only reads and writes rows.
 */
@Entity
@Table(name = "processed_settlements")
public class ProcessedSettlement {

    @Id
    private UUID id;

    @Column(name = "market_id", nullable = false)
    private String marketId;

    @Column(name = "settlement_version", nullable = false)
    private long settlementVersion;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected ProcessedSettlement() {
        // for JPA
    }

    public static ProcessedSettlement of(String marketId, long settlementVersion) {
        ProcessedSettlement settlement = new ProcessedSettlement();
        settlement.id = UUID.randomUUID();
        settlement.marketId = marketId;
        settlement.settlementVersion = settlementVersion;
        settlement.processedAt = Instant.now();
        return settlement;
    }

    public UUID getId() {
        return id;
    }

    public String getMarketId() {
        return marketId;
    }

    public long getSettlementVersion() {
        return settlementVersion;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
