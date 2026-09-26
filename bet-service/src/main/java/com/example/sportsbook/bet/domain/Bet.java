package com.example.sportsbook.bet.domain;

import com.example.sportsbook.common.BetStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** A durably accepted bet. */
@Entity
@Table(name = "bet")
public class Bet {

    @Id
    private UUID id;

    @Column(name = "user_id", nullable = false)
    private String userId;

    @Column(name = "event_id", nullable = false)
    private String eventId;

    @Column(name = "market_id", nullable = false)
    private String marketId;

    @Column(name = "selection_id", nullable = false)
    private String selectionId;

    @Column(nullable = false)
    private BigDecimal stake;

    @Column(name = "accepted_odds", nullable = false)
    private BigDecimal acceptedOdds;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private BetStatus status;

    @Column(name = "idempotency_key", nullable = false)
    private String idempotencyKey;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "settled_at")
    private Instant settledAt;

    private BigDecimal payout;

    protected Bet() {
        // for JPA
    }

    public static Bet accept(String userId, String eventId, String marketId, String selectionId,
                             BigDecimal stake, BigDecimal acceptedOdds, String idempotencyKey) {
        Bet bet = new Bet();
        bet.id = UUID.randomUUID();
        bet.userId = userId;
        bet.eventId = eventId;
        bet.marketId = marketId;
        bet.selectionId = selectionId;
        bet.stake = stake;
        bet.acceptedOdds = acceptedOdds;
        bet.status = BetStatus.OPEN;
        bet.idempotencyKey = idempotencyKey;
        bet.createdAt = Instant.now();
        return bet;
    }

    public UUID getId() {
        return id;
    }

    public String getUserId() {
        return userId;
    }

    public String getEventId() {
        return eventId;
    }

    public String getMarketId() {
        return marketId;
    }

    public String getSelectionId() {
        return selectionId;
    }

    public BigDecimal getStake() {
        return stake;
    }

    public BigDecimal getAcceptedOdds() {
        return acceptedOdds;
    }

    public BetStatus getStatus() {
        return status;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getSettledAt() {
        return settledAt;
    }

    public BigDecimal getPayout() {
        return payout;
    }
}
