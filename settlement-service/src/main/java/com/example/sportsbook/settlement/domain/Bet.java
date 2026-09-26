package com.example.sportsbook.settlement.domain;

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

/**
 * Settlement's view of the shared {@code bet} table.
 *
 * <p>Two services mapping one table is a deliberate simplification for this project. In a system
 * where these teams ship independently, the bet store would sit behind an owning service's API (or
 * settlement would own its own projection) rather than being reached into directly.
 *
 * <p>This mapping is read-plus-settle only: it never creates bets, and it owns no migrations.
 */
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

    @Column(name = "settled_at")
    private Instant settledAt;

    private BigDecimal payout;

    protected Bet() {
        // for JPA
    }

    /** Applies the settlement outcome. */
    public void settle(BetStatus outcome, BigDecimal payout) {
        this.status = outcome;
        this.payout = payout;
        this.settledAt = Instant.now();
    }

    public boolean won(String winningSelectionId) {
        return selectionId.equals(winningSelectionId);
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

    public Instant getSettledAt() {
        return settledAt;
    }

    public BigDecimal getPayout() {
        return payout;
    }
}
