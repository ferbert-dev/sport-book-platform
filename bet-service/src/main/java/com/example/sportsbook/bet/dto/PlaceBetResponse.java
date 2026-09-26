package com.example.sportsbook.bet.dto;

import java.math.BigDecimal;

/**
 * @param currentOdds populated only on ODDS_CHANGED, so the client can immediately re-offer
 * @param reason      human-readable detail; null when accepted
 */
public record PlaceBetResponse(
        String betId,
        BetOutcome status,
        BigDecimal acceptedOdds,
        BigDecimal currentOdds,
        String reason
) {

    public static PlaceBetResponse accepted(String betId, BigDecimal acceptedOdds) {
        return new PlaceBetResponse(betId, BetOutcome.ACCEPTED, acceptedOdds, null, null);
    }

    public static PlaceBetResponse rejected(BetOutcome outcome, String reason) {
        return new PlaceBetResponse(null, outcome, null, null, reason);
    }

    public static PlaceBetResponse oddsChanged(BigDecimal currentOdds) {
        return new PlaceBetResponse(null, BetOutcome.ODDS_CHANGED, null, currentOdds,
                "Odds moved before the bet was accepted");
    }
}
