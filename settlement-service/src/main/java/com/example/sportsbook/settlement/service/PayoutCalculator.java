package com.example.sportsbook.settlement.service;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Payout arithmetic, kept pure so it is trivially testable.
 *
 * <p>Decimal odds include the stake, so a winning payout is {@code stake * odds} — not
 * {@code stake * (odds - 1)}, which would be the profit.
 */
public final class PayoutCalculator {

    /** Currency scale. HALF_UP matches conventional money rounding. */
    private static final int MONEY_SCALE = 2;

    private PayoutCalculator() {
    }

    public static BigDecimal winningPayout(BigDecimal stake, BigDecimal acceptedOdds) {
        return stake.multiply(acceptedOdds).setScale(MONEY_SCALE, RoundingMode.HALF_UP);
    }

    public static BigDecimal losingPayout() {
        return BigDecimal.ZERO.setScale(MONEY_SCALE);
    }
}
