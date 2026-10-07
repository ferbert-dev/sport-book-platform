package com.example.sportsbook.bet.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * @param marketFreshnessThreshold market state older than this is rejected as STALE_MARKET_DATA
 * @param oddsTolerance            absolute decimal-odds drift accepted without rejecting the bet
 * @param maxStake                 largest stake accepted for one bet; above it the bet is rejected
 *                                 as STAKE_LIMIT_EXCEEDED
 */
@ConfigurationProperties(prefix = "sportsbook.bet")
public record BetServiceProperties(
        Duration marketFreshnessThreshold,
        BigDecimal oddsTolerance,
        BigDecimal maxStake
) {

    static final BigDecimal DEFAULT_MAX_STAKE = new BigDecimal("10000");

    /**
     * Exclusive upper bound for {@code maxStake}. A payout is {@code stake x odds}; odds have at most
     * 6 integer digits ({@code PlaceBetRequest}) and {@code bet.payout} is NUMERIC(19,4), which holds
     * less than 10^15. So a stake must stay below 10^9 or a winning bet could not be stored.
     */
    static final BigDecimal UNPAYABLE_STAKE = new BigDecimal("1000000000");

    public BetServiceProperties {
        if (marketFreshnessThreshold == null) {
            marketFreshnessThreshold = Duration.ofSeconds(30);
        }
        if (oddsTolerance == null) {
            oddsTolerance = BigDecimal.ZERO;
        }
        if (maxStake == null) {
            maxStake = DEFAULT_MAX_STAKE;
        }
        // A limit the schema cannot honour must stop the service at startup, not fail a settlement
        // long after the bet was accepted.
        if (maxStake.signum() <= 0 || maxStake.compareTo(UNPAYABLE_STAKE) >= 0) {
            throw new IllegalArgumentException("sportsbook.bet.max-stake must be above 0 and below "
                    + UNPAYABLE_STAKE.toPlainString() + ", got " + maxStake.toPlainString());
        }
    }
}
