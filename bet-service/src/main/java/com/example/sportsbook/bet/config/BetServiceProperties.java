package com.example.sportsbook.bet.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * @param marketFreshnessThreshold market state older than this is rejected as STALE_MARKET_DATA
 * @param oddsTolerance            absolute decimal-odds drift accepted without rejecting the bet
 */
@ConfigurationProperties(prefix = "sportsbook.bet")
public record BetServiceProperties(
        Duration marketFreshnessThreshold,
        BigDecimal oddsTolerance
) {

    public BetServiceProperties {
        if (marketFreshnessThreshold == null) {
            marketFreshnessThreshold = Duration.ofSeconds(30);
        }
        if (oddsTolerance == null) {
            oddsTolerance = BigDecimal.ZERO;
        }
    }
}
