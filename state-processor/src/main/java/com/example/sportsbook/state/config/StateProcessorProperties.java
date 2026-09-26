package com.example.sportsbook.state.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * @param marketFreshnessThreshold how old market state may get before it is treated as STALE_FEED
 * @param staleCheckInterval       how often the staleness sweep runs
 */
@ConfigurationProperties(prefix = "sportsbook.state")
public record StateProcessorProperties(
        Duration marketFreshnessThreshold,
        Duration staleCheckInterval
) {

    public StateProcessorProperties {
        if (marketFreshnessThreshold == null) {
            marketFreshnessThreshold = Duration.ofSeconds(30);
        }
        if (staleCheckInterval == null) {
            staleCheckInterval = Duration.ofSeconds(10);
        }
    }
}
