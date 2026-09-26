package com.example.sportsbook.state.service;

import com.example.sportsbook.common.MarketStatus;
import com.example.sportsbook.state.config.StateProcessorProperties;
import com.example.sportsbook.common.RedisKeys;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;

/**
 * Suspends markets whose state has gone stale.
 *
 * <p>If the provider connection dies, the last odds we hold stay in Redis and look perfectly valid.
 * Continuing to accept bets against them is the dangerous failure mode, so any market not updated
 * within the freshness threshold is flipped to SUSPENDED. Bet Service independently re-checks
 * freshness on every bet, so this sweep is a safety net rather than the only guard.
 */
@Service
public class StalenessDetector {

    private static final Logger log = LoggerFactory.getLogger(StalenessDetector.class);

    private final StringRedisTemplate redis;
    private final StateProcessorProperties properties;
    private final Counter staleSuspensions;

    public StalenessDetector(StringRedisTemplate redis,
                             StateProcessorProperties properties,
                             MeterRegistry meterRegistry) {
        this.redis = redis;
        this.properties = properties;
        this.staleSuspensions = Counter.builder("markets_suspended_stale_total").register(meterRegistry);
    }

    @Scheduled(fixedDelayString = "${sportsbook.state.stale-check-interval:10s}")
    public void suspendStaleMarkets() {
        Set<String> marketIds = redis.opsForSet().members(RedisKeys.allMarkets());
        if (marketIds == null || marketIds.isEmpty()) {
            return;
        }
        Instant cutoff = Instant.now().minus(properties.marketFreshnessThreshold());
        for (String marketId : marketIds) {
            suspendIfStale(marketId, cutoff);
        }
    }

    private void suspendIfStale(String marketId, Instant cutoff) {
        String key = RedisKeys.market(marketId);
        Object status = redis.opsForHash().get(key, RedisKeys.FIELD_STATUS);
        Object lastUpdatedAt = redis.opsForHash().get(key, RedisKeys.FIELD_LAST_UPDATED_AT);
        if (lastUpdatedAt == null) {
            return;
        }
        // Settled and closed markets are terminal; staleness is irrelevant for them.
        if (status != null && (MarketStatus.SETTLED.name().equals(status.toString())
                || MarketStatus.CLOSED.name().equals(status.toString()))) {
            return;
        }
        if (MarketStatus.SUSPENDED.name().equals(String.valueOf(status))) {
            return;
        }
        Instant updated = Instant.parse(lastUpdatedAt.toString());
        if (updated.isAfter(cutoff)) {
            return;
        }
        redis.opsForHash().put(key, RedisKeys.FIELD_STATUS, MarketStatus.SUSPENDED.name());
        staleSuspensions.increment();
        log.warn("MARKET_SUSPENDED_STALE_FEED marketId={} lastUpdatedAt={} thresholdSeconds={}",
                marketId, updated, Duration.ofMillis(properties.marketFreshnessThreshold().toMillis()).toSeconds());
    }
}
