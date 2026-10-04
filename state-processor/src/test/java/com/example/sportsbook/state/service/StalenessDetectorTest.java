package com.example.sportsbook.state.service;

import com.example.sportsbook.common.MarketStatus;
import com.example.sportsbook.common.RedisKeys;
import com.example.sportsbook.state.config.StateProcessorProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StalenessDetectorTest {

    private static final String MARKET_ID = "market-456";
    private static final String KEY = RedisKeys.market(MARKET_ID);

    @SuppressWarnings("unchecked")
    private final HashOperations<String, Object, Object> hash = mock(HashOperations.class);
    @SuppressWarnings("unchecked")
    private final SetOperations<String, String> set = mock(SetOperations.class);
    private final StateProcessorProperties properties = mock(StateProcessorProperties.class);

    private StalenessDetector detector;

    @BeforeEach
    @SuppressWarnings({"unchecked", "rawtypes"})
    void setUp() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.opsForHash()).thenReturn((HashOperations) hash);
        when(redis.opsForSet()).thenReturn(set);
        when(set.members(RedisKeys.allMarkets())).thenReturn(Set.of(MARKET_ID));
        when(properties.marketFreshnessThreshold()).thenReturn(Duration.ofSeconds(30));
        detector = new StalenessDetector(redis, properties, new SimpleMeterRegistry());
    }

    private void stored(MarketStatus status, Instant lastUpdatedAt) {
        when(hash.get(KEY, RedisKeys.FIELD_STATUS)).thenReturn(status.name());
        when(hash.get(KEY, RedisKeys.FIELD_LAST_UPDATED_AT)).thenReturn(lastUpdatedAt.toString());
    }

    @Test
    void aStaleActiveMarketIsSuspendedWithTheReasonThatLetsAPriceReopenIt() {
        stored(MarketStatus.ACTIVE, Instant.now().minusSeconds(60));

        detector.suspendStaleMarkets();

        verify(hash).putAll(KEY, Map.of(
                RedisKeys.FIELD_STATUS, MarketStatus.SUSPENDED.name(),
                RedisKeys.FIELD_SUSPEND_REASON, RedisKeys.SUSPEND_REASON_STALE_FEED));
    }

    @Test
    void aFreshMarketIsLeftAlone() {
        stored(MarketStatus.ACTIVE, Instant.now());

        detector.suspendStaleMarkets();

        verify(hash, never()).putAll(any(), any());
    }

    @Test
    void aMarketTheFeedSuspendedKeepsItsOwnSuspensionAndGetsNoStaleReason() {
        stored(MarketStatus.SUSPENDED, Instant.now().minusSeconds(60));

        detector.suspendStaleMarkets();

        verify(hash, never()).putAll(any(), any());
    }

    @Test
    void aSettledMarketIsNeverSuspended() {
        stored(MarketStatus.SETTLED, Instant.now().minusSeconds(60));

        detector.suspendStaleMarkets();

        verify(hash, never()).putAll(any(), any());
    }
}
