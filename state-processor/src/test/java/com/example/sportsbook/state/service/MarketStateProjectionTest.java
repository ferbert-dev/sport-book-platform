package com.example.sportsbook.state.service;

import com.example.sportsbook.common.MarketOpenedEvent;
import com.example.sportsbook.common.MarketSettledEvent;
import com.example.sportsbook.common.MarketStatus;
import com.example.sportsbook.common.MarketSuspendedEvent;
import com.example.sportsbook.common.MatchFinishedEvent;
import com.example.sportsbook.common.MatchStartedEvent;
import com.example.sportsbook.common.OddsUpdatedEvent;
import com.example.sportsbook.common.RedisKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MarketStateProjectionTest {

    private static final String EVENT_ID = "event-123";
    private static final String MARKET_ID = "market-456";

    private final Instant now = Instant.parse("2026-09-26T10:00:00Z");

    private StringRedisTemplate redis;
    @SuppressWarnings("unchecked")
    private final HashOperations<String, Object, Object> hash = mock(HashOperations.class);
    @SuppressWarnings("unchecked")
    private final SetOperations<String, String> set = mock(SetOperations.class);

    private MarketStateProjection projection;

    @BeforeEach
    void setUp() {
        redis = mock(StringRedisTemplate.class);
        when(redis.opsForHash()).thenReturn((HashOperations) hash);
        when(redis.opsForSet()).thenReturn(set);
        projection = new MarketStateProjection(redis);
    }

    private void storedMarketVersion(Long version) {
        when(hash.get(RedisKeys.market(MARKET_ID), RedisKeys.FIELD_VERSION))
                .thenReturn(version == null ? null : version.toString());
    }

    private void storedEventVersion(Long version) {
        when(hash.get(RedisKeys.event(EVENT_ID), RedisKeys.FIELD_VERSION))
                .thenReturn(version == null ? null : version.toString());
    }

    @Test
    void oddsUpdatedWritesTheSelectionPriceIntoTheOddsHash() {
        storedMarketVersion(null);

        boolean applied = projection.apply(new OddsUpdatedEvent(EVENT_ID, MARKET_ID, "real-madrid",
                new BigDecimal("2.10"), 1001, now));

        assertThat(applied).isTrue();
        verify(hash).put(RedisKeys.marketOdds(MARKET_ID), "real-madrid", "2.10");
    }

    @Test
    void oddsUpdatedRegistersTheMarketSoSnapshotsAndSweepsCanFindIt() {
        storedMarketVersion(null);

        projection.apply(new OddsUpdatedEvent(EVENT_ID, MARKET_ID, "real-madrid",
                new BigDecimal("2.10"), 1001, now));

        verify(set).add(RedisKeys.eventMarkets(EVENT_ID), MARKET_ID);
        verify(set).add(RedisKeys.allMarkets(), MARKET_ID);
    }

    @Test
    void olderVersionIsIgnoredAndNothingIsWritten() {
        storedMarketVersion(1005L);

        boolean applied = projection.apply(new OddsUpdatedEvent(EVENT_ID, MARKET_ID, "real-madrid",
                new BigDecimal("9.99"), 1001, now));

        assertThat(applied).isFalse();
        verify(hash, never()).put(eq(RedisKeys.marketOdds(MARKET_ID)), any(), any());
    }

    @Test
    void duplicateVersionIsIgnoredSoRedeliveryIsIdempotent() {
        storedMarketVersion(1001L);

        boolean applied = projection.apply(new OddsUpdatedEvent(EVENT_ID, MARKET_ID, "real-madrid",
                new BigDecimal("2.10"), 1001, now));

        assertThat(applied).isFalse();
        verify(hash, never()).put(eq(RedisKeys.marketOdds(MARKET_ID)), any(), any());
    }

    @Test
    void marketSuspendedSetsStatusSuspended() {
        storedMarketVersion(1001L);

        boolean applied = projection.apply(new MarketSuspendedEvent(EVENT_ID, MARKET_ID, 1002, now));

        assertThat(applied).isTrue();
        assertThat(capturedMarketFields()).containsEntry(RedisKeys.FIELD_STATUS, MarketStatus.SUSPENDED.name());
    }

    @Test
    void marketOpenedReopensTheMarket() {
        storedMarketVersion(1002L);

        boolean applied = projection.apply(new MarketOpenedEvent(EVENT_ID, MARKET_ID, 1003, now));

        assertThat(applied).isTrue();
        assertThat(capturedMarketFields()).containsEntry(RedisKeys.FIELD_STATUS, MarketStatus.ACTIVE.name());
    }

    @Test
    void marketSettledMarksTheMarketTerminal() {
        storedMarketVersion(1005L);

        projection.apply(new MarketSettledEvent(EVENT_ID, MARKET_ID, "real-madrid", 1006, now));

        assertThat(capturedMarketFields()).containsEntry(RedisKeys.FIELD_STATUS, MarketStatus.SETTLED.name());
    }

    @Test
    void oddsUpdatedDoesNotTouchStatusSoASuspendedMarketStaysSuspended() {
        storedMarketVersion(1002L);

        projection.apply(new OddsUpdatedEvent(EVENT_ID, MARKET_ID, "real-madrid",
                new BigDecimal("1.95"), 1003, now));

        assertThat(capturedMarketFields()).doesNotContainKey(RedisKeys.FIELD_STATUS);
    }

    @Test
    void matchStartedSetsEventStatusLive() {
        storedEventVersion(null);

        boolean applied = projection.apply(new MatchStartedEvent(EVENT_ID, 1000, now));

        assertThat(applied).isTrue();
        verify(hash).putAll(eq(RedisKeys.event(EVENT_ID)), any());
    }

    @Test
    void matchFinishedIsIgnoredWhenAnOlderVersionArrivesLate() {
        storedEventVersion(2000L);

        boolean applied = projection.apply(new MatchFinishedEvent(EVENT_ID, 1999, now));

        assertThat(applied).isFalse();
        verify(hash, never()).putAll(eq(RedisKeys.event(EVENT_ID)), any());
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> capturedMarketFields() {
        ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
        verify(hash).putAll(eq(RedisKeys.market(MARKET_ID)), captor.capture());
        return captor.getValue();
    }
}
