package com.example.sportsbook.bet.repository;

import com.example.sportsbook.common.EventStatus;
import com.example.sportsbook.common.MarketStatus;
import com.example.sportsbook.common.RedisKeys;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Reads the current-state projection to validate bets.
 *
 * <p>A thin reader of its own rather than a shared library: common-domain stays plain Java with no
 * Spring Data dependency, and bet-service needs different fields than odds-service does.
 */
@Repository
public class MarketStateReader {

    private final StringRedisTemplate redis;

    public MarketStateReader(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public Optional<EventStatus> findEventStatus(String eventId) {
        Object status = redis.opsForHash().get(RedisKeys.event(eventId), RedisKeys.FIELD_STATUS);
        if (status == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(EventStatus.valueOf(String.valueOf(status)));
        } catch (IllegalArgumentException unknown) {
            return Optional.empty();
        }
    }

    public Optional<MarketState> findMarket(String marketId) {
        Map<Object, Object> fields = redis.opsForHash().entries(RedisKeys.market(marketId));
        if (fields.isEmpty()) {
            return Optional.empty();
        }
        Object status = fields.get(RedisKeys.FIELD_STATUS);
        Object lastUpdatedAt = fields.get(RedisKeys.FIELD_LAST_UPDATED_AT);
        if (status == null || lastUpdatedAt == null) {
            return Optional.empty();
        }
        MarketStatus marketStatus;
        try {
            marketStatus = MarketStatus.valueOf(String.valueOf(status));
        } catch (IllegalArgumentException unknown) {
            // Unknown must never read as bettable.
            marketStatus = MarketStatus.SUSPENDED;
        }
        return Optional.of(new MarketState(marketId, marketStatus, Instant.parse(String.valueOf(lastUpdatedAt))));
    }

    public Optional<BigDecimal> findOdds(String marketId, String selectionId) {
        Object odds = redis.opsForHash().get(RedisKeys.marketOdds(marketId), selectionId);
        return odds == null ? Optional.empty() : Optional.of(new BigDecimal(String.valueOf(odds)));
    }

    public record MarketState(String marketId, MarketStatus status, Instant lastUpdatedAt) {
    }
}
