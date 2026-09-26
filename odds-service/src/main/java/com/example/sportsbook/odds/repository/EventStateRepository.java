package com.example.sportsbook.odds.repository;

import com.example.sportsbook.common.EventStatus;
import com.example.sportsbook.common.MarketStatus;
import com.example.sportsbook.common.RedisKeys;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Read-only access to the current-state projection written by state-processor.
 *
 * <p>Reads Redis only. Querying PostgreSQL here would couple the hot read path to the durable bet
 * store and defeat the point of the projection.
 */
@Repository
public class EventStateRepository {

    private final StringRedisTemplate redis;

    public EventStateRepository(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public Optional<EventState> findEvent(String eventId) {
        Map<Object, Object> fields = redis.opsForHash().entries(RedisKeys.event(eventId));
        if (fields.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new EventState(
                eventId,
                enumOrDefault(fields.get(RedisKeys.FIELD_STATUS), EventStatus.SCHEDULED),
                parseLong(fields.get(RedisKeys.FIELD_VERSION)),
                parseInstant(fields.get(RedisKeys.FIELD_LAST_UPDATED_AT))));
    }

    /** Sorted so the response ordering is stable across calls. */
    public Set<String> findMarketIds(String eventId) {
        Set<String> members = redis.opsForSet().members(RedisKeys.eventMarkets(eventId));
        return members == null ? Collections.emptySet() : new TreeSet<>(members);
    }

    public Optional<MarketState> findMarket(String marketId) {
        Map<Object, Object> fields = redis.opsForHash().entries(RedisKeys.market(marketId));
        if (fields.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new MarketState(
                marketId,
                String.valueOf(fields.get(RedisKeys.FIELD_EVENT_ID)),
                enumOrDefault(fields.get(RedisKeys.FIELD_STATUS), MarketStatus.SUSPENDED),
                parseLong(fields.get(RedisKeys.FIELD_VERSION)),
                parseInstant(fields.get(RedisKeys.FIELD_LAST_UPDATED_AT))));
    }

    /** Selection id -> decimal odds, insertion-ordered by selection id. */
    public Map<String, BigDecimal> findOdds(String marketId) {
        Map<Object, Object> raw = redis.opsForHash().entries(RedisKeys.marketOdds(marketId));
        Map<String, BigDecimal> odds = new LinkedHashMap<>();
        new TreeSet<>(raw.keySet().stream().map(String::valueOf).toList())
                .forEach(selectionId -> odds.put(selectionId, new BigDecimal(String.valueOf(raw.get(selectionId)))));
        return odds;
    }

    /**
     * A market with no odds and no status has never been projected.
     * Defaulting an unknown status to SUSPENDED is deliberate: unknown must never read as bettable.
     */
    private static <E extends Enum<E>> E enumOrDefault(Object raw, E fallback) {
        if (raw == null) {
            return fallback;
        }
        try {
            return Enum.valueOf(fallback.getDeclaringClass(), String.valueOf(raw));
        } catch (IllegalArgumentException unknownValue) {
            return fallback;
        }
    }

    private static long parseLong(Object raw) {
        return raw == null ? 0L : Long.parseLong(String.valueOf(raw));
    }

    private static Instant parseInstant(Object raw) {
        return raw == null ? Instant.EPOCH : Instant.parse(String.valueOf(raw));
    }

    public record EventState(String eventId, EventStatus status, long version, Instant lastUpdatedAt) {
    }

    public record MarketState(String marketId, String eventId, MarketStatus status, long version,
                              Instant lastUpdatedAt) {
    }
}
