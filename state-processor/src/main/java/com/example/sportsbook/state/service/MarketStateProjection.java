package com.example.sportsbook.state.service;

import com.example.sportsbook.common.EventStatus;
import com.example.sportsbook.common.MarketOpenedEvent;
import com.example.sportsbook.common.MarketSettledEvent;
import com.example.sportsbook.common.MarketStatus;
import com.example.sportsbook.common.MarketSuspendedEvent;
import com.example.sportsbook.common.MatchFinishedEvent;
import com.example.sportsbook.common.MatchStartedEvent;
import com.example.sportsbook.common.OddsUpdatedEvent;
import com.example.sportsbook.common.SportsEvent;
import com.example.sportsbook.common.RedisKeys;
import com.example.sportsbook.state.domain.VersionGuard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Projects the {@code sports-events} stream onto the current-state view in Redis.
 *
 * <p>Kafka holds WHAT HAPPENED; Redis holds WHAT IS TRUE NOW. Redis is therefore disposable — it can
 * be rebuilt by replaying the topic from the beginning.
 *
 * <p>Read-modify-write on the version field is safe without locking because market-scoped events are
 * partitioned by {@code marketId} and match-level events by {@code eventId}: all events for a given
 * key are handled in order by a single consumer thread.
 */
@Service
public class MarketStateProjection {

    private static final Logger log = LoggerFactory.getLogger(MarketStateProjection.class);

    private final StringRedisTemplate redis;

    public MarketStateProjection(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** @return true when the event advanced state, false when it was ignored as stale */
    public boolean apply(SportsEvent event) {
        return switch (event) {
            case OddsUpdatedEvent e -> applyOddsUpdated(e);
            case MarketSuspendedEvent e -> applyMarketStatus(e, e.marketId(), MarketStatus.SUSPENDED);
            case MarketOpenedEvent e -> applyMarketStatus(e, e.marketId(), MarketStatus.ACTIVE);
            case MarketSettledEvent e -> applyMarketStatus(e, e.marketId(), MarketStatus.SETTLED);
            case MatchStartedEvent e -> applyEventStatus(e, EventStatus.LIVE);
            case MatchFinishedEvent e -> applyEventStatus(e, EventStatus.FINISHED);
        };
    }

    private boolean applyOddsUpdated(OddsUpdatedEvent event) {
        if (!marketVersionAdvances(event.marketId(), event.version())) {
            logStale(event.type().name(), event.marketId(), event.version());
            return false;
        }
        registerMarket(event.eventId(), event.marketId());
        redis.opsForHash().put(RedisKeys.marketOdds(event.marketId()),
                event.selectionId(), event.odds().toPlainString());
        touchMarket(event.marketId(), event.eventId(), null, event.version());

        log.info("ODDS_UPDATED eventId={} marketId={} selectionId={} odds={} version={}",
                event.eventId(), event.marketId(), event.selectionId(), event.odds(), event.version());
        return true;
    }

    private boolean applyMarketStatus(SportsEvent event, String marketId, MarketStatus status) {
        if (!marketVersionAdvances(marketId, event.version())) {
            logStale(event.type().name(), marketId, event.version());
            return false;
        }
        registerMarket(event.eventId(), marketId);
        touchMarket(marketId, event.eventId(), status, event.version());

        log.info("{} eventId={} marketId={} status={} version={}",
                event.type().name(), event.eventId(), marketId, status, event.version());
        return true;
    }

    private boolean applyEventStatus(SportsEvent event, EventStatus status) {
        String key = RedisKeys.event(event.eventId());
        Long current = readVersion(key);
        if (!VersionGuard.shouldApply(current, event.version())) {
            logStale(event.type().name(), event.eventId(), event.version());
            return false;
        }
        redis.opsForHash().putAll(key, Map.of(
                RedisKeys.FIELD_STATUS, status.name(),
                RedisKeys.FIELD_VERSION, Long.toString(event.version()),
                RedisKeys.FIELD_LAST_UPDATED_AT, event.timestamp().toString()));

        log.info("{} eventId={} status={} version={}",
                event.type().name(), event.eventId(), status, event.version());
        return true;
    }

    private boolean marketVersionAdvances(String marketId, long incomingVersion) {
        return VersionGuard.shouldApply(readVersion(RedisKeys.market(marketId)), incomingVersion);
    }

    private Long readVersion(String key) {
        Object raw = redis.opsForHash().get(key, RedisKeys.FIELD_VERSION);
        return raw == null ? null : Long.parseLong(raw.toString());
    }

    /** Writes version and lastUpdatedAt, and status only when the caller supplies one. */
    private void touchMarket(String marketId, String eventId, MarketStatus status, long version) {
        Map<String, String> fields = new java.util.HashMap<>();
        fields.put(RedisKeys.FIELD_EVENT_ID, eventId);
        fields.put(RedisKeys.FIELD_VERSION, Long.toString(version));
        fields.put(RedisKeys.FIELD_LAST_UPDATED_AT, java.time.Instant.now().toString());
        if (status != null) {
            fields.put(RedisKeys.FIELD_STATUS, status.name());
        }
        redis.opsForHash().putAll(RedisKeys.market(marketId), fields);
    }

    private void registerMarket(String eventId, String marketId) {
        redis.opsForSet().add(RedisKeys.eventMarkets(eventId), marketId);
        redis.opsForSet().add(RedisKeys.allMarkets(), marketId);
    }

    private void logStale(String type, String id, long version) {
        log.debug("STALE_EVENT_IGNORED type={} id={} version={}", type, id, version);
    }
}
