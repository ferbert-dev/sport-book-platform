package com.example.sportsbook.common;

/**
 * The Redis key layout, shared in spirit with odds-service (which reads these same keys).
 *
 * <pre>
 *   event:{eventId}           hash: status, version, lastUpdatedAt
 *   market:{marketId}         hash: eventId, status, version, lastUpdatedAt, suspendReason (optional)
 *   market:{marketId}:odds    hash: selectionId -> odds
 *   event:{eventId}:markets   set:  marketIds belonging to the event
 * </pre>
 */
public final class RedisKeys {

    public static final String FIELD_STATUS = "status";
    public static final String FIELD_VERSION = "version";
    public static final String FIELD_LAST_UPDATED_AT = "lastUpdatedAt";
    public static final String FIELD_EVENT_ID = "eventId";
    /**
     * Present only while a market is suspended by the staleness sweep rather than by a feed event.
     * It is what lets a returning price reopen the market: a suspension the provider asked for has
     * no reason stored and still needs the provider's own MARKET_OPENED.
     */
    public static final String FIELD_SUSPEND_REASON = "suspendReason";
    public static final String SUSPEND_REASON_STALE_FEED = "STALE_FEED";

    private RedisKeys() {
    }

    public static String event(String eventId) {
        return "event:" + eventId;
    }

    public static String market(String marketId) {
        return "market:" + marketId;
    }

    public static String marketOdds(String marketId) {
        return "market:" + marketId + ":odds";
    }

    /** Lets odds-service assemble a full event snapshot without scanning the keyspace. */
    public static String eventMarkets(String eventId) {
        return "event:" + eventId + ":markets";
    }

    /** Registry of every market we have seen, so the staleness sweep never needs KEYS/SCAN. */
    public static String allMarkets() {
        return "markets";
    }
}
