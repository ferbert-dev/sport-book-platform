package com.example.sportsbook.feed.provider;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A match created on demand through the dev control API.
 *
 * <p>Each one carries its own sequence counter. Versions only need to be monotonic per
 * market/event key — which is exactly how the downstream version guard is scoped — so independent
 * matches can number themselves independently without colliding.
 */
public class ManualMatch {

    private final String eventId;
    private final String marketId;
    private final Map<String, BigDecimal> odds = new LinkedHashMap<>();

    private long sequence = 1000L;
    private boolean settled;

    public ManualMatch(String eventId, String marketId) {
        this.eventId = eventId;
        this.marketId = marketId;
    }

    public long nextVersion() {
        return ++sequence;
    }

    public String eventId() {
        return eventId;
    }

    public String marketId() {
        return marketId;
    }

    public Map<String, BigDecimal> odds() {
        return odds;
    }

    public void putOdds(String selectionId, BigDecimal price) {
        odds.put(selectionId, price);
    }

    public boolean isSettled() {
        return settled;
    }

    public void markSettled() {
        this.settled = true;
    }
}
