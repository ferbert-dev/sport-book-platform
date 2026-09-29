package com.example.sportsbook.simulator.feed;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A match created on demand through the dev control API. Sequence numbers are not tracked here:
 * the {@link ProviderFeed} numbers every message on the stream.
 */
public class ManualMatch {

    private final String eventId;
    private final String marketId;
    private final Map<String, BigDecimal> odds = new LinkedHashMap<>();
    private boolean settled;

    public ManualMatch(String eventId, String marketId) {
        this.eventId = eventId;
        this.marketId = marketId;
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
