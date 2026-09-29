package com.example.sportsbook.feed.safety;

import com.example.sportsbook.common.MarketOpenedEvent;
import com.example.sportsbook.common.MarketSettledEvent;
import com.example.sportsbook.common.MarketSuspendedEvent;
import com.example.sportsbook.common.MatchFinishedEvent;
import com.example.sportsbook.common.MatchStartedEvent;
import com.example.sportsbook.common.OddsUpdatedEvent;
import com.example.sportsbook.common.SportsEvent;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * FIX 3: the markets this worker has published and not yet seen settled — the set a sequence gap
 * could be hiding a {@code MARKET_LOCK} for.
 *
 * <p>Built only from events Kafka has acknowledged, so it never includes a market downstream has
 * not heard of. In memory: a restarted worker starts empty, and until each market sends again,
 * state-processor's staleness sweep is the safety net for it. The worker deliberately does not
 * read Redis to rebuild this (the feed path never touches the projection).
 *
 * <p>Not thread-safe by design: used only on the worker verticle's context.
 */
public final class ActiveMarkets {

    /** marketId -> eventId, in first-seen order so suspensions go out in a stable order. */
    private final Map<String, String> markets = new LinkedHashMap<>();
    /** Settled is terminal: these must never be suspended again. Grows with settled markets only. */
    private final Set<String> settled = new HashSet<>();

    public void track(SportsEvent published) {
        switch (published) {
            case OddsUpdatedEvent e -> markets.put(e.marketId(), e.eventId());
            case MarketOpenedEvent e -> markets.put(e.marketId(), e.eventId());
            // Still active in the sense that matters: it can be reopened, so a gap could hide that.
            case MarketSuspendedEvent e -> markets.put(e.marketId(), e.eventId());
            case MarketSettledEvent e -> {
                markets.remove(e.marketId());
                settled.add(e.marketId());
            }
            case MatchStartedEvent ignored -> { }
            case MatchFinishedEvent ignored -> { }
        }
    }

    /** Seen by this process, active or settled: either way not a stranger to quarantine. */
    public boolean knows(String marketId) {
        return markets.containsKey(marketId) || settled.contains(marketId);
    }

    public Map<String, String> snapshot() {
        return Map.copyOf(markets);
    }

    public int size() {
        return markets.size();
    }
}
