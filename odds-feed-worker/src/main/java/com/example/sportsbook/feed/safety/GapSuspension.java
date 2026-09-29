package com.example.sportsbook.feed.safety;

import com.example.sportsbook.common.MarketSuspendedEvent;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * FIX 3: suspend-by-default on a sequence gap.
 *
 * <p>A gap means provider messages were lost, and nothing says which markets they were for. The
 * dangerous one is a lost {@code MARKET_LOCK}: prices keep flowing, so the staleness sweep never
 * fires, the market stays {@code ACTIVE}, and bets are accepted on a market the provider closed.
 * So every active market is suspended, and reopens only when the provider itself sends
 * {@code MARKET_UNLOCK} again.
 *
 * <h2>Why version = receivedSequence - 1</h2>
 *
 * <p>The synthetic suspension must pass the projection's version guard, and must not outrank what
 * the provider sends next:
 *
 * <pre>
 *   every market's stored version  &lt;  expected  &lt;=  received - 1  &lt;  received  &lt;=  later provider messages
 * </pre>
 *
 * <ul>
 *   <li>Higher than anything already stored for the market (all of it came before the gap), so
 *       the guard applies it.</li>
 *   <li>Lower than the message that revealed the gap and everything after it, so a real
 *       {@code MARKET_UNLOCK} from the provider still wins and reopens the market.</li>
 * </ul>
 *
 * <p>{@code received - 1} is a sequence inside the lost range, so no real message will ever claim
 * it: the cursor has already moved past it. Versions are per market key, so every market can share it.
 */
public final class GapSuspension {

    private GapSuspension() {
    }

    public static List<MarketSuspendedEvent> suspendAll(Map<String, String> activeMarkets,
                                                        long receivedSequence, Instant now) {
        long version = receivedSequence - 1;
        return activeMarkets.entrySet().stream()
                .map(market -> new MarketSuspendedEvent(market.getValue(), market.getKey(), version, now))
                .toList();
    }
}
