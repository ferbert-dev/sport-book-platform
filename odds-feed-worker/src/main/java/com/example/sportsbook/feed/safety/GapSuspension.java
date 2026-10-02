package com.example.sportsbook.feed.safety;

import com.example.sportsbook.common.MarketSuspendedEvent;
import com.example.sportsbook.common.OddsUpdatedEvent;
import com.example.sportsbook.common.SportsEvent;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * FIX 3: suspend-by-default on a sequence gap.
 *
 * <p>A gap means provider messages were lost, and nothing says which markets they were for. The
 * dangerous one is a lost {@code MARKET_LOCK}: prices keep flowing, so the staleness sweep never
 * fires, the market stays {@code ACTIVE}, and bets are accepted on a market the provider closed.
 * So every active market is suspended, and reopens only when the provider itself sends
 * {@code MARKET_UNLOCK} again.
 *
 * <p>Versions here are event versions, {@code ProviderVersion.compose(epoch, sequence)} (FIX 4),
 * never bare sequences: the projection stores composite versions, so a suspension built from a
 * bare sequence would be dropped by its version guard.
 *
 * <h2>Why version = receivedVersion - 1</h2>
 *
 * <p>The synthetic suspension must pass the projection's version guard, and must not outrank what
 * the provider sends next:
 *
 * <pre>
 *   every market's stored version  &lt;  expected  &lt;=  receivedVersion - 1  &lt;  receivedVersion  &lt;=  later messages
 * </pre>
 *
 * <ul>
 *   <li>Higher than anything already stored for the market (all of it came before the gap), so
 *       the guard applies it.</li>
 *   <li>Lower than the message that revealed the gap and everything after it, so a real
 *       {@code MARKET_UNLOCK} from the provider still wins and reopens the market.</li>
 * </ul>
 *
 * <p>{@code compose(e, s) - 1 == compose(e, s - 1)}: a position inside the lost range, which no real
 * message will claim since the cursor has moved past it. For a new session,
 * {@code compose(e, 1) - 1 == compose(e, 0)} still outranks every version of epoch {@code e - 1}.
 * Versions are per market key, so every market can share it.
 */
public final class GapSuspension {

    private GapSuspension() {
    }

    /**
     * FIX 3, quarantine: suspending "every active market" only covers markets this process has
     * published. After a restart that list starts empty, so a gap then suspends nothing, and a
     * market whose MARKET_LOCK was lost would keep taking bets as its prices flow in. So once any
     * gap has been seen, a market this process does not know yet is suspect: before its first price
     * is published, it is suspended too.
     *
     * <p>Only prices trigger it. A first MARKET_UNLOCK or MARKET_SUSPENDED is the provider stating
     * the status itself, and a MARKET_RESULT closes the market anyway. The version is
     * {@code version - 1} for the same reason as {@link #suspendAll}: everything stored for this
     * market predates the gap, and the price itself, at {@code version}, still outranks it.
     */
    public static Optional<MarketSuspendedEvent> quarantine(SportsEvent next, long version, boolean gapSeen,
                                                            ActiveMarkets active, Instant now) {
        if (gapSeen && next instanceof OddsUpdatedEvent price && !active.knows(price.marketId())) {
            return Optional.of(new MarketSuspendedEvent(price.eventId(), price.marketId(), version - 1, now));
        }
        return Optional.empty();
    }

    public static List<MarketSuspendedEvent> suspendAll(Map<String, String> activeMarkets,
                                                        long receivedVersion, Instant now) {
        long version = receivedVersion - 1;
        return activeMarkets.entrySet().stream()
                .map(market -> new MarketSuspendedEvent(market.getValue(), market.getKey(), version, now))
                .toList();
    }
}
