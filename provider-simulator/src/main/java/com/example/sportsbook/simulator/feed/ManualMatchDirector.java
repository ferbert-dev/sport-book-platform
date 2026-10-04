package com.example.sportsbook.simulator.feed;

import com.example.sportsbook.simulator.wire.ProviderMessage;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

/**
 * Drives matches on demand for the demo control panel.
 *
 * <p>Emits provider messages into the same {@link ProviderFeed} as the scripted loop, so hand-driven
 * matches reach Kafka through the worker's full pipeline — validation, sequence check,
 * normalization — and nothing downstream can tell them apart from scripted traffic.
 *
 * <p>Confined to the simulator verticle's context like the feed, hence a plain {@link HashMap}.
 */
public class ManualMatchDirector {

    private static final List<String> HOME = List.of("real-madrid", "barcelona", "bayern",
            "liverpool", "inter", "ajax", "porto", "napoli");
    private static final List<String> AWAY = List.of("arsenal", "juventus", "chelsea", "milan",
            "dortmund", "sevilla", "roma", "benfica");

    /** Ids travel as Kafka keys, Redis key parts and URL path segments, so keep them plain. */
    private static final Pattern EVENT_ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    private final Map<String, ManualMatch> matches = new HashMap<>();
    private final ProviderFeed feed;
    private final Set<String> reservedMarketIds;

    public ManualMatchDirector(ProviderFeed feed) {
        this(feed, Set.of());
    }

    /**
     * @param reservedMarketIds market ids owned by something else on the same feed — the scripted
     *                          match — that no dev match may take
     */
    public ManualMatchDirector(ProviderFeed feed, Set<String> reservedMarketIds) {
        this.feed = feed;
        this.reservedMarketIds = Set.copyOf(reservedMarketIds);
    }

    public Optional<ManualMatch> find(String eventId) {
        return Optional.ofNullable(matches.get(eventId));
    }

    public List<ManualMatch> all() {
        return List.copyOf(matches.values());
    }

    /** Creates a match under a generated id; see {@link #startMatch(String)}. */
    public ManualMatch startRandomMatch() {
        String eventId;
        do {
            eventId = "event-" + ThreadLocalRandom.current().nextInt(10_000, 100_000);
        } while (matches.containsKey(eventId) || marketInUse(marketIdFor(eventId)));
        return startMatch(eventId);
    }

    /**
     * Creates a match with random teams and prices under the given event id, then sends
     * MATCH_START, MARKET_UNLOCK and one PRICE_CHANGE per selection so it is immediately bettable.
     *
     * <p>The market id is derived from the event id: {@code event-777 -> market-777},
     * {@code derby -> market-derby}.
     *
     * @throws IllegalArgumentException if the id is not 1-64 of {@code [A-Za-z0-9_-]}
     * @throws IllegalStateException    if a match with this id already exists in this simulator,
     *                                  or its derived market id is already in use — the derivation
     *                                  is not one-to-one ({@code event-777} and {@code 777} both
     *                                  give {@code market-777}), and two events on one market key
     *                                  would mix their odds and settle each other's bets
     */
    public ManualMatch startMatch(String eventId) {
        if (eventId == null || !EVENT_ID.matcher(eventId).matches()) {
            throw new IllegalArgumentException("eventId must be 1-64 characters of [A-Za-z0-9_-], got '"
                    + eventId + "'");
        }
        if (matches.containsKey(eventId)) {
            throw new IllegalStateException("match " + eventId + " already exists");
        }
        String marketId = marketIdFor(eventId);
        if (marketInUse(marketId)) {
            throw new IllegalStateException("market " + marketId + " (derived from " + eventId
                    + ") is already used by another match");
        }
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String home = HOME.get(random.nextInt(HOME.size()));
        String away = AWAY.get(random.nextInt(AWAY.size()));

        ManualMatch match = new ManualMatch(eventId, marketId);
        match.putOdds(home, randomPrice(1.60, 3.20));
        match.putOdds("draw", randomPrice(2.80, 4.20));
        match.putOdds(away, randomPrice(1.60, 3.20));
        matches.put(match.eventId(), match);

        feed.emit(ProviderMessage.matchStart(match.eventId()));
        feed.emit(ProviderMessage.marketUnlock(match.eventId(), match.marketId()));
        match.odds().forEach((selectionId, price) ->
                feed.emit(ProviderMessage.priceChange(match.eventId(), match.marketId(), selectionId, price)));
        return match;
    }

    /** Nudges every selection's price by up to +/-15%, floored just above evens. */
    public ManualMatch driftOdds(ManualMatch match) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (Map.Entry<String, BigDecimal> entry : Map.copyOf(match.odds()).entrySet()) {
            double factor = 1.0 + random.nextDouble(-0.15, 0.15);
            BigDecimal moved = entry.getValue()
                    .multiply(BigDecimal.valueOf(factor))
                    .setScale(2, RoundingMode.HALF_UP)
                    .max(new BigDecimal("1.01"));
            match.putOdds(entry.getKey(), moved);
            feed.emit(ProviderMessage.priceChange(match.eventId(), match.marketId(), entry.getKey(), moved));
        }
        return match;
    }

    public void suspend(ManualMatch match) {
        feed.emit(ProviderMessage.marketLock(match.eventId(), match.marketId()));
        match.setMarketLocked(true);
    }

    public void open(ManualMatch match) {
        feed.emit(ProviderMessage.marketUnlock(match.eventId(), match.marketId()));
        match.setMarketLocked(false);
    }

    /**
     * The recovery snapshot a provider sends after it restarts: the current status of every market
     * that is still in play, MARKET_UNLOCK or MARKET_LOCK. A consumer that suspended everything on
     * the session change (it cannot know what the old session sent last) gets the authoritative
     * status back from the provider itself, so open markets reopen without anyone stepping in.
     *
     * <p>Settled matches are skipped: their result was final in the old session.
     *
     * @return how many markets were announced
     */
    public int announceMarketStates() {
        int announced = 0;
        for (ManualMatch match : matches.values()) {
            if (match.isSettled()) {
                continue;
            }
            feed.emit(match.isMarketLocked()
                    ? ProviderMessage.marketLock(match.eventId(), match.marketId())
                    : ProviderMessage.marketUnlock(match.eventId(), match.marketId()));
            announced++;
        }
        return announced;
    }

    /**
     * Finishes and settles the match. MATCH_END precedes MARKET_RESULT so the event is terminal
     * before the market resolves, the order a real provider would use.
     *
     * @param winningSelectionId explicit winner, or null to pick one at random
     * @return the winning selection
     */
    public String settle(ManualMatch match, String winningSelectionId) {
        List<String> selections = List.copyOf(match.odds().keySet());
        String winner = winningSelectionId != null && match.odds().containsKey(winningSelectionId)
                ? winningSelectionId
                : selections.get(ThreadLocalRandom.current().nextInt(selections.size()));

        feed.emit(ProviderMessage.matchEnd(match.eventId()));
        feed.emit(ProviderMessage.marketResult(match.eventId(), match.marketId(), winner));
        match.markSettled();
        return winner;
    }

    private boolean marketInUse(String marketId) {
        return reservedMarketIds.contains(marketId)
                || matches.values().stream().anyMatch(match -> match.marketId().equals(marketId));
    }

    static String marketIdFor(String eventId) {
        return "market-" + (eventId.startsWith("event-") ? eventId.substring("event-".length()) : eventId);
    }

    private static BigDecimal randomPrice(double low, double high) {
        return BigDecimal.valueOf(ThreadLocalRandom.current().nextDouble(low, high))
                .setScale(2, RoundingMode.HALF_UP);
    }
}
