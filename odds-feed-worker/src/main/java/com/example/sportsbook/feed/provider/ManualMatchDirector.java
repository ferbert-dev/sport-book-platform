package com.example.sportsbook.feed.provider;

import com.example.sportsbook.common.MarketOpenedEvent;
import com.example.sportsbook.common.MarketSettledEvent;
import com.example.sportsbook.common.MarketSuspendedEvent;
import com.example.sportsbook.common.MatchFinishedEvent;
import com.example.sportsbook.common.MatchStartedEvent;
import com.example.sportsbook.common.OddsUpdatedEvent;
import com.example.sportsbook.common.SportsEvent;
import com.example.sportsbook.feed.messaging.SportsEventPublisher;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Drives matches on demand for the demo control panel, alongside the scripted looping provider.
 *
 * <p>These publish to the same {@code sports-events} topic through the same publisher, so nothing
 * downstream can tell a hand-driven match from a scripted one — which is the point: the control
 * panel exercises the real pipeline rather than a side channel.
 *
 * <p>Events are published with {@code concatMapCompletable}, so a burst is emitted strictly in
 * order and back-pressured the same way the scripted stream is.
 */
public class ManualMatchDirector {

    private static final List<String> HOME = List.of("real-madrid", "barcelona", "bayern",
            "liverpool", "inter", "ajax", "porto", "napoli");
    private static final List<String> AWAY = List.of("arsenal", "juventus", "chelsea", "milan",
            "dortmund", "sevilla", "roma", "benfica");

    private final Map<String, ManualMatch> matches = new ConcurrentHashMap<>();
    private final SportsEventPublisher publisher;

    public ManualMatchDirector(SportsEventPublisher publisher) {
        this.publisher = publisher;
    }

    public Optional<ManualMatch> find(String eventId) {
        return Optional.ofNullable(matches.get(eventId));
    }

    public List<ManualMatch> all() {
        return List.copyOf(matches.values());
    }

    /**
     * Creates a match with random teams and prices, then publishes MATCH_STARTED, MARKET_OPENED and
     * one ODDS_UPDATED per selection so it is immediately bettable.
     */
    public Single<ManualMatch> startRandomMatch() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String home = HOME.get(random.nextInt(HOME.size()));
        String away = AWAY.get(random.nextInt(AWAY.size()));
        String suffix = Long.toString(System.currentTimeMillis() % 100000);

        ManualMatch match = new ManualMatch("event-" + suffix, "market-" + suffix);
        match.putOdds(home, randomPrice(1.60, 3.20));
        match.putOdds("draw", randomPrice(2.80, 4.20));
        match.putOdds(away, randomPrice(1.60, 3.20));
        matches.put(match.eventId(), match);

        List<SportsEvent> burst = new ArrayList<>();
        burst.add(new MatchStartedEvent(match.eventId(), match.nextVersion(), Instant.now()));
        burst.add(new MarketOpenedEvent(match.eventId(), match.marketId(), match.nextVersion(), Instant.now()));
        match.odds().forEach((selectionId, price) -> burst.add(new OddsUpdatedEvent(
                match.eventId(), match.marketId(), selectionId, price, match.nextVersion(), Instant.now())));

        return publishAll(burst).andThen(Single.just(match));
    }

    /** Nudges every selection's price by up to +/-15%, floored just above evens. */
    public Single<ManualMatch> driftOdds(ManualMatch match) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<SportsEvent> burst = new ArrayList<>();

        for (Map.Entry<String, BigDecimal> entry : Map.copyOf(match.odds()).entrySet()) {
            double factor = 1.0 + random.nextDouble(-0.15, 0.15);
            BigDecimal moved = entry.getValue()
                    .multiply(BigDecimal.valueOf(factor))
                    .setScale(2, RoundingMode.HALF_UP)
                    .max(new BigDecimal("1.01"));
            match.putOdds(entry.getKey(), moved);
            burst.add(new OddsUpdatedEvent(match.eventId(), match.marketId(), entry.getKey(),
                    moved, match.nextVersion(), Instant.now()));
        }
        return publishAll(burst).andThen(Single.just(match));
    }

    public Completable suspend(ManualMatch match) {
        return publishAll(List.of(new MarketSuspendedEvent(
                match.eventId(), match.marketId(), match.nextVersion(), Instant.now())));
    }

    public Completable open(ManualMatch match) {
        return publishAll(List.of(new MarketOpenedEvent(
                match.eventId(), match.marketId(), match.nextVersion(), Instant.now())));
    }

    /**
     * Finishes and settles the match.
     *
     * <p>MATCH_FINISHED precedes MARKET_SETTLED so the event is terminal before the market resolves,
     * matching the order a real provider would use. The MARKET_SETTLED version doubles as the
     * settlement version that settlement-service fences on.
     *
     * @param winningSelectionId explicit winner, or null to pick one at random
     */
    public Single<String> settle(ManualMatch match, String winningSelectionId) {
        List<String> selections = List.copyOf(match.odds().keySet());
        String winner = winningSelectionId != null && match.odds().containsKey(winningSelectionId)
                ? winningSelectionId
                : selections.get(ThreadLocalRandom.current().nextInt(selections.size()));

        List<SportsEvent> burst = List.of(
                new MatchFinishedEvent(match.eventId(), match.nextVersion(), Instant.now()),
                new MarketSettledEvent(match.eventId(), match.marketId(), winner,
                        match.nextVersion(), Instant.now()));

        match.markSettled();
        return publishAll(burst).andThen(Single.just(winner));
    }

    private Completable publishAll(List<SportsEvent> events) {
        return Flowable.fromIterable(events).concatMapCompletable(publisher::publish);
    }

    private static BigDecimal randomPrice(double low, double high) {
        return BigDecimal.valueOf(ThreadLocalRandom.current().nextDouble(low, high))
                .setScale(2, RoundingMode.HALF_UP);
    }
}
