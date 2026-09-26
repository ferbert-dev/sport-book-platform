package com.example.sportsbook.feed.provider;

import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Stands in for Sportradar/Betgenius. Emits a scripted match lifecycle, then loops.
 *
 * <p>The script deliberately injects one duplicate and one gap so the sequence validation and
 * resynchronization paths are exercised on every cycle rather than only in tests.
 */
public class SimulatedSportsProvider {

    private final String matchId;
    private final String marketId;
    private final Duration interval;

    public SimulatedSportsProvider(String matchId, String marketId, Duration interval) {
        this.matchId = matchId;
        this.marketId = marketId;
        this.interval = interval;
    }

    /**
     * Cold stream of provider messages.
     *
     * <p>{@code BackpressureStrategy.BUFFER} is deliberate: a real provider socket cannot be told
     * to slow down, so the only options are buffer or drop. Buffering keeps the stream lossless and
     * makes any consumer slowness visible as memory pressure instead of silent data loss.
     */
    public Flowable<ProviderMessage> messages() {
        List<ProviderMessage> script = script();
        return Flowable.interval(0, interval.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)
                .map(tick -> script.get((int) (tick % script.size())))
                .toObservable()
                .toFlowable(BackpressureStrategy.BUFFER);
    }

    /** The scripted lifecycle from the design: prices drift, market locks, reopens, then settles. */
    List<ProviderMessage> script() {
        List<ProviderMessage> messages = new ArrayList<>();
        long seq = 100;

        messages.add(match(seq++, "MATCH_START"));
        messages.add(price(seq++, "real-madrid", "2.10"));
        messages.add(price(seq++, "real-madrid", "2.05"));

        // Duplicate: the provider resends a message we have already processed.
        messages.add(price(seq - 1, "real-madrid", "2.05"));

        messages.add(price(seq++, "real-madrid", "1.95"));
        messages.add(market(seq++, "MARKET_LOCK"));
        messages.add(market(seq++, "MARKET_UNLOCK"));

        // Gap: skip one sequence number so SEQUENCE_GAP_DETECTED fires.
        seq++;
        messages.add(price(seq++, "real-madrid", "1.85"));

        messages.add(match(seq++, "MATCH_END"));
        messages.add(result(seq, "real-madrid"));
        return messages;
    }

    private ProviderMessage match(long seq, String type) {
        return new ProviderMessage(seq, type, matchId, null, null, null, null, Instant.now());
    }

    private ProviderMessage market(long seq, String type) {
        return new ProviderMessage(seq, type, matchId, marketId, null, null, null, Instant.now());
    }

    private ProviderMessage price(long seq, String outcome, String price) {
        return new ProviderMessage(seq, "PRICE_CHANGE", matchId, marketId, outcome,
                new BigDecimal(price), null, Instant.now());
    }

    private ProviderMessage result(long seq, String winner) {
        return new ProviderMessage(seq, "MARKET_RESULT", matchId, marketId, null, null, winner,
                Instant.now());
    }
}
