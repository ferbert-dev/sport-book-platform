package com.example.sportsbook.simulator.feed;

import com.example.sportsbook.simulator.wire.ProviderMessage;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.processors.PublishProcessor;

import java.time.Clock;

/**
 * The provider's single outbound stream. Every message — scripted or hand-driven — goes through
 * here and gets the next sequence number, so a connected worker sees ONE contiguous sequence,
 * which is how real provider feeds number a connection.
 *
 * <p>One global sequence is also monotonic per market and per match, which is all the downstream
 * version guard needs, since the worker uses the sequence as the domain version.
 *
 * <p><b>Hot stream:</b> messages emitted while no worker is connected are gone, as with a real
 * feed. A worker that reconnects sees the jump in sequence numbers as a gap.
 *
 * <p><b>Not thread-safe by design.</b> Stamping and emitting must happen together, in order, so
 * the feed is confined to one Vert.x context (one verticle); no locks needed.
 */
public final class ProviderFeed {

    private final PublishProcessor<ProviderMessage> stream = PublishProcessor.create();
    private final Clock clock;

    private long lastSequence;
    private ProviderMessage lastEmitted;

    /**
     * @param startSequence last sequence considered already used. Seed it from the wall clock so a
     *                      restarted simulator keeps moving forward instead of replaying numbers the
     *                      version guard has already seen.
     */
    public ProviderFeed(long startSequence, Clock clock) {
        this.lastSequence = startSequence;
        this.clock = clock;
    }

    /** Stamps the next sequence onto a draft and pushes it to connected subscribers. */
    public ProviderMessage emit(ProviderMessage draft) {
        ProviderMessage message = draft.stamped(++lastSequence, clock.instant());
        lastEmitted = message;
        stream.onNext(message);
        return message;
    }

    /** Re-sends the previous message unchanged: a provider duplicate. */
    public void resendLast() {
        if (lastEmitted != null) {
            stream.onNext(lastEmitted);
        }
    }

    /** Burns one sequence number without sending it: a provider gap. */
    public void skipSequence() {
        lastSequence++;
    }

    public long lastSequence() {
        return lastSequence;
    }

    /**
     * Hot stream of stamped messages. Note: a {@link PublishProcessor} does not buffer — a
     * subscriber that stops requesting gets {@code MissingBackpressureException}. Deciding what
     * to do about slow subscribers is the stream handler's job.
     */
    public Flowable<ProviderMessage> messages() {
        return stream;
    }

    public void complete() {
        stream.onComplete();
    }
}
