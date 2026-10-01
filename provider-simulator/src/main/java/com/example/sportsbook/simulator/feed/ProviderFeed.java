package com.example.sportsbook.simulator.feed;

import com.example.sportsbook.simulator.wire.ProviderMessage;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.processors.PublishProcessor;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * The provider's single outbound stream. Every message — scripted or hand-driven — goes through
 * here and gets the next sequence number, so a connected worker sees ONE contiguous sequence,
 * which is how real provider feeds number a connection.
 *
 * <p>One global sequence is also monotonic per market and per match, which is all the downstream
 * version guard needs, since the worker uses the sequence as the domain version.
 *
 * <p><b>Hot stream with a replay window:</b> the last {@code replayCapacity} messages are kept, so
 * a worker that reconnects with a cursor ({@link #replayFrom}) gets what it missed before live
 * traffic resumes. Only a disconnect longer than the window leaves a real gap — the case a real
 * provider covers with a snapshot.
 *
 * <p><b>Not thread-safe by design.</b> Stamping and emitting must happen together, in order, so
 * the feed is confined to one Vert.x context (one verticle); no locks needed.
 */
public final class ProviderFeed {

    private final PublishProcessor<ProviderMessage> stream = PublishProcessor.create();
    private final Clock clock;
    private final int replayCapacity;
    private final Deque<ProviderMessage> replay = new ArrayDeque<>();

    private long lastSequence;
    private final long sessionEpoch;
    private long emitLimit = Long.MAX_VALUE;
    private ProviderMessage lastEmitted;

    /**
     * @param startSequence last sequence considered already used. Seed it from a
     *                      {@link SequenceReservation} so a restarted simulator keeps moving forward
     *                      instead of replaying numbers the version guard has already seen.
     */
    public ProviderFeed(long startSequence, Clock clock, long sessionEpoch) {
        this(startSequence, clock, 10_000, sessionEpoch);
    }

    public ProviderFeed(long startSequence, Clock clock, int replayCapacity, long sessionEpoch) {
        this.lastSequence = startSequence;
        this.clock = clock;
        this.replayCapacity = replayCapacity;
	    this.sessionEpoch = sessionEpoch;
    }

    /** Stamps the next sequence onto a draft and pushes it to connected subscribers. */
    public ProviderMessage emit(ProviderMessage draft) {
        ensureWithinLimit();
        ProviderMessage message = draft.stamped(sessionEpoch, ++lastSequence, clock.instant());
        lastEmitted = message;
        replay.addLast(message);
        if (replay.size() > replayCapacity) {
            replay.removeFirst();
        }
        stream.onNext(message);
        return message;
    }

    /**
     * Messages with {@code sequenceNumber >= fromSequence}, oldest first. Injected duplicates are
     * not replayed: the window holds each sequence once.
     *
     * <p>Replay then subscribe must happen in the same event-loop turn (the stream handler does
     * both inside one call), so no live message can slip in between the two.
     */
    public Replay replayFrom(long fromSequence) {
        List<ProviderMessage> missed = replay.stream()
                .filter(message -> message.sequenceNumber() >= fromSequence)
                .toList();
        long oldestKept = replay.isEmpty() ? lastSequence + 1 : replay.peekFirst().sequenceNumber();
        return new Replay(missed, fromSequence >= oldestKept);
    }

    /**
     * @param complete false when part of the requested range has already left the window; the
     *                 worker will see that part as a sequence gap
     */
    public record Replay(List<ProviderMessage> messages, boolean complete) {
    }

    /** Re-sends the previous message unchanged: a provider duplicate. */
    public void resendLast() {
        if (lastEmitted != null) {
            stream.onNext(lastEmitted);
        }
    }

    /** Burns one sequence number without sending it: a provider gap. */
    public void skipSequence() {
        ensureWithinLimit();
        lastSequence++;
    }

    /**
     * Highest sequence the feed may hand out: the last reservation known to be durable. Emitting
     * past it fails closed, because a restart would then reuse the numbers above it.
     */
    public void setEmitLimit(long limit) {
        this.emitLimit = limit;
    }

    private void ensureWithinLimit() {
        if (lastSequence + 1 > emitLimit) {
            throw new IllegalStateException("SEQUENCE_RESERVATION_EXHAUSTED lastSequence=" + lastSequence
                    + " durableLimit=" + emitLimit);
        }
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
