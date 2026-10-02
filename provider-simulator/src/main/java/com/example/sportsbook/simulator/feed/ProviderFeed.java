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
 * <p>Every message also carries the session epoch (FIX 4). {@link #startNewSession} is a provider
 * restart: the epoch goes up and numbering starts again at 1, so the (epoch, sequence) pair — which
 * the worker turns into the event version — keeps growing while the bare sequence does not.
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
    private long sessionEpoch;
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

    /**
     * A provider restart: the next message is {@code (newEpoch, 1)}. Connected workers stay
     * subscribed and simply see the new epoch. The replay window is kept, so a worker that missed the
     * end of the old session still gets it before the new one.
     *
     * <p>The caller must raise {@link #setEmitLimit} for the new session: the old limit was a
     * sequence of the old numbering.
     *
     * @throws IllegalArgumentException unless {@code newEpoch} is higher than the current epoch;
     *                                  a session that went back would be dropped as stale
     */
    public void startNewSession(long newEpoch) {
        if (newEpoch <= sessionEpoch) {
            throw new IllegalArgumentException("new session epoch must be above " + sessionEpoch + ", got " + newEpoch);
        }
        sessionEpoch = newEpoch;
        lastSequence = 0;
    }

    /** Stamps the epoch and the next sequence onto a draft and pushes it to connected subscribers. */
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
     * Messages at or after {@code (fromEpoch, fromSequence)}, oldest first: the rest of that session
     * and every later one. Positions compare by epoch first, then by sequence within the epoch.
     * Injected duplicates are not replayed: the window holds each position once.
     *
     * <p>Replay then subscribe must happen in the same event-loop turn (the stream handler does
     * both inside one call), so no live message can slip in between the two.
     */
    public Replay replayFrom(long fromEpoch, long fromSequence) {
        List<ProviderMessage> missed = replay.stream()
                .filter(message -> compare(message.sessionEpoch(), message.sequenceNumber(), fromEpoch, fromSequence) >= 0)
                .toList();
        // Complete when nothing the worker asked for has left the window: the oldest message kept
        // (or, with an empty window, the next one to be emitted) is not after the requested position.
        boolean complete = replay.isEmpty()
                ? compare(sessionEpoch, lastSequence + 1, fromEpoch, fromSequence) <= 0
                : compare(replay.peekFirst().sessionEpoch(), replay.peekFirst().sequenceNumber(),
                        fromEpoch, fromSequence) <= 0;
        return new Replay(missed, complete);
    }

    /** Orders two stream positions: epoch first, then sequence. The simulator's own rule, no worker types. */
    private static int compare(long epoch, long sequence, long otherEpoch, long otherSequence) {
        return epoch != otherEpoch ? Long.compare(epoch, otherEpoch) : Long.compare(sequence, otherSequence);
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

    public long sessionEpoch() {
        return sessionEpoch;
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
