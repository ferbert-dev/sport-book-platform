package com.example.sportsbook.simulator.feed;

import java.util.OptionalLong;

/**
 * Keeps provider sequences monotonic across simulator restarts (the hi/lo pattern).
 *
 * <p>The wall clock alone is not enough: it can step backwards, and a burst of more than one message
 * per millisecond outruns it, so a restart could seed below the previous high-water mark and the
 * version guard would discard everything as stale. Instead a block of sequences is <em>reserved</em>
 * and persisted before it is used; a restart starts above the last reservation. Only the reservation
 * is written, once per block, never per message.
 *
 * <p>The next block is reserved when the current one is half used, so the asynchronous write has
 * half a block of headroom to land before the sequence could pass the persisted mark.
 */
public final class SequenceReservation {

    public static final long DEFAULT_BLOCK = 100_000;

    private final long block;
    private long reservedUpTo;

    private SequenceReservation(long start, long block) {
        this.block = block;
        this.reservedUpTo = start + block;
    }

    /**
     * @param clockMillis        the wall clock, used when nothing was persisted yet or it is ahead
     * @param persistedReserved  the reservation a previous run wrote, if any
     */
    public static SequenceReservation startingAt(long clockMillis, OptionalLong persistedReserved, long block) {
        return new SequenceReservation(Math.max(clockMillis, persistedReserved.orElse(0L)), block);
    }

    /** The first sequence this run may hand out, minus one: the value to seed the feed with. */
    public long start() {
        return reservedUpTo - block;
    }

    /** What must be durable before the feed starts emitting. */
    public long reservedUpTo() {
        return reservedUpTo;
    }

    /**
     * @return the new reservation to persist, or empty while the current block still has more than
     *         half left
     */
    public OptionalLong extendIfNeeded(long lastSequence) {
        if (lastSequence < reservedUpTo - block / 2) {
            return OptionalLong.empty();
        }
        reservedUpTo = Math.max(reservedUpTo, lastSequence) + block;
        return OptionalLong.of(reservedUpTo);
    }
}
