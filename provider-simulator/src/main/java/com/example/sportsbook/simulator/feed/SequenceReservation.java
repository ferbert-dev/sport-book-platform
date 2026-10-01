package com.example.sportsbook.simulator.feed;

import java.util.Optional;
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
 *
 * <p>FIX 4: the persisted state is the pair (session epoch, reservedUpTo), written as
 * {@code "epoch:reservedUpTo"}. A normal restart keeps the epoch and continues above the reservation;
 * only {@link #nextSession} starts a new epoch from sequence 1. A file holding a single number — the
 * format before FIX 4 — reads as epoch 1.
 */
public final class SequenceReservation {

    public static final long DEFAULT_BLOCK = 100_000;

    private final long epoch;
    private final long block;
    private long reservedUpTo;

    private SequenceReservation(long epoch, long start, long block) {
        this.epoch = epoch;
        this.block = block;
        this.reservedUpTo = start + block;
    }

    /** What the file holds: the session epoch and the highest sequence reserved in it. */
    public record Persisted(long epoch, long reservedUpTo) {
    }

    public static String format(long epoch, long reservedUpTo) {
        return epoch + ":" + reservedUpTo;
    }

    /**
     * Reads {@code "epoch:reservedUpTo"}, and also a single number — the file written before FIX 4 —
     * as epoch 1, so an existing reservation keeps working after the upgrade.
     *
     * @throws IllegalArgumentException if the content is neither form
     */
    public static Persisted parse(String content) {
        String[] parts = content.trim().split(":");
        try {
            return switch (parts.length) {
                case 1 -> new Persisted(1, Long.parseLong(parts[0].trim()));
                case 2 -> new Persisted(Long.parseLong(parts[0].trim()), Long.parseLong(parts[1].trim()));
                default -> throw new IllegalArgumentException(invalid(content));
            };
        } catch (NumberFormatException notANumber) {
            throw new IllegalArgumentException(invalid(content), notANumber);
        }
    }

    private static String invalid(String content) {
        return "expected \"epoch:reservedUpTo\" or a single number, got '" + content + "'";
    }

    /**
     * @param clockMillis the wall clock, used when nothing was persisted yet or it is ahead
     * @param persisted   the reservation a previous run wrote, if any; without one the session is epoch 1
     */
    public static SequenceReservation startingAt(long clockMillis, Optional<Persisted> persisted, long block) {
        return new SequenceReservation(
                persisted.map(Persisted::epoch).orElse(1L),
                Math.max(clockMillis, persisted.map(Persisted::reservedUpTo).orElse(0L)),
                block);
    }

    /** A new provider session: epoch + 1, numbered again from sequence 1 ({@code start() == 0}). */
    public SequenceReservation nextSession(long block) {
        return new SequenceReservation(epoch + 1, 0, block);
    }

    /** The provider session this reservation numbers; persisted together with reservedUpTo. */
    public long epoch() {
        return epoch;
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
