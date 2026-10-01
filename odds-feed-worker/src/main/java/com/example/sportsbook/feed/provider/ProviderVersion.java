package com.example.sportsbook.feed.provider;

/**
 * FIX 4: one event version from the provider's (session epoch, sequence number) pair.
 *
 * <p>The provider's sequence number used to be the version on its own. When a provider starts a new
 * session it numbers from 1 again, so new events would carry versions lower than what the projection
 * already holds and the version guard would drop all of them. Putting the epoch in front keeps
 * versions growing across sessions:
 *
 * <pre>
 *   version = epoch * 10^15 + sequence        2_000_000_000_000_001 = session 2, message 1
 * </pre>
 *
 * <p>The factor is a power of ten so a version in a log reads as "session, message" at a glance, and
 * it still leaves room for 9_222 sessions of up to 10^15 messages each in a {@code long}. The mapping
 * is deterministic: a replayed message gets the same version, so redelivery stays idempotent.
 */
public final class ProviderVersion {

    static final long EPOCH_FACTOR = 1_000_000_000_000_000L;

    private ProviderVersion() {
    }

    /**
     * @throws IllegalArgumentException if epoch is negative or sequence is outside {@code [0, 10^15)}
     * @throws ArithmeticException      if the epoch is too large for the version to fit in a long
     */
    public static long compose(long epoch, long sequence) {
        requireNonNegative(epoch, "epoch");
        requireNonNegative(sequence, "sequence");
        if (sequence >= EPOCH_FACTOR) {
            throw new IllegalArgumentException("sequence must be below " + EPOCH_FACTOR + ", got " + sequence);
        }
        return Math.addExact(Math.multiplyExact(epoch, EPOCH_FACTOR), sequence);
    }

    public static long epochOf(long version) {
        requireNonNegative(version, "version");
        return version / EPOCH_FACTOR;
    }

    public static long sequenceOf(long version) {
        requireNonNegative(version, "version");
        return version % EPOCH_FACTOR;
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " cannot be negative, got " + value);
        }
    }
}
