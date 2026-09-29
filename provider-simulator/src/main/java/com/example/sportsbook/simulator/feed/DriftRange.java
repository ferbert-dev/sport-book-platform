package com.example.sportsbook.simulator.feed;

import java.util.concurrent.ThreadLocalRandom;

/**
 * How long to wait between automatic odds drifts: a fresh random gap in {@code [minMs, maxMs]}
 * each time, since a real feed does not tick on a metronome. Equal bounds give a fixed cadence.
 */
public record DriftRange(long minMs, long maxMs) {

    /** Below ~10ms the timer, not the feed, becomes what you are measuring. */
    public static final long FLOOR_MS = 10;
    public static final long CEILING_MS = 60_000;

    public DriftRange {
        if (minMs < FLOOR_MS || maxMs > CEILING_MS || minMs > maxMs) {
            throw new IllegalArgumentException("drift range must satisfy " + FLOOR_MS
                    + " <= minMs <= maxMs <= " + CEILING_MS + ", got " + minMs + ".." + maxMs);
        }
    }

    public long sampleDelayMillis() {
        return minMs == maxMs ? minMs : ThreadLocalRandom.current().nextLong(minMs, maxMs + 1);
    }
}
