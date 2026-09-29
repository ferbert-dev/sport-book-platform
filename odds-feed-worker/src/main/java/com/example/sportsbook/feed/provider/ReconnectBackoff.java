package com.example.sportsbook.feed.provider;

import java.time.Duration;

/**
 * Exponential reconnect delay: initial, 2x, 4x, ... capped at max, and back to initial once a
 * connection succeeds.
 *
 * <p>Without the cap a long provider outage would push retries out to hours; without the reset a
 * short blip after a long outage would still wait the maximum.
 *
 * <p>Not thread-safe by design: used only from the worker verticle's context.
 */
public class ReconnectBackoff {

    private final long initialMillis;
    private final long maxMillis;
    private int failures;

    public ReconnectBackoff(Duration initial, Duration max) {
        this.initialMillis = initial.toMillis();
        this.maxMillis = max.toMillis();
    }

    /** Delay before the next attempt; each call counts as one more consecutive failure. */
    public long nextDelayMillis() {
        // Shift capped at 30 so the multiplication can never overflow a long.
        long delay = initialMillis << Math.min(failures, 30);
        failures++;
        return Math.min(delay, maxMillis);
    }

    public void reset() {
        failures = 0;
    }

    public int consecutiveFailures() {
        return failures;
    }
}
