package com.example.sportsbook.state.domain;

/**
 * Decides whether an incoming event is newer than the state we already hold.
 *
 * <p>Kafka gives us at-least-once delivery and per-partition ordering only. Across a partition
 * rebalance or a producer retry we can legitimately see the same version twice, or an older one.
 * The projection must therefore be version-guarded rather than trusting arrival order.
 */
public final class VersionGuard {

    private VersionGuard() {
    }

    /**
     * @param currentVersion  version already stored, or {@code null} when nothing is stored yet
     * @param incomingVersion version carried by the event being processed
     * @return true when the event should be applied
     */
    public static boolean shouldApply(Long currentVersion, long incomingVersion) {
        return currentVersion == null || incomingVersion > currentVersion;
    }
}
