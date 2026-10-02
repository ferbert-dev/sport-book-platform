package com.example.sportsbook.feed;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Counters for the worker. Deliberately plain: this module has no Spring Actuator, so the values
 * are logged periodically. In production these would be exported through Micrometer/Prometheus.
 */
public class FeedMetrics {

    public final AtomicLong oddsFeedMessagesReceivedTotal = new AtomicLong();
    public final AtomicLong oddsFeedMessagesInvalidTotal = new AtomicLong();
    public final AtomicLong oddsFeedDuplicatesTotal = new AtomicLong();
    public final AtomicLong oddsFeedSequenceGapsTotal = new AtomicLong();
    public final AtomicLong oddsFeedEventsPublishedTotal = new AtomicLong();
    public final AtomicLong oddsFeedPublishFailuresTotal = new AtomicLong();
    public final AtomicLong oddsFeedSessionChangesTotal = new AtomicLong();
    public final AtomicLong oddsFeedStaleSessionDropsTotal = new AtomicLong();
    /** FIX 3: markets suspended because a sequence gap may have hidden their MARKET_LOCK. */
    public final AtomicLong oddsFeedGapSuspensionsTotal = new AtomicLong();

    public String snapshot() {
        return "odds_feed_messages_received_total=" + oddsFeedMessagesReceivedTotal.get()
                + " odds_feed_messages_invalid_total=" + oddsFeedMessagesInvalidTotal.get()
                + " odds_feed_duplicates_total=" + oddsFeedDuplicatesTotal.get()
                + " odds_feed_sequence_gaps_total=" + oddsFeedSequenceGapsTotal.get()
                + " odds_feed_events_published_total=" + oddsFeedEventsPublishedTotal.get()
                + " odds_feed_publish_failures_total=" + oddsFeedPublishFailuresTotal.get()
                + " odds_feed_gap_suspensions_total=" + oddsFeedGapSuspensionsTotal.get()
                + " odds_feed_session_changes_total=" + oddsFeedSessionChangesTotal.get()
                + " odds_feed_stale_session_drops_total=" + oddsFeedStaleSessionDropsTotal.get();
    }
}
