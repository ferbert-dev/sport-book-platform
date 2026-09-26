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

    public String snapshot() {
        return "odds_feed_messages_received_total=" + oddsFeedMessagesReceivedTotal.get()
                + " odds_feed_messages_invalid_total=" + oddsFeedMessagesInvalidTotal.get()
                + " odds_feed_duplicates_total=" + oddsFeedDuplicatesTotal.get()
                + " odds_feed_sequence_gaps_total=" + oddsFeedSequenceGapsTotal.get()
                + " odds_feed_events_published_total=" + oddsFeedEventsPublishedTotal.get()
                + " odds_feed_publish_failures_total=" + oddsFeedPublishFailuresTotal.get();
    }
}
