package com.example.sportsbook.common;

import java.time.Instant;

/** Match went live. Match-level, so keyed on eventId. */
public record MatchStartedEvent(
        String eventId,
        long version,
        Instant timestamp
) implements SportsEvent {

    @Override
    public SportsEventType type() {
        return SportsEventType.MATCH_STARTED;
    }

    @Override
    public String partitionKey() {
        return eventId;
    }
}
