package com.example.sportsbook.common;

import java.time.Instant;

/** Match ended. Match-level, so keyed on eventId. */
public record MatchFinishedEvent(
        String eventId,
        long version,
        Instant timestamp
) implements SportsEvent {

    @Override
    public SportsEventType type() {
        return SportsEventType.MATCH_FINISHED;
    }

    @Override
    public String partitionKey() {
        return eventId;
    }
}
