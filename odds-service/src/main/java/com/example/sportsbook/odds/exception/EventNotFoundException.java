package com.example.sportsbook.odds.exception;

public class EventNotFoundException extends RuntimeException {

    public EventNotFoundException(String eventId) {
        super("No current state for event " + eventId);
    }
}
