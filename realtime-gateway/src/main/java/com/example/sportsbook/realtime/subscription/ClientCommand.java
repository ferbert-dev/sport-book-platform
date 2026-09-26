package com.example.sportsbook.realtime.subscription;

/** Inbound client frame: {@code {"action":"SUBSCRIBE","eventId":"event-123"}}. */
public record ClientCommand(ClientAction action, String eventId) {
}
