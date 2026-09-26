package com.example.sportsbook.realtime.subscription;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@code eventId -> connections} index.
 *
 * <p>Generic over the connection type so it can be unit-tested without a real WebSocket.
 *
 * <p>Concurrent because Vert.x may serve sockets on several event-loop threads: a subscribe arriving
 * on one loop can race a Kafka-driven fan-out on another.
 *
 * <p>Also keeps the reverse {@code connection -> eventIds} index, so disconnect cleanup is O(number
 * of that client's subscriptions) rather than a scan of every event.
 */
public class SubscriptionRegistry<C> {

    private final Map<String, Set<C>> byEvent = new ConcurrentHashMap<>();
    private final Map<C, Set<String>> byConnection = new ConcurrentHashMap<>();

    public void subscribe(String eventId, C connection) {
        byEvent.computeIfAbsent(eventId, key -> ConcurrentHashMap.newKeySet()).add(connection);
        byConnection.computeIfAbsent(connection, key -> ConcurrentHashMap.newKeySet()).add(eventId);
    }

    public void unsubscribe(String eventId, C connection) {
        byEvent.computeIfPresent(eventId, (key, connections) -> {
            connections.remove(connection);
            return connections.isEmpty() ? null : connections;
        });
        byConnection.computeIfPresent(connection, (key, eventIds) -> {
            eventIds.remove(eventId);
            return eventIds.isEmpty() ? null : eventIds;
        });
    }

    /** Removes the connection from every event it was subscribed to. */
    public void remove(C connection) {
        Set<String> eventIds = byConnection.remove(connection);
        if (eventIds == null) {
            return;
        }
        for (String eventId : eventIds) {
            byEvent.computeIfPresent(eventId, (key, connections) -> {
                connections.remove(connection);
                return connections.isEmpty() ? null : connections;
            });
        }
    }

    /** Connections that should receive events for this event id. */
    public Set<C> subscribersOf(String eventId) {
        return Collections.unmodifiableSet(byEvent.getOrDefault(eventId, Set.of()));
    }

    public Set<String> subscriptionsOf(C connection) {
        return Collections.unmodifiableSet(byConnection.getOrDefault(connection, Set.of()));
    }

    public int connectionCount() {
        return byConnection.size();
    }

    public int eventCount() {
        return byEvent.size();
    }
}
