package com.example.sportsbook.realtime.subscription;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Uses String stand-ins for sockets; the registry is generic precisely so this is possible. */
class SubscriptionRegistryTest {

    private final SubscriptionRegistry<String> registry = new SubscriptionRegistry<>();

    @Test
    void subscriptionIsRegistered() {
        registry.subscribe("event-123", "client-a");

        assertThat(registry.subscribersOf("event-123")).containsExactly("client-a");
        assertThat(registry.subscriptionsOf("client-a")).containsExactly("event-123");
    }

    @Test
    void eventIsDeliveredOnlyToSubscribersOfThatEvent() {
        registry.subscribe("event-123", "client-a");
        registry.subscribe("event-123", "client-b");
        registry.subscribe("event-999", "client-c");

        assertThat(registry.subscribersOf("event-123")).containsExactlyInAnyOrder("client-a", "client-b");
        assertThat(registry.subscribersOf("event-999")).containsExactly("client-c");
    }

    @Test
    void eventWithNoSubscribersYieldsAnEmptySetRatherThanNull() {
        assertThat(registry.subscribersOf("nobody-watching")).isEmpty();
    }

    @Test
    void subscribingTwiceToTheSameEventDoesNotDuplicateDelivery() {
        registry.subscribe("event-123", "client-a");
        registry.subscribe("event-123", "client-a");

        assertThat(registry.subscribersOf("event-123")).hasSize(1);
    }

    @Test
    void unsubscribeRemovesOnlyThatOneSubscription() {
        registry.subscribe("event-123", "client-a");
        registry.subscribe("event-999", "client-a");

        registry.unsubscribe("event-123", "client-a");

        assertThat(registry.subscribersOf("event-123")).isEmpty();
        assertThat(registry.subscribersOf("event-999")).containsExactly("client-a");
    }

    @Test
    void closedConnectionIsRemovedFromEveryEventItWatched() {
        registry.subscribe("event-1", "client-a");
        registry.subscribe("event-2", "client-a");
        registry.subscribe("event-1", "client-b");

        registry.remove("client-a");

        assertThat(registry.subscribersOf("event-1")).containsExactly("client-b");
        assertThat(registry.subscribersOf("event-2")).isEmpty();
        assertThat(registry.subscriptionsOf("client-a")).isEmpty();
        assertThat(registry.connectionCount()).isEqualTo(1);
    }

    @Test
    void emptyEventEntriesArePrunedSoTheIndexDoesNotLeak() {
        registry.subscribe("event-1", "client-a");
        assertThat(registry.eventCount()).isEqualTo(1);

        registry.remove("client-a");

        assertThat(registry.eventCount()).isZero();
        assertThat(registry.connectionCount()).isZero();
    }

    @Test
    void removingAnUnknownConnectionIsANoOp() {
        registry.remove("never-connected");

        assertThat(registry.connectionCount()).isZero();
    }
}
