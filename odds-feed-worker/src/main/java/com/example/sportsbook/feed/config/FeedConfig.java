package com.example.sportsbook.feed.config;

import java.time.Duration;

/** Worker configuration, environment-variable driven so it matches the Spring services' style. */
public record FeedConfig(
        String kafkaBootstrapServers,
        String sportsEventsTopic,
        Duration providerInterval,
        String eventId,
        String marketId
) {

    public static FeedConfig fromEnv() {
        return new FeedConfig(
                env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                env("SPORTS_EVENTS_TOPIC", "sports-events"),
                Duration.ofMillis(Long.parseLong(env("PROVIDER_INTERVAL_MS", "2000"))),
                env("DEMO_EVENT_ID", "event-123"),
                env("DEMO_MARKET_ID", "market-456")
        );
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
