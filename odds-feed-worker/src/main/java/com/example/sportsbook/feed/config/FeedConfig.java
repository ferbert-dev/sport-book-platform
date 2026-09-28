package com.example.sportsbook.feed.config;

import java.time.Duration;

/** Worker configuration, environment-variable driven so it matches the Spring services' style. */
public record FeedConfig(
        String kafkaBootstrapServers,
        String sportsEventsTopic,
        String providerUrl,
        Duration providerConnectTimeout,
        Duration reconnectInitialDelay,
        Duration reconnectMaxDelay
) {

    public static FeedConfig fromEnv() {
        return new FeedConfig(
                env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                env("SPORTS_EVENTS_TOPIC", "sports-events"),
                // The provider's streaming endpoint. Locally that is provider-simulator; in a real
                // deployment it would be the vendor's feed URL.
                env("PROVIDER_URL", "ws://localhost:8086/provider/stream"),
                Duration.ofMillis(Long.parseLong(env("PROVIDER_CONNECT_TIMEOUT_MS", "5000"))),
                Duration.ofMillis(Long.parseLong(env("PROVIDER_RECONNECT_INITIAL_MS", "500"))),
                Duration.ofMillis(Long.parseLong(env("PROVIDER_RECONNECT_MAX_MS", "30000")))
        );
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
