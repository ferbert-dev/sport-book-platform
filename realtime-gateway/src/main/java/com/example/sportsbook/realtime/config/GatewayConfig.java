package com.example.sportsbook.realtime.config;

/** Gateway configuration, environment-variable driven. */
public record GatewayConfig(
        int port,
        String kafkaBootstrapServers,
        String sportsEventsTopic,
        String consumerGroupId
) {

    public static GatewayConfig fromEnv() {
        return new GatewayConfig(
                Integer.parseInt(env("SERVER_PORT", "8083")),
                env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                env("SPORTS_EVENTS_TOPIC", "sports-events"),
                // Unique per instance: every gateway needs every event, so instances must not
                // share a group or they would split the partitions between them.
                env("CONSUMER_GROUP_ID", "realtime-gateway-" + java.util.UUID.randomUUID())
        );
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
