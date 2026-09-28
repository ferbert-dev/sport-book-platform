package com.example.sportsbook.simulator.config;

import java.time.Duration;

/** Simulator configuration, environment-variable driven like every other service. */
public record SimulatorConfig(
        int port,
        String streamPath,
        Duration scriptInterval,
        String scriptedEventId,
        String scriptedMarketId,
        boolean autoplay
) {

    public static SimulatorConfig fromEnv() {
        return new SimulatorConfig(
                // Same port the dev panel always used, so nginx and the UI only change host name.
                Integer.parseInt(env("SIMULATOR_PORT", "8086")),
                env("PROVIDER_STREAM_PATH", "/provider/stream"),
                Duration.ofMillis(Long.parseLong(env("PROVIDER_INTERVAL_MS", "2000"))),
                env("DEMO_EVENT_ID", "event-123"),
                env("DEMO_MARKET_ID", "market-456"),
                // Turn off to drive matches purely from the dev panel with no scripted noise.
                Boolean.parseBoolean(env("FEED_AUTOPLAY", "true"))
        );
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
