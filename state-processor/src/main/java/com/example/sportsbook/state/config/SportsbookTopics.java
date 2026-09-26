package com.example.sportsbook.state.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Topic names, injected so they can be overridden per environment. */
@ConfigurationProperties(prefix = "sportsbook.topics")
public record SportsbookTopics(String sportsEvents, String betEvents) {
}
