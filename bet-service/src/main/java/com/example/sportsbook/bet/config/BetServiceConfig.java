package com.example.sportsbook.bet.config;

import com.example.sportsbook.common.SportsbookJson;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
public class BetServiceConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return SportsbookJson.create();
    }

    /** Injected so freshness checks are testable without sleeping. */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
