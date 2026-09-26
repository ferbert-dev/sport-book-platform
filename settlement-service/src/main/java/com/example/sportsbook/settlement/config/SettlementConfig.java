package com.example.sportsbook.settlement.config;

import com.example.sportsbook.common.SportsbookJson;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SettlementConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return SportsbookJson.create();
    }
}
