package com.example.sportsbook.state.config;

import com.example.sportsbook.common.SportsbookJson;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Uses the shared mapper so Spring and Vert.x modules agree on the wire format. */
@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return SportsbookJson.create();
    }
}
