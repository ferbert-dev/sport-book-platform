package com.example.sportsbook.state;

import com.example.sportsbook.state.config.SportsbookTopics;
import com.example.sportsbook.state.config.StateProcessorProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties({StateProcessorProperties.class, SportsbookTopics.class})
public class StateProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(StateProcessorApplication.class, args);
    }
}
