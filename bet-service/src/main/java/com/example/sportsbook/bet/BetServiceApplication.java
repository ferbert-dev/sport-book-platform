package com.example.sportsbook.bet;

import com.example.sportsbook.bet.config.BetServiceProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(BetServiceProperties.class)
public class BetServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(BetServiceApplication.class, args);
    }
}
