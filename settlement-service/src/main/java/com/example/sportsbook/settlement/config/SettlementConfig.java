package com.example.sportsbook.settlement.config;

import com.example.sportsbook.common.SportsbookJson;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class SettlementConfig {

    private static final Logger log = LoggerFactory.getLogger(SettlementConfig.class);

    @Bean
    public ObjectMapper objectMapper() {
        return SportsbookJson.create();
    }

    /**
     * A settlement that fails is retried until it succeeds, with a growing pause (1s up to 30s).
     *
     * <p>Without this bean Spring Kafka's default applies: ten attempts with no pause, then the
     * record is logged and skipped. A database outage of a few seconds would then drop
     * MARKET_SETTLED for good and leave every bet on that market OPEN.
     *
     * <p>The price is that a record which can never succeed blocks its partition, and with it the
     * settlement of other markets on that partition. That is the safer failure for money: it is
     * loud and loses nothing. A production system would bound the retries and park the record on a
     * dead-letter topic with an alert; this project keeps to its two topics.
     */
    @Bean
    public DefaultErrorHandler settlementErrorHandler() {
        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxInterval(30_000L);
        DefaultErrorHandler handler = new DefaultErrorHandler(backOff);
        handler.setRetryListeners((record, failure, attempt) ->
                log.error("SETTLEMENT_DELIVERY_FAILED topic={} partition={} offset={} attempt={} reason={}",
                        record.topic(), record.partition(), record.offset(), attempt, failure.getMessage()));
        return handler;
    }
}
