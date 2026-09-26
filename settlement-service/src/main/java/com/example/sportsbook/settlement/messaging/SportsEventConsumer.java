package com.example.sportsbook.settlement.messaging;

import com.example.sportsbook.common.MarketSettledEvent;
import com.example.sportsbook.common.SportsEvent;
import com.example.sportsbook.settlement.service.SettlementService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Consumes {@code sports-events} and acts only on MARKET_SETTLED. */
@Component
public class SportsEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(SportsEventConsumer.class);

    private final SettlementService settlementService;
    private final ObjectMapper objectMapper;

    public SportsEventConsumer(SettlementService settlementService, ObjectMapper objectMapper) {
        this.settlementService = settlementService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${sportsbook.topics.sports-events}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(String payload) {
        SportsEvent event;
        try {
            event = objectMapper.readValue(payload, SportsEvent.class);
        } catch (Exception parseFailure) {
            // Skip rather than rethrow: an unparseable record would otherwise block the partition.
            log.error("SPORTS_EVENT_UNPARSEABLE length={}", payload == null ? 0 : payload.length(),
                    parseFailure);
            return;
        }

        // Every other event type is another consumer's business.
        if (event instanceof MarketSettledEvent settled) {
            settlementService.settle(settled);
        }
    }
}
