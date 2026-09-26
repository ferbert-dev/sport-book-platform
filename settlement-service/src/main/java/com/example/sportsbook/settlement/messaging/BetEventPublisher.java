package com.example.sportsbook.settlement.messaging;

import com.example.sportsbook.common.BetEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Publishes betting-domain events, keyed by bet id so per-bet ordering is preserved. */
@Component
public class BetEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(BetEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String betEventsTopic;

    public BetEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                             ObjectMapper objectMapper,
                             @Value("${sportsbook.topics.bet-events}") String betEventsTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.betEventsTopic = betEventsTopic;
    }

    /**
     * Sends synchronously and lets failures propagate.
     *
     * <p>Called inside the settlement transaction on purpose: if the broker is unreachable the
     * transaction rolls back, the offset is not committed, and Kafka redelivers MARKET_SETTLED so the
     * whole settlement is retried. The alternative — publish after commit — would silently lose
     * BET_SETTLED whenever the broker was down, because the idempotency fence would then skip the retry.
     */
    public void publish(BetEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(betEventsTopic, event.partitionKey(), payload).get();
            log.info("BET_EVENT_PUBLISHED type={} betId={}", event.type(), event.betId());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted publishing " + event.type(), interrupted);
        } catch (Exception failure) {
            throw new IllegalStateException("Could not publish " + event.type()
                    + " for bet " + event.betId(), failure);
        }
    }
}
