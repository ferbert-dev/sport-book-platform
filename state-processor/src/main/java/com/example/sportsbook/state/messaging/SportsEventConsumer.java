package com.example.sportsbook.state.messaging;

import com.example.sportsbook.common.SportsEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.sportsbook.state.service.MarketStateProjection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code sports-events} and feeds the projection.
 *
 * <p>Deserializes from a plain String rather than using a typed Kafka deserializer: the Vert.x
 * producer writes JSON with a {@code type} discriminator, and parsing it here with the shared
 * ObjectMapper keeps both sides on one contract with no broker-side type headers.
 */
@Component
public class SportsEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(SportsEventConsumer.class);

    private final MarketStateProjection projection;
    private final ObjectMapper objectMapper;
    private final Counter processed;
    private final Counter ignored;
    private final Counter failed;

    public SportsEventConsumer(MarketStateProjection projection,
                               ObjectMapper objectMapper,
                               MeterRegistry meterRegistry) {
        this.projection = projection;
        this.objectMapper = objectMapper;
        this.processed = Counter.builder("sports_events_processed_total").register(meterRegistry);
        this.ignored = Counter.builder("sports_events_ignored_total").register(meterRegistry);
        this.failed = Counter.builder("sports_events_failed_total").register(meterRegistry);
    }

    @KafkaListener(
            topics = "${sportsbook.topics.sports-events}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(String payload) {
        SportsEvent event;
        try {
            event = objectMapper.readValue(payload, SportsEvent.class);
        } catch (Exception parseFailure) {
            // A malformed record must not stall the partition forever.
            failed.increment();
            log.error("SPORTS_EVENT_UNPARSEABLE payloadLength={}", payload == null ? 0 : payload.length(),
                    parseFailure);
            return;
        }

        if (projection.apply(event)) {
            processed.increment();
        } else {
            ignored.increment();
        }
    }
}
