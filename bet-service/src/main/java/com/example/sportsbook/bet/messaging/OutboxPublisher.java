package com.example.sportsbook.bet.messaging;

import com.example.sportsbook.bet.domain.OutboxEvent;
import com.example.sportsbook.bet.repository.OutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Drains the outbox to {@code bet-events}.
 *
 * <p>Delivery is deliberately at-least-once: a row can be published and then fail to be marked, so
 * the same event is re-sent on the next poll. Consumers must therefore be idempotent. This is NOT
 * exactly-once, and nothing in the system claims it is.
 *
 * <p>Events are keyed by {@code aggregateId} (the bet id) so all events for one bet stay ordered.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int BATCH_SIZE = 100;

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String betEventsTopic;
    private final Counter published;
    private final Counter failures;

    public OutboxPublisher(OutboxRepository outboxRepository,
                           KafkaTemplate<String, String> kafkaTemplate,
                           @Value("${sportsbook.topics.bet-events}") String betEventsTopic,
                           MeterRegistry meterRegistry) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.betEventsTopic = betEventsTopic;
        this.published = Counter.builder("outbox_events_published_total").register(meterRegistry);
        this.failures = Counter.builder("outbox_publish_failures_total").register(meterRegistry);
    }

    @Scheduled(fixedDelayString = "${sportsbook.bet.outbox-poll-interval:1000}")
    @Transactional
    public void publishPending() {
        List<OutboxEvent> pending =
                outboxRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(Limit.of(BATCH_SIZE));
        if (pending.isEmpty()) {
            return;
        }
        for (OutboxEvent event : pending) {
            if (!publish(event)) {
                // Stop on first failure so ordering is preserved; the next poll retries from here.
                break;
            }
        }
    }

    private boolean publish(OutboxEvent event) {
        try {
            // Synchronous get(): the row must only be marked published once the broker has acked.
            kafkaTemplate.send(betEventsTopic, event.getAggregateId(), event.getPayload()).get();
            event.markPublished();
            outboxRepository.save(event);
            published.increment();
            log.info("OUTBOX_EVENT_PUBLISHED eventType={} aggregateId={} outboxId={}",
                    event.getEventType(), event.getAggregateId(), event.getId());
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            log.warn("OUTBOX_PUBLISH_INTERRUPTED outboxId={}", event.getId());
            return false;
        } catch (Exception publishFailure) {
            failures.increment();
            log.error("OUTBOX_PUBLISH_FAILED outboxId={} eventType={}",
                    event.getId(), event.getEventType(), publishFailure);
            return false;
        }
    }
}
