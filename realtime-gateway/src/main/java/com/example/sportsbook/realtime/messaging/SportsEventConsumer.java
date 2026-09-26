package com.example.sportsbook.realtime.messaging;

import io.vertx.core.Handler;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.kafka.client.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Consumes {@code sports-events} and hands each raw payload to the fan-out handler.
 *
 * <p>The gateway is a pure pass-through: it forwards the provider's JSON unchanged rather than
 * reserializing it, so clients see exactly what was published.
 */
public class SportsEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(SportsEventConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public SportsEventConsumer(Vertx vertx, String bootstrapServers, String groupId, String topic) {
        this.topic = topic;
        this.consumer = KafkaConsumer.create(vertx, consumerConfig(bootstrapServers, groupId));
    }

    private static Map<String, String> consumerConfig(String bootstrapServers, String groupId) {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", bootstrapServers);
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("group.id", groupId);
        // Live updates only: a reconnecting gateway should not replay history to clients,
        // because clients get their starting state from the REST snapshot instead.
        config.put("auto.offset.reset", "latest");
        config.put("enable.auto.commit", "true");
        return config;
    }

    public void start(Handler<String> payloadHandler) {
        consumer.handler(record -> payloadHandler.handle(record.value()));
        consumer.exceptionHandler(error -> log.error("KAFKA_CONSUMER_ERROR topic={}", topic, error));
        consumer.subscribe(Set.of(topic))
                .subscribe(
                        () -> log.info("KAFKA_SUBSCRIBED topic={}", topic),
                        error -> log.error("KAFKA_SUBSCRIBE_FAILED topic={}", topic, error));
    }

    public io.reactivex.rxjava3.core.Completable close() {
        return consumer.rxClose();
    }
}
