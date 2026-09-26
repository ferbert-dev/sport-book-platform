package com.example.sportsbook.feed.messaging;

import com.example.sportsbook.common.SportsEvent;
import com.example.sportsbook.common.SportsbookJson;
import io.reactivex.rxjava3.core.Completable;
import io.vertx.rxjava3.kafka.client.producer.KafkaProducerRecord;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.kafka.client.producer.KafkaProducer;

import java.util.HashMap;
import java.util.Map;

/**
 * Publishes normalized events to {@code sports-events}, keyed by {@link SportsEvent#partitionKey()}
 * so ordering is preserved per market (or per match for match-level events).
 */
public class SportsEventPublisher {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public SportsEventPublisher(Vertx vertx, String bootstrapServers, String topic) {
        this.topic = topic;
        this.producer = KafkaProducer.create(vertx, producerConfig(bootstrapServers));
    }

    private static Map<String, String> producerConfig(String bootstrapServers) {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", bootstrapServers);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // Durability over latency: the feed is the system's source of truth.
        config.put("acks", "all");
        config.put("enable.idempotence", "true");
        config.put("max.in.flight.requests.per.connection", "5");
        return config;
    }

    public Completable publish(SportsEvent event) {
        return Completable.defer(() -> {
            String payload = SportsbookJson.mapper().writeValueAsString(event);
            KafkaProducerRecord<String, String> record =
                    KafkaProducerRecord.create(topic, event.partitionKey(), payload);
            return producer.rxSend(record).ignoreElement();
        });
    }

    public Completable close() {
        return producer.rxClose();
    }
}
