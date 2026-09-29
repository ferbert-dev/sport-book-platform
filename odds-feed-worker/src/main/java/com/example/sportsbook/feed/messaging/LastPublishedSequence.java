package com.example.sportsbook.feed.messaging;

import com.example.sportsbook.common.SportsbookJson;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Where a freshly started worker should resume the provider stream: the highest {@code version}
 * already on {@code sports-events}.
 *
 * <p>The provider's sequence number becomes the domain version, and the provider numbers the whole
 * stream with one increasing sequence, so the newest record of each partition carries the highest
 * version that partition holds, and the maximum over partitions is the last sequence Kafka has
 * acknowledged. Deriving the cursor from Kafka rather than a local file means it can never claim
 * more than was actually delivered.
 *
 * <p><b>Blocking</b> — plain Kafka consumer calls. Run it through {@code executeBlocking}, never on
 * the event loop. It reads only the last few records per partition and joins no consumer group.
 */
public final class LastPublishedSequence {

    private static final Logger log = LoggerFactory.getLogger(LastPublishedSequence.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    /** How far back to look for a readable record when the newest one is malformed. */
    private static final int LOOKBACK = 10;

    private LastPublishedSequence() {
    }

    /**
     * @return the highest version on the topic, or -1 if the topic is empty
     * @throws RuntimeException if Kafka cannot be read. Deliberately not folded into -1: "empty"
     *                          makes the worker ask for a full replay, while "unknown" must be
     *                          retried, never guessed
     */
    public static long read(String bootstrapServers, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("enable.auto.commit", "false");
        props.put("max.poll.records", Integer.toString(LOOKBACK));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic, TIMEOUT);
            if (partitions == null || partitions.isEmpty()) {
                return -1;
            }
            List<TopicPartition> assigned = partitions.stream()
                    .map(info -> new TopicPartition(topic, info.partition()))
                    .toList();
            consumer.assign(assigned);
            Map<TopicPartition, Long> ends = consumer.endOffsets(assigned, TIMEOUT);

            long highest = -1;
            for (TopicPartition partition : assigned) {
                long end = ends.getOrDefault(partition, 0L);
                if (end > 0) {
                    highest = Math.max(highest, lastVersion(consumer, partition, end - 1));
                }
            }
            log.debug("LAST_PUBLISHED_SEQUENCE_READ topic={} highest={}", topic, highest);
            return highest;
        }
    }

    /**
     * Highest version among the last {@link #LOOKBACK} records of a non-empty partition. Looking back
     * rather than trusting only the newest record means one malformed record cannot block startup
     * forever; but if the newest record cannot be fetched at all, or none of the window parses, the
     * partition's state is unknown and this throws so startup retries instead of guessing.
     */
    private static long lastVersion(KafkaConsumer<String, String> consumer, TopicPartition partition, long lastOffset) {
        consumer.seek(partition, Math.max(0, lastOffset - LOOKBACK + 1));
        long highest = -1;
        boolean reachedLast = false;
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (!reachedLast && System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500)).records(partition)) {
                highest = Math.max(highest, version(record.value()));
                reachedLast |= record.offset() >= lastOffset;
            }
        }
        if (!reachedLast) {
            throw new IllegalStateException("could not fetch offset " + lastOffset + " of " + partition);
        }
        if (highest < 0) {
            throw new IllegalStateException("no readable version in the last " + LOOKBACK + " records of " + partition);
        }
        return highest;
    }

    private static long version(String payload) {
        try {
            JsonNode version = SportsbookJson.mapper().readTree(payload).get("version");
            return version == null ? -1 : version.asLong(-1);
        } catch (Exception malformed) {
            log.warn("LAST_PUBLISHED_RECORD_UNREADABLE length={}", payload == null ? 0 : payload.length());
            return -1;
        }
    }
}
