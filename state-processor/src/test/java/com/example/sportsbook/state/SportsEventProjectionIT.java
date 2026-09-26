package com.example.sportsbook.state;

import com.example.sportsbook.common.MarketSuspendedEvent;
import com.example.sportsbook.common.MarketStatus;
import com.example.sportsbook.common.OddsUpdatedEvent;
import com.example.sportsbook.common.RedisKeys;
import com.example.sportsbook.common.SportsEvent;
import com.example.sportsbook.common.SportsbookJson;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the real pipeline: Kafka -> State Processor -> Redis.
 *
 * <p>{@code disabledWithoutDocker} keeps {@code ./mvnw clean verify} green on machines with no
 * Docker daemon instead of failing the build.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers(disabledWithoutDocker = true)
class SportsEventProjectionIT {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:4.0.0"));

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private StringRedisTemplate redis;

    @Test
    void publishingOddsUpdatedProjectsThePriceIntoRedis() {
        String marketId = "market-" + System.nanoTime();

        publish(new OddsUpdatedEvent("event-it-1", marketId, "real-madrid",
                new BigDecimal("2.10"), 1001, Instant.now()));

        Object odds = await(() -> redis.opsForHash().get(RedisKeys.marketOdds(marketId), "real-madrid"));
        assertThat(odds).isNotNull();
        assertThat(new BigDecimal(odds.toString())).isEqualByComparingTo("2.10");
    }

    @Test
    void newerVersionOverwritesAndOlderVersionIsIgnored() {
        String marketId = "market-" + System.nanoTime();

        publish(new OddsUpdatedEvent("event-it-2", marketId, "real-madrid",
                new BigDecimal("2.10"), 1001, Instant.now()));
        await(() -> redis.opsForHash().get(RedisKeys.marketOdds(marketId), "real-madrid"));

        publish(new OddsUpdatedEvent("event-it-2", marketId, "real-madrid",
                new BigDecimal("1.95"), 1002, Instant.now()));
        awaitValue(() -> {
            Object value = redis.opsForHash().get(RedisKeys.marketOdds(marketId), "real-madrid");
            return value != null && "1.95".equals(value.toString()) ? value : null;
        });

        // Late-arriving older version must not rewind the projection.
        publish(new OddsUpdatedEvent("event-it-2", marketId, "real-madrid",
                new BigDecimal("9.99"), 1000, Instant.now()));

        // Give the consumer time to process-and-ignore before asserting.
        sleep(1500);
        assertThat(redis.opsForHash().get(RedisKeys.marketOdds(marketId), "real-madrid"))
                .hasToString("1.95");
    }

    @Test
    void marketSuspendedProjectsTheStatusChange() {
        String marketId = "market-" + System.nanoTime();

        publish(new MarketSuspendedEvent("event-it-3", marketId, 2001, Instant.now()));

        Object status = await(() -> redis.opsForHash().get(RedisKeys.market(marketId), RedisKeys.FIELD_STATUS));
        assertThat(status).hasToString(MarketStatus.SUSPENDED.name());
    }

    private void publish(SportsEvent event) {
        try {
            kafkaTemplate.send("sports-events", event.partitionKey(),
                    SportsbookJson.mapper().writeValueAsString(event)).get();
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private Object await(Supplier<Object> read) {
        return awaitValue(read);
    }

    /** Polls until the supplier returns non-null or the deadline passes. */
    private Object awaitValue(Supplier<Object> read) {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        Object value = null;
        while (Instant.now().isBefore(deadline)) {
            value = read.get();
            if (value != null) {
                return value;
            }
            sleep(250);
        }
        return value;
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
