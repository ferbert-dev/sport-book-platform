package com.example.sportsbook.bet;

import com.example.sportsbook.bet.domain.OutboxEvent;
import com.example.sportsbook.bet.dto.BetOutcome;
import com.example.sportsbook.bet.dto.PlaceBetRequest;
import com.example.sportsbook.bet.dto.PlaceBetResponse;
import com.example.sportsbook.bet.repository.BetRepository;
import com.example.sportsbook.bet.repository.OutboxRepository;
import com.example.sportsbook.common.BetStatus;
import com.example.sportsbook.common.EventStatus;
import com.example.sportsbook.common.MarketStatus;
import com.example.sportsbook.common.RedisKeys;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the full acceptance chain: HTTP -> Redis validation -> Postgres bet + outbox row
 * -> outbox publisher -> Kafka bet-events.
 *
 * <p>Also exercises the real Flyway migration, since the schema is created by it here.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
class PlaceBetIT {

    private static final String EVENT_ID = "event-it-bet";
    private static final String MARKET_ID = "market-it-bet";
    private static final String SELECTION = "real-madrid";

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("sportsbook")
                    .withUsername("sportsbook")
                    .withPassword("sportsbook");

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:4.0.0"));

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private BetRepository betRepository;

    @Autowired
    private OutboxRepository outboxRepository;

    @BeforeEach
    void seedBettableMarket() {
        redis.opsForHash().putAll(RedisKeys.event(EVENT_ID), Map.of(
                RedisKeys.FIELD_STATUS, EventStatus.LIVE.name(),
                RedisKeys.FIELD_VERSION, "1000",
                RedisKeys.FIELD_LAST_UPDATED_AT, Instant.now().toString()));
        redis.opsForHash().putAll(RedisKeys.market(MARKET_ID), Map.of(
                RedisKeys.FIELD_EVENT_ID, EVENT_ID,
                RedisKeys.FIELD_STATUS, MarketStatus.ACTIVE.name(),
                RedisKeys.FIELD_VERSION, "1001",
                RedisKeys.FIELD_LAST_UPDATED_AT, Instant.now().toString()));
        redis.opsForHash().put(RedisKeys.marketOdds(MARKET_ID), SELECTION, "2.10");
    }

    private PlaceBetRequest request(String idempotencyKey) {
        return new PlaceBetRequest("user-42", EVENT_ID, MARKET_ID, SELECTION,
                new BigDecimal("100.00"), new BigDecimal("2.10"), idempotencyKey);
    }

    @Test
    void acceptedBetIsPersistedWithAnOutboxRowAndReachesKafka() {
        String idempotencyKey = "it-" + UUID.randomUUID();

        ResponseEntity<PlaceBetResponse> response =
                restTemplate.postForEntity("/api/v1/bets", request(idempotencyKey), PlaceBetResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        PlaceBetResponse body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.status()).isEqualTo(BetOutcome.ACCEPTED);
        assertThat(body.acceptedOdds()).isEqualByComparingTo("2.10");

        // Durable bet row.
        UUID betId = UUID.fromString(body.betId());
        var stored = betRepository.findById(betId).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(BetStatus.OPEN);
        assertThat(stored.getStake()).isEqualByComparingTo("100.00");
        assertThat(stored.getIdempotencyKey()).isEqualTo(idempotencyKey);

        // Outbox row written in the same transaction, then drained by the publisher.
        List<OutboxEvent> outboxRows = outboxRepository.findAll().stream()
                .filter(row -> row.getAggregateId().equals(betId.toString()))
                .toList();
        assertThat(outboxRows).hasSize(1);
        assertThat(outboxRows.getFirst().getEventType()).isEqualTo("BET_PLACED");

        // BET_PLACED lands on bet-events, keyed by bet id.
        String payload = awaitBetEvent(betId.toString());
        assertThat(payload)
                .contains("\"type\":\"BET_PLACED\"")
                .contains("\"betId\":\"" + betId + "\"")
                .contains("\"odds\":2.10");
    }

    @Test
    void outboxRowIsMarkedPublishedOnceTheBrokerHasAcked() {
        String idempotencyKey = "it-" + UUID.randomUUID();

        PlaceBetResponse body = restTemplate
                .postForEntity("/api/v1/bets", request(idempotencyKey), PlaceBetResponse.class)
                .getBody();
        assertThat(body).isNotNull();

        Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        OutboxEvent row = null;
        while (Instant.now().isBefore(deadline)) {
            row = outboxRepository.findAll().stream()
                    .filter(candidate -> candidate.getAggregateId().equals(body.betId()))
                    .findFirst().orElse(null);
            if (row != null && row.getPublishedAt() != null) {
                break;
            }
            sleep(500);
        }
        assertThat(row).isNotNull();
        assertThat(row.getPublishedAt()).as("outbox row should be marked published").isNotNull();
    }

    @Test
    void retryWithTheSameIdempotencyKeyReturnsTheSameBetAndCreatesNoSecondRow() {
        String idempotencyKey = "it-" + UUID.randomUUID();

        PlaceBetResponse first = restTemplate
                .postForEntity("/api/v1/bets", request(idempotencyKey), PlaceBetResponse.class).getBody();
        PlaceBetResponse second = restTemplate
                .postForEntity("/api/v1/bets", request(idempotencyKey), PlaceBetResponse.class).getBody();

        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(second.betId()).isEqualTo(first.betId());

        long rows = betRepository.findAll().stream()
                .filter(bet -> bet.getIdempotencyKey().equals(idempotencyKey))
                .count();
        assertThat(rows).isEqualTo(1);
    }

    @Test
    void suspendedMarketIsRejectedWithUnprocessableEntityAndNothingIsPersisted() {
        redis.opsForHash().put(RedisKeys.market(MARKET_ID), RedisKeys.FIELD_STATUS,
                MarketStatus.SUSPENDED.name());
        String idempotencyKey = "it-" + UUID.randomUUID();
        long before = betRepository.count();

        ResponseEntity<PlaceBetResponse> response =
                restTemplate.postForEntity("/api/v1/bets", request(idempotencyKey), PlaceBetResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo(BetOutcome.MARKET_SUSPENDED);
        assertThat(betRepository.count()).isEqualTo(before);
    }

    /** Reads bet-events from the beginning looking for this bet's BET_PLACED payload. */
    private String awaitBetEvent(String betId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        props.put("group.id", "it-verifier-" + UUID.randomUUID());
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("bet-events"));
            Instant deadline = Instant.now().plus(Duration.ofSeconds(45));
            while (Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (betId.equals(record.key())) {
                        return record.value();
                    }
                }
            }
        }
        throw new AssertionError("No BET_PLACED on bet-events for bet " + betId);
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
