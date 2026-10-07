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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @BeforeEach
    void seedBettableMarket() {
        seedBettableMarket(EVENT_ID, MARKET_ID);
    }

    /** What Redis looks like for a market that takes bets: event LIVE, market ACTIVE and fresh. */
    private void seedBettableMarket(String eventId, String marketId) {
        redis.opsForHash().putAll(RedisKeys.event(eventId), Map.of(
                RedisKeys.FIELD_STATUS, EventStatus.LIVE.name(),
                RedisKeys.FIELD_VERSION, "1000",
                RedisKeys.FIELD_LAST_UPDATED_AT, Instant.now().toString()));
        redis.opsForHash().putAll(RedisKeys.market(marketId), Map.of(
                RedisKeys.FIELD_EVENT_ID, eventId,
                RedisKeys.FIELD_STATUS, MarketStatus.ACTIVE.name(),
                RedisKeys.FIELD_VERSION, "1001",
                RedisKeys.FIELD_LAST_UPDATED_AT, Instant.now().toString()));
        redis.opsForHash().put(RedisKeys.marketOdds(marketId), SELECTION, "2.10");
    }

    private PlaceBetRequest request(String idempotencyKey) {
        return request(EVENT_ID, MARKET_ID, idempotencyKey);
    }

    private PlaceBetRequest request(String eventId, String marketId, String idempotencyKey) {
        return new PlaceBetRequest("user-42", eventId, marketId, SELECTION,
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

    private PlaceBetRequest requestWithStake(String stake) {
        return new PlaceBetRequest("user-42", EVENT_ID, MARKET_ID, SELECTION,
                new BigDecimal(stake), new BigDecimal("2.10"), "it-" + UUID.randomUUID());
    }

    @Test
    void stakeAboveTheBusinessLimitIsRejectedAsABusinessRule() {
        long before = betRepository.count();

        ResponseEntity<PlaceBetResponse> response = restTemplate.postForEntity("/api/v1/bets",
                requestWithStake("10000.01"), PlaceBetResponse.class);

        // Well-formed request, refused by a rule: 422, like a suspended market.
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo(BetOutcome.STAKE_LIMIT_EXCEEDED);
        assertThat(betRepository.count()).isEqualTo(before);
    }

    @Test
    void stakeBeyondTheTechnicalCeilingIsAMalformedRequest() {
        long before = betRepository.count();

        // Nine integer digits: more than the request allows, whatever the configured limit is.
        ResponseEntity<PlaceBetResponse> response = restTemplate.postForEntity("/api/v1/bets",
                requestWithStake("123456789.00"), PlaceBetResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo(BetOutcome.INVALID_REQUEST);
        assertThat(betRepository.count()).isEqualTo(before);
    }

    /**
     * A bet names an event and a market. Each is checked on its own (the event is LIVE, the market
     * is ACTIVE), but nothing checks that the market belongs to that event. Settlement selects bets
     * by event id AND market id, so a bet stored with event A and a market of event B matches no
     * settlement and stays OPEN forever, even when its selection wins.
     */
    @Test
    void betThatNamesOneEventAndAnotherEventsMarketIsRejected() {
        String otherEvent = "event-it-other-" + UUID.randomUUID();
        String otherEventsMarket = "market-it-other-" + UUID.randomUUID();
        seedBettableMarket(otherEvent, otherEventsMarket);

        // EVENT_ID is live and has its own market; the request pairs it with the other event's market.
        ResponseEntity<PlaceBetResponse> response = restTemplate.postForEntity("/api/v1/bets",
                request(EVENT_ID, otherEventsMarket, "it-" + UUID.randomUUID()), PlaceBetResponse.class);

        // 400, not 422: this pair of ids can never be accepted, so there is nothing to retry.
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status())
                .as("the market does not belong to the event the bet names")
                .isEqualTo(BetOutcome.INVALID_REQUEST);
        assertThat(openBetsOn(otherEventsMarket)).isZero();
    }

    @Test
    void unknownSelectionIsABadRequest() {
        ResponseEntity<PlaceBetResponse> response = restTemplate.postForEntity("/api/v1/bets",
                new PlaceBetRequest("user-42", EVENT_ID, MARKET_ID, "no-such-selection",
                        new BigDecimal("100.00"), new BigDecimal("2.10"), "it-" + UUID.randomUUID()),
                PlaceBetResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo(BetOutcome.INVALID_REQUEST);
    }

    // ------------------------------------------------------------------------------------------
    // Late bets: a bet must never end up OPEN on a market settlement has already processed.
    //
    // settlement-service selects a market's OPEN bets exactly once, and its fence row in
    // processed_settlements makes a redelivered MARKET_SETTLED a no-op. So a bet that commits
    // after that point is never settled: nothing will ever look at it again.
    //
    // Both tests use their own event and market, because they leave a settlement behind.
    // ------------------------------------------------------------------------------------------

    /**
     * Race 1, Redis lag. settlement-service consumes MARKET_SETTLED straight from Kafka;
     * bet-service validates against Redis, which state-processor updates on its own schedule.
     * Between the two, settlement is finished while Redis still says LIVE / ACTIVE.
     */
    @Test
    void betOnAMarketSettlementHasAlreadyProcessedIsRejectedEvenWhileRedisStillSaysActive() {
        String eventId = "event-it-late-" + UUID.randomUUID();
        String marketId = "market-it-late-" + UUID.randomUUID();
        seedBettableMarket(eventId, marketId);
        // settlement-service is done with this market: its fence row is committed.
        jdbc.update("INSERT INTO processed_settlements (id, market_id, settlement_version, processed_at) "
                + "VALUES (?, ?, ?, now())", UUID.randomUUID(), marketId, 2000L);

        PlaceBetResponse response = restTemplate.postForEntity("/api/v1/bets",
                request(eventId, marketId, "it-" + UUID.randomUUID()), PlaceBetResponse.class).getBody();

        assertThat(response).isNotNull();
        assertThat(response.status())
                .as("a bet on a market that is already settled must not be accepted")
                .isEqualTo(BetOutcome.MARKET_SETTLED);
        assertThat(openBetsOn(marketId))
                .as("OPEN bets nobody will ever settle")
                .isZero();
    }

    /**
     * Race 2, interleaved transactions, with Redis perfectly up to date at the moment the bet reads
     * it. Settlement has selected the OPEN bets but not committed yet; the bet commits in that gap.
     *
     * <p>The settlement side is a stand-in that runs the same statements as
     * {@code SettlementService.settle}, in the same order, in one transaction; the latches hold it
     * open between its SELECT and its commit so the interleaving is deterministic.
     *
     * <p>With the per-market lock the bet waits for the settlement transaction, then finds its fence
     * row and is rejected. The bet is given a moment before settlement is released, so the test also
     * shows the pre-fix behaviour (the bet committing at once) instead of hanging on it.
     */
    @Test
    void betCommittedWhileSettlementIsInFlightIsNotLeftOpenOnASettledMarket() throws Exception {
        String eventId = "event-it-race-" + UUID.randomUUID();
        String marketId = "market-it-race-" + UUID.randomUUID();
        seedBettableMarket(eventId, marketId);

        CountDownLatch settlementHasSelected = new CountDownLatch(1);
        CountDownLatch betHasBeenAttempted = new CountDownLatch(1);
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Integer> settlement = pool.submit(() -> transaction.execute(status -> {
                // Same first statement as SettlementService.settle.
                jdbc.queryForObject("SELECT 1 FROM pg_advisory_xact_lock(hashtextextended(?, 0))",
                        Integer.class, marketId);
                jdbc.update("INSERT INTO processed_settlements (id, market_id, settlement_version, processed_at) "
                        + "VALUES (?, ?, ?, now())", UUID.randomUUID(), marketId, 2000L);
                Integer openBets = jdbc.queryForObject("SELECT count(*) FROM bet "
                        + "WHERE event_id = ? AND market_id = ? AND status = 'OPEN'", Integer.class, eventId, marketId);
                settlementHasSelected.countDown();
                awaitQuietly(betHasBeenAttempted);
                return openBets;
            }));
            assertThat(settlementHasSelected.await(10, TimeUnit.SECONDS)).isTrue();

            Future<PlaceBetResponse> bet = pool.submit(() -> restTemplate.postForEntity("/api/v1/bets",
                    request(eventId, marketId, "it-" + UUID.randomUUID()), PlaceBetResponse.class).getBody());
            try {
                bet.get(2, TimeUnit.SECONDS);
            } catch (TimeoutException waitingForSettlement) {
                // Fine: the bet is blocked behind the settlement transaction.
            }
            betHasBeenAttempted.countDown();

            assertThat(settlement.get(10, TimeUnit.SECONDS))
                    .as("settlement selected its OPEN bets before the bet existed")
                    .isZero();
            PlaceBetResponse response = bet.get(10, TimeUnit.SECONDS);

            assertThat(response).isNotNull();
            assertThat(response.status()).isEqualTo(BetOutcome.MARKET_SETTLED);
            assertThat(openBetsOn(marketId))
                    .as("OPEN bets on a settled market: settlement never saw them and never will")
                    .isZero();
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * settlement-service does not send to Kafka: it inserts BET_SETTLED / PAYOUT_REQUIRED rows into
     * this service's outbox table, in its own settlement transaction. This relay is what delivers
     * them, on the same key and after the bet's own BET_PLACED.
     *
     * <p>The row below is stamped by the database clock, the bet's own row by this JVM's: two
     * clocks, as with two services. Ordering the relay by created_at sent BET_SETTLED first here;
     * that is why it orders by the database-assigned seq.
     */
    @Test
    void rowsQueuedBySettlementAreRelayedOnTheBetsKeyAfterItsBetPlaced() {
        PlaceBetResponse placed = restTemplate.postForEntity("/api/v1/bets",
                request("it-" + UUID.randomUUID()), PlaceBetResponse.class).getBody();
        assertThat(placed).isNotNull();
        String betId = placed.betId();

        // What SettlementService.enqueue writes.
        jdbc.update("INSERT INTO outbox_event (id, aggregate_type, aggregate_id, event_type, payload, created_at) "
                        + "VALUES (?, 'BET', ?, 'BET_SETTLED', ?, now())", UUID.randomUUID(), betId,
                "{\"type\":\"BET_SETTLED\",\"betId\":\"" + betId + "\",\"status\":\"WON\"}");

        List<String> onKafka = awaitBetEvents(betId, 2);

        assertThat(onKafka).hasSize(2);
        assertThat(onKafka.get(0)).contains("\"type\":\"BET_PLACED\"");
        assertThat(onKafka.get(1)).contains("\"type\":\"BET_SETTLED\"");
    }

    /**
     * Two requests with the same idempotency key at the same time. Both pass the step-1 lookup
     * (neither row is committed yet); the unique constraint lets one insert win. The loser must get
     * the winner's bet back, exactly as a sequential retry does.
     *
     * <p>The winner is a stand-in transaction that inserts the bet row and stays open, so the real
     * request deterministically blocks on the unique index and loses when the winner commits.
     */
    @Test
    void concurrentRetryThatLosesTheInsertRaceGetsTheWinnersBetBack() throws Exception {
        String idempotencyKey = "it-" + UUID.randomUUID();
        UUID winnerId = UUID.randomUUID();
        CountDownLatch winnerHasInserted = new CountDownLatch(1);
        CountDownLatch loserHasBeenAttempted = new CountDownLatch(1);
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> winner = pool.submit(() -> transaction.executeWithoutResult(status -> {
                jdbc.update("INSERT INTO bet (id, user_id, event_id, market_id, selection_id, stake, "
                                + "accepted_odds, status, idempotency_key, created_at) "
                                + "VALUES (?, 'user-42', ?, ?, ?, 100.00, 2.10, 'OPEN', ?, now())",
                        winnerId, EVENT_ID, MARKET_ID, SELECTION, idempotencyKey);
                winnerHasInserted.countDown();
                awaitQuietly(loserHasBeenAttempted);
            }));
            assertThat(winnerHasInserted.await(10, TimeUnit.SECONDS)).isTrue();

            Future<ResponseEntity<PlaceBetResponse>> loser = pool.submit(() -> restTemplate.postForEntity(
                    "/api/v1/bets", request(idempotencyKey), PlaceBetResponse.class));
            try {
                loser.get(2, TimeUnit.SECONDS);
            } catch (TimeoutException blockedOnTheUniqueIndex) {
                // Expected: the insert waits for the winner's transaction to end.
            }
            loserHasBeenAttempted.countDown();
            winner.get(10, TimeUnit.SECONDS);
            ResponseEntity<PlaceBetResponse> response = loser.get(10, TimeUnit.SECONDS);

            assertThat(response.getStatusCode())
                    .as("the losing retry is a duplicate, not a server error")
                    .isEqualTo(HttpStatus.CREATED);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().betId()).isEqualTo(winnerId.toString());
            assertThat(jdbc.queryForObject("SELECT count(*) FROM bet WHERE user_id = 'user-42' "
                    + "AND idempotency_key = ?", Long.class, idempotencyKey)).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
    }

    private long openBetsOn(String marketId) {
        Long open = jdbc.queryForObject(
                "SELECT count(*) FROM bet WHERE market_id = ? AND status = 'OPEN'", Long.class, marketId);
        return open == null ? 0 : open;
    }

    private void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Reads bet-events from the beginning looking for this bet's BET_PLACED payload. */
    private String awaitBetEvent(String betId) {
        return awaitBetEvents(betId, 1).getFirst();
    }

    /** This bet's payloads on bet-events, in partition order, once at least {@code expected} arrived. */
    private List<String> awaitBetEvents(String betId, int expected) {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        props.put("group.id", "it-verifier-" + UUID.randomUUID());
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        List<String> payloads = new java.util.ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("bet-events"));
            Instant deadline = Instant.now().plus(Duration.ofSeconds(45));
            while (Instant.now().isBefore(deadline) && payloads.size() < expected) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (betId.equals(record.key())) {
                        payloads.add(record.value());
                    }
                }
            }
        }
        if (payloads.size() < expected) {
            throw new AssertionError("Expected " + expected + " event(s) on bet-events for bet " + betId
                    + ", got " + payloads);
        }
        return payloads;
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
