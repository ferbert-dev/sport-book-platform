package com.example.sportsbook.settlement;

import com.example.sportsbook.common.MarketSettledEvent;
import com.example.sportsbook.settlement.service.SettlementService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SettlementService against real PostgreSQL: the transaction boundaries and the per-market lock are
 * exactly the things mocks cannot show.
 *
 * <p>settlement-service never migrates the schema (bet-service owns it), so the database is created
 * from bet-service's own migration files: the column types here are the production ones.
 *
 * <p>The Kafka listener is not started. Settlement no longer sends anything itself; it only writes
 * to the outbox, and that is what these tests look at.
 */
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
@Testcontainers(disabledWithoutDocker = true)
class SettlementIT {

    private static final String WINNER = "real-madrid";
    private static final long SETTLEMENT_VERSION = 2000L;
    // The whole directory: Postgres runs the files of its init directory in name order.
    private static final Path BET_SERVICE_MIGRATIONS = Path.of("..", "bet-service", "src", "main",
            "resources", "db", "migration").toAbsolutePath().normalize();

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("sportsbook")
                    .withUsername("sportsbook")
                    .withPassword("sportsbook")
                    .withCopyFileToContainer(MountableFile.forHostPath(BET_SERVICE_MIGRATIONS),
                            "/docker-entrypoint-initdb.d/");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    private SettlementService settlementService;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private PlatformTransactionManager transactionManager;

    // Every test uses its own event and market, so they cannot see each other's rows.
    private final String eventId = "event-it-" + UUID.randomUUID();
    private final String marketId = "market-it-" + UUID.randomUUID();

    private MarketSettledEvent settled() {
        return new MarketSettledEvent(eventId, marketId, WINNER, SETTLEMENT_VERSION, Instant.now());
    }

    private UUID openBet(String selectionId, String stake, String odds) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO bet (id, user_id, event_id, market_id, selection_id, stake, accepted_odds, "
                        + "status, idempotency_key, created_at) VALUES (?, 'user-42', ?, ?, ?, ?::numeric, ?::numeric, "
                        + "'OPEN', ?, now())",
                id, eventId, marketId, selectionId, stake, odds, "it-" + id);
        return id;
    }

    private String statusOf(UUID betId) {
        return jdbc.queryForObject("SELECT status FROM bet WHERE id = ?", String.class, betId);
    }

    private List<String> queuedEventTypesFor(UUID betId) {
        return jdbc.queryForList("SELECT event_type FROM outbox_event WHERE aggregate_id = ? ORDER BY event_type",
                String.class, betId.toString());
    }

    private long fenceRows() {
        Long rows = jdbc.queryForObject(
                "SELECT count(*) FROM processed_settlements WHERE market_id = ?", Long.class, marketId);
        return rows == null ? 0 : rows;
    }

    @Test
    void settlementWritesTheBetsTheFenceAndTheOutboxRowsInOneTransaction() {
        UUID winning = openBet(WINNER, "100.00", "2.10");
        UUID losing = openBet("draw", "100.00", "3.40");

        boolean performed = settlementService.settle(settled());

        assertThat(performed).isTrue();
        assertThat(statusOf(winning)).isEqualTo("WON");
        assertThat(statusOf(losing)).isEqualTo("LOST");
        assertThat(fenceRows()).isEqualTo(1);
        // Queued, not sent: the relay in bet-service picks these up.
        assertThat(queuedEventTypesFor(winning)).containsExactly("BET_SETTLED", "PAYOUT_REQUIRED");
        assertThat(queuedEventTypesFor(losing)).containsExactly("BET_SETTLED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id = ? "
                + "AND published_at IS NULL", Long.class, winning.toString())).isEqualTo(2);
        String payout = jdbc.queryForObject("SELECT payload FROM outbox_event WHERE aggregate_id = ? "
                + "AND event_type = 'PAYOUT_REQUIRED'", String.class, winning.toString());
        assertThat(payout)
                .contains("\"type\":\"PAYOUT_REQUIRED\"")
                .contains("\"paymentId\":\"" + winning + "-settlement-" + SETTLEMENT_VERSION + "\"");
    }

    /**
     * The dual-write scenario, for real. This stake is valid (15 integer digits fit NUMERIC(19,4)),
     * but at odds 2.00 its payout has 16 and does not. Postgres only reports that when the bet row
     * is written, after every event of the settlement has been produced.
     *
     * <p>Before the outbox those events were already in Kafka at that point: a payout for a bet that
     * is still OPEN. Now they are rows of the same transaction, and they are gone with it.
     */
    @Test
    void aSettlementThatFailsInTheDatabaseLeavesNoQueuedEventBehind() {
        UUID ordinary = openBet(WINNER, "100.00", "2.10");
        UUID tooLargeToPay = openBet(WINNER, "600000000000000.0000", "2.0000");

        // The type matters: SettlementConfig's error handler does not retry this one, because it
        // would fail the same way forever and block the partition.
        assertThatThrownBy(() -> settlementService.settle(settled()))
                .isInstanceOf(DataIntegrityViolationException.class)
                .rootCause().hasMessageContaining("numeric field overflow");

        assertThat(statusOf(ordinary)).isEqualTo("OPEN");
        assertThat(statusOf(tooLargeToPay)).isEqualTo("OPEN");
        assertThat(queuedEventTypesFor(ordinary)).isEmpty();
        assertThat(queuedEventTypesFor(tooLargeToPay)).isEmpty();
        // The fence went too, so a redelivered MARKET_SETTLED is a real retry, not a skipped duplicate.
        assertThat(fenceRows()).isZero();
    }

    @Test
    void aRedeliveredSettlementQueuesNothingASecondTime() {
        UUID winning = openBet(WINNER, "100.00", "2.10");

        assertThat(settlementService.settle(settled())).isTrue();
        assertThat(settlementService.settle(settled())).isFalse();

        assertThat(queuedEventTypesFor(winning)).containsExactly("BET_SETTLED", "PAYOUT_REQUIRED");
    }

    /**
     * The other half of the late-bet fix (PlaceBetIT covers a bet waiting for a settlement): a bet
     * that is still in flight holds the market's shared lock, so the settlement waits for it and
     * then selects it. The bet side is a stand-in running BetService's statements in one transaction.
     */
    @Test
    void settlementWaitsForABetThatIsStillInFlightAndThenSettlesIt() throws Exception {
        UUID betId = UUID.randomUUID();
        CountDownLatch betIsInFlight = new CountDownLatch(1);
        CountDownLatch settlementHasBeenAttempted = new CountDownLatch(1);
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> bet = pool.submit(() -> transaction.executeWithoutResult(status -> {
                jdbc.queryForObject("SELECT 1 FROM pg_advisory_xact_lock_shared(hashtextextended(?, 0))",
                        Integer.class, marketId);
                jdbc.update("INSERT INTO bet (id, user_id, event_id, market_id, selection_id, stake, "
                        + "accepted_odds, status, idempotency_key, created_at) VALUES (?, 'user-42', ?, ?, ?, "
                        + "100.00, 2.10, 'OPEN', ?, now())", betId, eventId, marketId, WINNER, "it-" + betId);
                betIsInFlight.countDown();
                awaitQuietly(settlementHasBeenAttempted);
            }));
            assertThat(betIsInFlight.await(10, TimeUnit.SECONDS)).isTrue();

            Future<Boolean> settlement = pool.submit(() -> settlementService.settle(settled()));
            boolean blocked = false;
            try {
                settlement.get(2, TimeUnit.SECONDS);
            } catch (TimeoutException waitingForTheBet) {
                blocked = true;
            }
            settlementHasBeenAttempted.countDown();
            bet.get(10, TimeUnit.SECONDS);

            assertThat(blocked).as("settlement must wait for the bet that holds the shared lock").isTrue();
            assertThat(settlement.get(10, TimeUnit.SECONDS)).isTrue();
            assertThat(statusOf(betId)).as("the bet committed before settlement selected, so it is settled")
                    .isEqualTo("WON");
        } finally {
            pool.shutdownNow();
        }
    }

    private void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
