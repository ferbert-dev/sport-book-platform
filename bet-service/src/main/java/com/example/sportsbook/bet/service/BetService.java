package com.example.sportsbook.bet.service;

import com.example.sportsbook.bet.config.BetServiceProperties;
import com.example.sportsbook.bet.domain.Bet;
import com.example.sportsbook.bet.domain.OutboxEvent;
import com.example.sportsbook.bet.dto.BetOutcome;
import com.example.sportsbook.bet.dto.PlaceBetRequest;
import com.example.sportsbook.bet.dto.PlaceBetResponse;
import com.example.sportsbook.bet.repository.BetRepository;
import com.example.sportsbook.bet.repository.MarketStateReader;
import com.example.sportsbook.bet.repository.OutboxRepository;
import com.example.sportsbook.common.BetEventType;
import com.example.sportsbook.common.BetPlacedEvent;
import com.example.sportsbook.common.EventStatus;
import com.example.sportsbook.common.MarketStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Bet placement.
 *
 * <p>Validation reads Redis (cheap, current); acceptance writes PostgreSQL (durable, transactional).
 * The bet row and its outbox row commit together, so BET_PLACED can never be lost even if Kafka is
 * down at the moment of acceptance.
 */
@Service
public class BetService {

    private static final Logger log = LoggerFactory.getLogger(BetService.class);
    private static final String AGGREGATE_TYPE = "BET";

    private final BetRepository betRepository;
    private final OutboxRepository outboxRepository;
    private final MarketStateReader marketState;
    private final BetServiceProperties properties;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    private final Counter accepted;
    private final Counter rejected;
    private final Counter duplicates;
    private final Timer acceptanceLatency;

    public BetService(BetRepository betRepository,
                      OutboxRepository outboxRepository,
                      MarketStateReader marketState,
                      BetServiceProperties properties,
                      ObjectMapper objectMapper,
                      Clock clock,
                      MeterRegistry meterRegistry) {
        this.betRepository = betRepository;
        this.outboxRepository = outboxRepository;
        this.marketState = marketState;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.clock = clock;
        this.accepted = Counter.builder("bets_accepted_total").register(meterRegistry);
        this.rejected = Counter.builder("bets_rejected_total").register(meterRegistry);
        this.duplicates = Counter.builder("bets_duplicate_requests_total").register(meterRegistry);
        this.acceptanceLatency = Timer.builder("bet_acceptance_latency").register(meterRegistry);
    }

    @Transactional
    public PlaceBetResponse placeBet(PlaceBetRequest request) {
        return acceptanceLatency.record(() -> doPlaceBet(request));
    }

    private PlaceBetResponse doPlaceBet(PlaceBetRequest request) {
        // 1. Replay protection comes first: a retry must never be re-validated against newer odds,
        //    or a client retrying after a network timeout could be rejected for a bet it already holds.
        Optional<Bet> existing =
                betRepository.findByUserIdAndIdempotencyKey(request.userId(), request.idempotencyKey());
        if (existing.isPresent()) {
            Bet bet = existing.get();
            duplicates.increment();
            log.info("DUPLICATE_BET_REQUEST betId={} userId={} idempotencyKey={}",
                    bet.getId(), bet.getUserId(), bet.getIdempotencyKey());
            return PlaceBetResponse.accepted(bet.getId().toString(), bet.getAcceptedOdds());
        }

        // 2. Event must be live.
        EventStatus eventStatus = marketState.findEventStatus(request.eventId()).orElse(null);
        if (eventStatus != EventStatus.LIVE) {
            return reject(request, BetOutcome.EVENT_NOT_LIVE,
                    "Event status is " + (eventStatus == null ? "UNKNOWN" : eventStatus));
        }

        // 3. Market must exist and be active.
        MarketStateReader.MarketState market = marketState.findMarket(request.marketId()).orElse(null);
        if (market == null) {
            return reject(request, BetOutcome.MARKET_SUSPENDED, "Market has no current state");
        }
        if (market.status() != MarketStatus.ACTIVE) {
            return reject(request, BetOutcome.MARKET_SUSPENDED, "Market status is " + market.status());
        }

        // 4. Market state must be fresh. Without this, a dead provider feed would let us keep
        //    accepting bets against odds that stopped reflecting reality.
        if (isStale(market.lastUpdatedAt())) {
            log.warn("BET_REJECTED_STALE_MARKET userId={} marketId={} lastUpdatedAt={}",
                    request.userId(), request.marketId(), market.lastUpdatedAt());
            rejected.increment();
            return PlaceBetResponse.rejected(BetOutcome.STALE_MARKET_DATA,
                    "Market state older than " + properties.marketFreshnessThreshold());
        }

        // 5. Odds must still match what the client was shown.
        BigDecimal currentOdds = marketState.findOdds(request.marketId(), request.selectionId()).orElse(null);
        if (currentOdds == null) {
            return reject(request, BetOutcome.INVALID_REQUEST, "Unknown selection for market");
        }
        if (!oddsAcceptable(currentOdds, request.expectedOdds())) {
            log.info("BET_REJECTED_ODDS_CHANGED userId={} marketId={} expected={} current={}",
                    request.userId(), request.marketId(), request.expectedOdds(), currentOdds);
            rejected.increment();
            return PlaceBetResponse.oddsChanged(currentOdds);
        }

        return persist(request, currentOdds);
    }

    /**
     * Writes the bet and its outbox event in one transaction.
     *
     * <p>The unique constraint is the real guard against concurrent duplicate requests: two
     * simultaneous retries can both pass the step-1 read, so one of them loses the insert race and
     * is resolved by re-reading the winner's row.
     */
    private PlaceBetResponse persist(PlaceBetRequest request, BigDecimal acceptedOdds) {
        Bet bet = Bet.accept(request.userId(), request.eventId(), request.marketId(),
                request.selectionId(), request.stake(), acceptedOdds, request.idempotencyKey());
        try {
            betRepository.saveAndFlush(bet);
        } catch (DataIntegrityViolationException raceLost) {
            duplicates.increment();
            log.info("DUPLICATE_BET_REQUEST_RACE userId={} idempotencyKey={}",
                    request.userId(), request.idempotencyKey());
            Bet winner = betRepository
                    .findByUserIdAndIdempotencyKey(request.userId(), request.idempotencyKey())
                    .orElseThrow(() -> raceLost);
            return PlaceBetResponse.accepted(winner.getId().toString(), winner.getAcceptedOdds());
        }

        outboxRepository.save(OutboxEvent.pending(AGGREGATE_TYPE, bet.getId().toString(),
                BetEventType.BET_PLACED.name(), serialize(bet, acceptedOdds)));

        accepted.increment();
        log.info("BET_ACCEPTED betId={} userId={} eventId={} marketId={} selectionId={} stake={} odds={}",
                bet.getId(), bet.getUserId(), bet.getEventId(), bet.getMarketId(),
                bet.getSelectionId(), bet.getStake(), acceptedOdds);

        return PlaceBetResponse.accepted(bet.getId().toString(), acceptedOdds);
    }

    private String serialize(Bet bet, BigDecimal acceptedOdds) {
        BetPlacedEvent event = new BetPlacedEvent(bet.getId().toString(), bet.getUserId(),
                bet.getEventId(), bet.getMarketId(), bet.getSelectionId(), bet.getStake(),
                acceptedOdds, bet.getCreatedAt());
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception serializationFailure) {
            // Rolls back the whole transaction: a bet we cannot announce must not be accepted.
            throw new IllegalStateException("Could not serialize BET_PLACED for bet " + bet.getId(),
                    serializationFailure);
        }
    }

    private boolean isStale(Instant lastUpdatedAt) {
        Duration age = Duration.between(lastUpdatedAt, Instant.now(clock));
        return age.compareTo(properties.marketFreshnessThreshold()) > 0;
    }

    /** Drift beyond the configured tolerance in either direction rejects the bet. */
    private boolean oddsAcceptable(BigDecimal currentOdds, BigDecimal expectedOdds) {
        return currentOdds.subtract(expectedOdds).abs().compareTo(properties.oddsTolerance()) <= 0;
    }

    private PlaceBetResponse reject(PlaceBetRequest request, BetOutcome outcome, String reason) {
        rejected.increment();
        log.info("BET_REJECTED outcome={} userId={} eventId={} marketId={} reason={}",
                outcome, request.userId(), request.eventId(), request.marketId(), reason);
        return PlaceBetResponse.rejected(outcome, reason);
    }
}
