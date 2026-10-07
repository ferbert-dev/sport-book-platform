package com.example.sportsbook.settlement.service;

import com.example.sportsbook.common.BetEvent;
import com.example.sportsbook.common.BetSettledEvent;
import com.example.sportsbook.common.BetStatus;
import com.example.sportsbook.common.MarketSettledEvent;
import com.example.sportsbook.common.PayoutRequiredEvent;
import com.example.sportsbook.settlement.domain.Bet;
import com.example.sportsbook.settlement.domain.OutboxEvent;
import com.example.sportsbook.settlement.domain.ProcessedSettlement;
import com.example.sportsbook.settlement.repository.BetRepository;
import com.example.sportsbook.settlement.repository.OutboxRepository;
import com.example.sportsbook.settlement.repository.ProcessedSettlementRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Settles all open bets on a market, exactly once per {@code (marketId, settlementVersion)}.
 *
 * <p>Deliberately does not touch user balances: Payments/Wallet is a separate bounded context. This
 * service's responsibility ends at queueing PAYOUT_REQUIRED.
 *
 * <p><b>Nothing here talks to Kafka.</b> BET_SETTLED and PAYOUT_REQUIRED are written to the
 * transactional outbox ({@code outbox_event}) in the same transaction as the bet rows and the fence
 * row, so they commit together or not at all; bet-service's {@code OutboxPublisher} relays them to
 * {@code bet-events} afterwards. Sending from inside this transaction could leave a payout in Kafka
 * for a bet the database then failed to settle; sending after the commit could lose the event for
 * good, because the fence would skip the retry.
 */
@Service
public class SettlementService {

    private static final Logger log = LoggerFactory.getLogger(SettlementService.class);
    private static final String AGGREGATE_TYPE = "BET";

    private final BetRepository betRepository;
    private final ProcessedSettlementRepository processedSettlements;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final String payoutCurrency;
    private final Counter settlementsProcessed;
    private final Counter duplicateSettlements;

    public SettlementService(BetRepository betRepository,
                             ProcessedSettlementRepository processedSettlements,
                             OutboxRepository outboxRepository,
                             ObjectMapper objectMapper,
                             @Value("${sportsbook.settlement.payout-currency:EUR}") String payoutCurrency,
                             MeterRegistry meterRegistry) {
        this.betRepository = betRepository;
        this.processedSettlements = processedSettlements;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.payoutCurrency = payoutCurrency;
        this.settlementsProcessed = Counter.builder("settlements_processed_total").register(meterRegistry);
        this.duplicateSettlements = Counter.builder("duplicate_settlements_total").register(meterRegistry);
    }

    /**
     * @return true when this delivery performed the settlement, false when it was a duplicate
     */
    @Transactional
    public boolean settle(MarketSettledEvent event) {
        long settlementVersion = event.version();

        // First statement, before the fence row and before the bets are selected: bet-service takes
        // the same lock in shared mode around "is this market settled? -> insert the bet". So every
        // bet that got in before us has committed by the time we hold the lock and is selected
        // below, and every bet after us finds the fence row and is rejected. Without it a bet could
        // commit between our SELECT and our COMMIT and stay OPEN forever.
        processedSettlements.lockForSettlement(event.marketId());

        if (!registerSettlement(event.marketId(), settlementVersion)) {
            duplicateSettlements.increment();
            log.info("DUPLICATE_SETTLEMENT_SKIPPED marketId={} settlementVersion={}",
                    event.marketId(), settlementVersion);
            return false;
        }

        log.info("SETTLEMENT_STARTED eventId={} marketId={} winningSelectionId={} settlementVersion={}",
                event.eventId(), event.marketId(), event.winningSelectionId(), settlementVersion);

        List<Bet> openBets = betRepository.findByEventIdAndMarketIdAndStatus(
                event.eventId(), event.marketId(), BetStatus.OPEN);

        int won = 0;
        for (Bet bet : openBets) {
            if (settleOne(bet, event.winningSelectionId(), settlementVersion)) {
                won++;
            }
        }
        betRepository.saveAll(openBets);

        settlementsProcessed.increment();
        log.info("SETTLEMENT_COMPLETED marketId={} settlementVersion={} bets={} won={} lost={}",
                event.marketId(), settlementVersion, openBets.size(), won, openBets.size() - won);
        return true;
    }

    /** @return true if this settlement is new, false if already processed */
    private boolean registerSettlement(String marketId, long settlementVersion) {
        if (processedSettlements.existsByMarketIdAndSettlementVersion(marketId, settlementVersion)) {
            return false;
        }
        try {
            // The unique constraint, not the read above, is the real fence: two concurrent
            // deliveries can both pass the read, and exactly one wins this insert.
            processedSettlements.saveAndFlush(ProcessedSettlement.of(marketId, settlementVersion));
            return true;
        } catch (DataIntegrityViolationException alreadyProcessed) {
            return false;
        }
    }

    private boolean settleOne(Bet bet, String winningSelectionId, long settlementVersion) {
        boolean won = bet.won(winningSelectionId);
        BigDecimal payout = won
                ? PayoutCalculator.winningPayout(bet.getStake(), bet.getAcceptedOdds())
                : PayoutCalculator.losingPayout();

        bet.settle(won ? BetStatus.WON : BetStatus.LOST, payout);

        enqueue(new BetSettledEvent(bet.getId().toString(), bet.getStatus(), payout,
                Instant.now()));

        if (won) {
            // Deterministic payment id. The outbox relay is at-least-once (it can send a row and
            // crash before marking it), so the payments context must be able to dedupe.
            enqueue(new PayoutRequiredEvent(
                    PayoutRequiredEvent.paymentId(bet.getId().toString(), settlementVersion),
                    bet.getId().toString(), bet.getUserId(), payout, payoutCurrency, Instant.now()));
        }
        return won;
    }

    /**
     * Queues an event in the outbox. The aggregate id is the bet id: the relay uses it as the Kafka
     * key, which is what keeps every event of one bet on one partition, in order.
     */
    private void enqueue(BetEvent event) {
        outboxRepository.save(OutboxEvent.pending(AGGREGATE_TYPE, event.betId(), event.type().name(),
                serialize(event)));
    }

    private String serialize(BetEvent event) {
        try {
            // Through the BetEvent interface, so Jackson writes the "type" discriminator.
            return objectMapper.writerFor(BetEvent.class).writeValueAsString(event);
        } catch (Exception serializationFailure) {
            // Rolls the settlement back: a bet whose event can never be sent must not be settled.
            throw new IllegalStateException("Could not serialize " + event.type() + " for bet " + event.betId(),
                    serializationFailure);
        }
    }
}
