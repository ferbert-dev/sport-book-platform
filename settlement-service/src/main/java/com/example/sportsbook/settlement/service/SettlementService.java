package com.example.sportsbook.settlement.service;

import com.example.sportsbook.common.BetSettledEvent;
import com.example.sportsbook.common.BetStatus;
import com.example.sportsbook.common.MarketSettledEvent;
import com.example.sportsbook.common.PayoutRequiredEvent;
import com.example.sportsbook.settlement.domain.Bet;
import com.example.sportsbook.settlement.domain.ProcessedSettlement;
import com.example.sportsbook.settlement.messaging.BetEventPublisher;
import com.example.sportsbook.settlement.repository.BetRepository;
import com.example.sportsbook.settlement.repository.ProcessedSettlementRepository;
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
 * service's responsibility ends at publishing PAYOUT_REQUIRED.
 */
@Service
public class SettlementService {

    private static final Logger log = LoggerFactory.getLogger(SettlementService.class);

    private final BetRepository betRepository;
    private final ProcessedSettlementRepository processedSettlements;
    private final BetEventPublisher publisher;
    private final String payoutCurrency;
    private final Counter settlementsProcessed;
    private final Counter duplicateSettlements;

    public SettlementService(BetRepository betRepository,
                             ProcessedSettlementRepository processedSettlements,
                             BetEventPublisher publisher,
                             @Value("${sportsbook.settlement.payout-currency:EUR}") String payoutCurrency,
                             MeterRegistry meterRegistry) {
        this.betRepository = betRepository;
        this.processedSettlements = processedSettlements;
        this.publisher = publisher;
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

        publisher.publish(new BetSettledEvent(bet.getId().toString(), bet.getStatus(), payout,
                Instant.now()));

        if (won) {
            // Deterministic payment id: a redelivered settlement would produce the same id, so the
            // payments context can dedupe even if it receives the event twice.
            publisher.publish(new PayoutRequiredEvent(
                    PayoutRequiredEvent.paymentId(bet.getId().toString(), settlementVersion),
                    bet.getId().toString(), bet.getUserId(), payout, payoutCurrency, Instant.now()));
        }
        return won;
    }
}
