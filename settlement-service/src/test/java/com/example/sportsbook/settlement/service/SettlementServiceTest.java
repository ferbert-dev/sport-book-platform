package com.example.sportsbook.settlement.service;

import com.example.sportsbook.common.BetEvent;
import com.example.sportsbook.common.BetSettledEvent;
import com.example.sportsbook.common.BetStatus;
import com.example.sportsbook.common.MarketSettledEvent;
import com.example.sportsbook.common.PayoutRequiredEvent;
import com.example.sportsbook.settlement.domain.Bet;
import com.example.sportsbook.settlement.messaging.BetEventPublisher;
import com.example.sportsbook.settlement.repository.BetRepository;
import com.example.sportsbook.settlement.repository.ProcessedSettlementRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SettlementServiceTest {

    private static final String EVENT_ID = "event-123";
    private static final String MARKET_ID = "market-456";
    private static final String WINNER = "real-madrid";
    private static final long SETTLEMENT_VERSION = 1001L;

    private final BetRepository betRepository = mock(BetRepository.class);
    private final ProcessedSettlementRepository processedSettlements =
            mock(ProcessedSettlementRepository.class);
    private final BetEventPublisher publisher = mock(BetEventPublisher.class);

    private SettlementService service;

    @BeforeEach
    void setUp() {
        service = new SettlementService(betRepository, processedSettlements, publisher, "EUR",
                new SimpleMeterRegistry());
        when(processedSettlements.existsByMarketIdAndSettlementVersion(MARKET_ID, SETTLEMENT_VERSION))
                .thenReturn(false);
    }

    /** Builds a Bet without going through bet-service; the entity has no public constructor. */
    private Bet bet(String selectionId, String stake, String odds) {
        Bet bet = new Bet() {
        };
        ReflectionTestUtils.setField(bet, "id", UUID.randomUUID());
        ReflectionTestUtils.setField(bet, "userId", "user-42");
        ReflectionTestUtils.setField(bet, "eventId", EVENT_ID);
        ReflectionTestUtils.setField(bet, "marketId", MARKET_ID);
        ReflectionTestUtils.setField(bet, "selectionId", selectionId);
        ReflectionTestUtils.setField(bet, "stake", new BigDecimal(stake));
        ReflectionTestUtils.setField(bet, "acceptedOdds", new BigDecimal(odds));
        ReflectionTestUtils.setField(bet, "status", BetStatus.OPEN);
        return bet;
    }

    private MarketSettledEvent settledEvent() {
        return new MarketSettledEvent(EVENT_ID, MARKET_ID, WINNER, SETTLEMENT_VERSION, Instant.now());
    }

    private void openBets(Bet... bets) {
        when(betRepository.findByEventIdAndMarketIdAndStatus(EVENT_ID, MARKET_ID, BetStatus.OPEN))
                .thenReturn(new java.util.ArrayList<>(List.of(bets)));
    }

    @Test
    void betOnTheWinningSelectionBecomesWonWithTheCorrectPayout() {
        Bet winning = bet(WINNER, "100.00", "2.10");
        openBets(winning);

        boolean settled = service.settle(settledEvent());

        assertThat(settled).isTrue();
        assertThat(winning.getStatus()).isEqualTo(BetStatus.WON);
        assertThat(winning.getPayout()).isEqualByComparingTo("210.00");
        assertThat(winning.getSettledAt()).isNotNull();
    }

    @Test
    void betOnAnyOtherSelectionBecomesLostWithZeroPayout() {
        Bet losing = bet("draw", "100.00", "3.40");
        openBets(losing);

        service.settle(settledEvent());

        assertThat(losing.getStatus()).isEqualTo(BetStatus.LOST);
        assertThat(losing.getPayout()).isEqualByComparingTo("0.00");
    }

    @Test
    void settledBetsArePersisted() {
        Bet winning = bet(WINNER, "50.00", "2.00");
        openBets(winning);

        service.settle(settledEvent());

        verify(betRepository).saveAll(any());
    }

    @Test
    void everySettledBetGetsABetSettledEvent() {
        openBets(bet(WINNER, "100.00", "2.10"), bet("draw", "100.00", "3.40"));

        service.settle(settledEvent());

        assertThat(publishedEvents()).filteredOn(BetSettledEvent.class::isInstance).hasSize(2);
    }

    @Test
    void onlyWinningBetsTriggerPayoutRequired() {
        openBets(bet(WINNER, "100.00", "2.10"), bet("draw", "100.00", "3.40"));

        service.settle(settledEvent());

        List<PayoutRequiredEvent> payouts = publishedEvents().stream()
                .filter(PayoutRequiredEvent.class::isInstance)
                .map(PayoutRequiredEvent.class::cast)
                .toList();

        assertThat(payouts).hasSize(1);
        assertThat(payouts.getFirst().amount()).isEqualByComparingTo("210.00");
        assertThat(payouts.getFirst().currency()).isEqualTo("EUR");
    }

    @Test
    void payoutIdIsDerivedFromBetAndSettlementVersionSoItIsStableAcrossRedeliveries() {
        Bet winning = bet(WINNER, "100.00", "2.10");
        openBets(winning);

        service.settle(settledEvent());

        PayoutRequiredEvent payout = publishedEvents().stream()
                .filter(PayoutRequiredEvent.class::isInstance)
                .map(PayoutRequiredEvent.class::cast)
                .findFirst().orElseThrow();

        assertThat(payout.paymentId())
                .isEqualTo(winning.getId() + "-settlement-" + SETTLEMENT_VERSION);
    }

    @Test
    void alreadyProcessedSettlementIsSkippedEntirely() {
        when(processedSettlements.existsByMarketIdAndSettlementVersion(MARKET_ID, SETTLEMENT_VERSION))
                .thenReturn(true);

        boolean settled = service.settle(settledEvent());

        assertThat(settled).isFalse();
        verify(betRepository, never()).findByEventIdAndMarketIdAndStatus(any(), any(), any());
        verify(publisher, never()).publish(any());
    }

    @Test
    void concurrentDeliveryLosingTheFenceInsertIsSkippedWithoutDuplicatePayouts() {
        when(processedSettlements.saveAndFlush(any()))
                .thenThrow(new DataIntegrityViolationException("uk_settlement_market_version"));

        boolean settled = service.settle(settledEvent());

        assertThat(settled).isFalse();
        verify(publisher, never()).publish(any());
    }

    @Test
    void settlementWithNoOpenBetsStillRecordsTheFence() {
        openBets();

        boolean settled = service.settle(settledEvent());

        assertThat(settled).isTrue();
        verify(processedSettlements).saveAndFlush(any());
        verify(publisher, never()).publish(any());
    }

    private List<BetEvent> publishedEvents() {
        ArgumentCaptor<BetEvent> captor = ArgumentCaptor.forClass(BetEvent.class);
        verify(publisher, org.mockito.Mockito.atLeastOnce()).publish(captor.capture());
        return captor.getAllValues();
    }
}
