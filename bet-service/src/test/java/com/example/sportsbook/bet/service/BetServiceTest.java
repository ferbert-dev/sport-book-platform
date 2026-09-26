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
import com.example.sportsbook.common.EventStatus;
import com.example.sportsbook.common.MarketStatus;
import com.example.sportsbook.common.SportsbookJson;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataIntegrityViolationException;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BetServiceTest {

    private static final String USER = "user-42";
    private static final String EVENT = "event-123";
    private static final String MARKET = "market-456";
    private static final String SELECTION = "real-madrid";

    private final Instant now = Instant.parse("2026-09-26T10:00:00Z");
    private final Clock clock = Clock.fixed(now, ZoneOffset.UTC);

    private final BetRepository betRepository = mock(BetRepository.class);
    private final OutboxRepository outboxRepository = mock(OutboxRepository.class);
    private final MarketStateReader marketState = mock(MarketStateReader.class);

    private BetService service;

    @BeforeEach
    void setUp() {
        BetServiceProperties properties =
                new BetServiceProperties(Duration.ofSeconds(30), BigDecimal.ZERO);
        service = new BetService(betRepository, outboxRepository, marketState, properties,
                SportsbookJson.create(), clock, new SimpleMeterRegistry());

        when(betRepository.findByUserIdAndIdempotencyKey(any(), any())).thenReturn(Optional.empty());
    }

    private void marketIsBettable(BigDecimal odds) {
        when(marketState.findEventStatus(EVENT)).thenReturn(Optional.of(EventStatus.LIVE));
        when(marketState.findMarket(MARKET)).thenReturn(Optional.of(
                new MarketStateReader.MarketState(MARKET, MarketStatus.ACTIVE, now.minusSeconds(1))));
        when(marketState.findOdds(MARKET, SELECTION)).thenReturn(Optional.of(odds));
    }

    private PlaceBetRequest request(String expectedOdds, String idempotencyKey) {
        return new PlaceBetRequest(USER, EVENT, MARKET, SELECTION,
                new BigDecimal("100.00"), new BigDecimal(expectedOdds), idempotencyKey);
    }

    @Test
    void validBetIsAccepted() {
        marketIsBettable(new BigDecimal("2.10"));

        PlaceBetResponse response = service.placeBet(request("2.10", "key-1"));

        assertThat(response.status()).isEqualTo(BetOutcome.ACCEPTED);
        assertThat(response.betId()).isNotNull();
        assertThat(response.acceptedOdds()).isEqualByComparingTo("2.10");
        verify(betRepository).saveAndFlush(any(Bet.class));
    }

    @Test
    void acceptedBetWritesTheOutboxEventInTheSameTransaction() {
        marketIsBettable(new BigDecimal("2.10"));

        service.placeBet(request("2.10", "key-1"));

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepository).save(captor.capture());
        OutboxEvent outbox = captor.getValue();
        assertThat(outbox.getEventType()).isEqualTo(BetEventType.BET_PLACED.name());
        assertThat(outbox.getAggregateType()).isEqualTo("BET");
        assertThat(outbox.getPublishedAt()).isNull();
        assertThat(outbox.getPayload())
                .contains("\"type\":\"BET_PLACED\"")
                .contains("\"userId\":\"user-42\"")
                .contains("\"odds\":2.10");
    }

    @Test
    void suspendedMarketIsRejected() {
        when(marketState.findEventStatus(EVENT)).thenReturn(Optional.of(EventStatus.LIVE));
        when(marketState.findMarket(MARKET)).thenReturn(Optional.of(
                new MarketStateReader.MarketState(MARKET, MarketStatus.SUSPENDED, now)));

        PlaceBetResponse response = service.placeBet(request("2.10", "key-1"));

        assertThat(response.status()).isEqualTo(BetOutcome.MARKET_SUSPENDED);
        verify(betRepository, never()).saveAndFlush(any());
        verify(outboxRepository, never()).save(any());
    }

    @Test
    void nonLiveEventIsRejected() {
        when(marketState.findEventStatus(EVENT)).thenReturn(Optional.of(EventStatus.SCHEDULED));

        PlaceBetResponse response = service.placeBet(request("2.10", "key-1"));

        assertThat(response.status()).isEqualTo(BetOutcome.EVENT_NOT_LIVE);
        verify(betRepository, never()).saveAndFlush(any());
    }

    @Test
    void unknownEventIsRejectedAsNotLiveRatherThanAccepted() {
        when(marketState.findEventStatus(EVENT)).thenReturn(Optional.empty());

        assertThat(service.placeBet(request("2.10", "key-1")).status())
                .isEqualTo(BetOutcome.EVENT_NOT_LIVE);
    }

    @Test
    void staleMarketDataIsRejectedSoADeadFeedCannotKeepTakingBets() {
        when(marketState.findEventStatus(EVENT)).thenReturn(Optional.of(EventStatus.LIVE));
        when(marketState.findMarket(MARKET)).thenReturn(Optional.of(
                new MarketStateReader.MarketState(MARKET, MarketStatus.ACTIVE, now.minusSeconds(31))));

        PlaceBetResponse response = service.placeBet(request("2.10", "key-1"));

        assertThat(response.status()).isEqualTo(BetOutcome.STALE_MARKET_DATA);
        verify(betRepository, never()).saveAndFlush(any());
    }

    @Test
    void marketExactlyAtTheFreshnessBoundaryIsStillAccepted() {
        when(marketState.findEventStatus(EVENT)).thenReturn(Optional.of(EventStatus.LIVE));
        when(marketState.findMarket(MARKET)).thenReturn(Optional.of(
                new MarketStateReader.MarketState(MARKET, MarketStatus.ACTIVE, now.minusSeconds(30))));
        when(marketState.findOdds(MARKET, SELECTION)).thenReturn(Optional.of(new BigDecimal("2.10")));

        assertThat(service.placeBet(request("2.10", "key-1")).status()).isEqualTo(BetOutcome.ACCEPTED);
    }

    @Test
    void changedOddsAreRejectedAndTheCurrentPriceIsReturned() {
        marketIsBettable(new BigDecimal("1.95"));

        PlaceBetResponse response = service.placeBet(request("2.10", "key-1"));

        assertThat(response.status()).isEqualTo(BetOutcome.ODDS_CHANGED);
        assertThat(response.currentOdds()).isEqualByComparingTo("1.95");
        verify(betRepository, never()).saveAndFlush(any());
    }

    @Test
    void oddsMovingInTheBettorsFavourIsAlsoRejectedUnderZeroTolerance() {
        marketIsBettable(new BigDecimal("2.50"));

        assertThat(service.placeBet(request("2.10", "key-1")).status())
                .isEqualTo(BetOutcome.ODDS_CHANGED);
    }

    @Test
    void unknownSelectionIsRejectedAsInvalid() {
        when(marketState.findEventStatus(EVENT)).thenReturn(Optional.of(EventStatus.LIVE));
        when(marketState.findMarket(MARKET)).thenReturn(Optional.of(
                new MarketStateReader.MarketState(MARKET, MarketStatus.ACTIVE, now)));
        when(marketState.findOdds(MARKET, SELECTION)).thenReturn(Optional.empty());

        assertThat(service.placeBet(request("2.10", "key-1")).status())
                .isEqualTo(BetOutcome.INVALID_REQUEST);
    }

    @Test
    void sameIdempotencyKeyReturnsTheOriginalBetWithoutCreatingASecondOne() {
        Bet existing = Bet.accept(USER, EVENT, MARKET, SELECTION,
                new BigDecimal("100.00"), new BigDecimal("2.10"), "key-1");
        when(betRepository.findByUserIdAndIdempotencyKey(USER, "key-1"))
                .thenReturn(Optional.of(existing));

        PlaceBetResponse response = service.placeBet(request("2.10", "key-1"));

        assertThat(response.status()).isEqualTo(BetOutcome.ACCEPTED);
        assertThat(response.betId()).isEqualTo(existing.getId().toString());
        verify(betRepository, never()).saveAndFlush(any());
        verify(outboxRepository, never()).save(any());
    }

    @Test
    void retryIsNotRevalidatedAgainstNewerOddsSoAHeldBetIsNeverRejectedLater() {
        Bet existing = Bet.accept(USER, EVENT, MARKET, SELECTION,
                new BigDecimal("100.00"), new BigDecimal("2.10"), "key-1");
        when(betRepository.findByUserIdAndIdempotencyKey(USER, "key-1"))
                .thenReturn(Optional.of(existing));
        // Market has since gone stale and suspended; the retry must still succeed.
        when(marketState.findEventStatus(EVENT)).thenReturn(Optional.of(EventStatus.FINISHED));

        PlaceBetResponse response = service.placeBet(request("2.10", "key-1"));

        assertThat(response.status()).isEqualTo(BetOutcome.ACCEPTED);
        assertThat(response.acceptedOdds()).isEqualByComparingTo("2.10");
    }

    @Test
    void concurrentDuplicateLosingTheInsertRaceResolvesToTheWinningBet() {
        marketIsBettable(new BigDecimal("2.10"));
        Bet winner = Bet.accept(USER, EVENT, MARKET, SELECTION,
                new BigDecimal("100.00"), new BigDecimal("2.10"), "key-1");
        when(betRepository.saveAndFlush(any(Bet.class)))
                .thenThrow(new DataIntegrityViolationException("uk_bet_user_idempotency"));
        when(betRepository.findByUserIdAndIdempotencyKey(USER, "key-1"))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(winner));

        PlaceBetResponse response = service.placeBet(request("2.10", "key-1"));

        assertThat(response.status()).isEqualTo(BetOutcome.ACCEPTED);
        assertThat(response.betId()).isEqualTo(winner.getId().toString());
        verify(outboxRepository, never()).save(any());
    }

    @Test
    void acceptedBetIsStoredAsOpenWithTheOddsActuallyAvailable() {
        marketIsBettable(new BigDecimal("2.10"));

        service.placeBet(request("2.10", "key-1"));

        ArgumentCaptor<Bet> captor = ArgumentCaptor.forClass(Bet.class);
        verify(betRepository).saveAndFlush(captor.capture());
        Bet saved = captor.getValue();
        assertThat(saved.getStatus()).isEqualTo(com.example.sportsbook.common.BetStatus.OPEN);
        assertThat(saved.getAcceptedOdds()).isEqualByComparingTo("2.10");
        assertThat(saved.getStake()).isEqualByComparingTo("100.00");
        assertThat(saved.getPayout()).isNull();
        assertThat(saved.getSettledAt()).isNull();
    }
}
