package com.example.sportsbook.odds.service;

import com.example.sportsbook.common.EventStatus;
import com.example.sportsbook.common.MarketStatus;
import com.example.sportsbook.odds.dto.EventSnapshotResponse;
import com.example.sportsbook.odds.exception.EventNotFoundException;
import com.example.sportsbook.odds.repository.EventStateRepository;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EventSnapshotServiceTest {

    private final Instant updatedAt = Instant.parse("2026-09-26T10:00:00Z");
    private final EventStateRepository repository = mock(EventStateRepository.class);
    private final EventSnapshotService service = new EventSnapshotService(repository);

    @Test
    void assemblesEventMarketsAndSelectionsIntoOneSnapshot() {
        when(repository.findEvent("event-123")).thenReturn(Optional.of(
                new EventStateRepository.EventState("event-123", EventStatus.LIVE, 1004, updatedAt)));
        when(repository.findMarketIds("event-123")).thenReturn(Set.of("market-456"));
        when(repository.findMarket("market-456")).thenReturn(Optional.of(
                new EventStateRepository.MarketState("market-456", "event-123",
                        MarketStatus.ACTIVE, 1003, updatedAt)));
        Map<String, BigDecimal> odds = new LinkedHashMap<>();
        odds.put("draw", new BigDecimal("3.40"));
        odds.put("real-madrid", new BigDecimal("2.10"));
        when(repository.findOdds("market-456")).thenReturn(odds);

        EventSnapshotResponse snapshot = service.snapshot("event-123");

        assertThat(snapshot.eventId()).isEqualTo("event-123");
        assertThat(snapshot.status()).isEqualTo(EventStatus.LIVE);
        assertThat(snapshot.markets()).hasSize(1);
        assertThat(snapshot.markets().getFirst().status()).isEqualTo(MarketStatus.ACTIVE);
        assertThat(snapshot.markets().getFirst().version()).isEqualTo(1003);
        assertThat(snapshot.markets().getFirst().selections())
                .extracting(s -> s.selectionId() + "@" + s.odds().toPlainString())
                .containsExactly("draw@3.40", "real-madrid@2.10");
    }

    @Test
    void unknownEventRaisesNotFoundRatherThanReturningAnEmptyShell() {
        when(repository.findEvent("missing")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.snapshot("missing"))
                .isInstanceOf(EventNotFoundException.class)
                .hasMessageContaining("missing");
    }

    @Test
    void eventWithNoProjectedMarketsStillReturnsTheEvent() {
        when(repository.findEvent("event-123")).thenReturn(Optional.of(
                new EventStateRepository.EventState("event-123", EventStatus.SCHEDULED, 1, updatedAt)));
        when(repository.findMarketIds("event-123")).thenReturn(Set.of());

        EventSnapshotResponse snapshot = service.snapshot("event-123");

        assertThat(snapshot.markets()).isEmpty();
        assertThat(snapshot.status()).isEqualTo(EventStatus.SCHEDULED);
    }

    @Test
    void marketIdRegisteredButNotYetProjectedIsSkippedInsteadOfFailingTheWholeSnapshot() {
        when(repository.findEvent("event-123")).thenReturn(Optional.of(
                new EventStateRepository.EventState("event-123", EventStatus.LIVE, 1, updatedAt)));
        when(repository.findMarketIds("event-123")).thenReturn(Set.of("market-456"));
        when(repository.findMarket("market-456")).thenReturn(Optional.empty());

        EventSnapshotResponse snapshot = service.snapshot("event-123");

        assertThat(snapshot.markets()).isEmpty();
    }
}
