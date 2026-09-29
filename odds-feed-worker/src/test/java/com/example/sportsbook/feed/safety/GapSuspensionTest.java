package com.example.sportsbook.feed.safety;

import com.example.sportsbook.common.MarketOpenedEvent;
import com.example.sportsbook.common.MarketSettledEvent;
import com.example.sportsbook.common.MarketSuspendedEvent;
import com.example.sportsbook.common.MatchStartedEvent;
import com.example.sportsbook.common.OddsUpdatedEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class GapSuspensionTest {

    private static final Instant NOW = Instant.parse("2026-09-29T10:00:00Z");

    private final ActiveMarkets active = new ActiveMarkets();

    @Test
    void marketsBecomeActiveOnPricesOrOpeningAndLeaveOnSettlement() {
        active.track(new MatchStartedEvent("e1", 100, NOW));
        active.track(new MarketOpenedEvent("e1", "m1", 101, NOW));
        active.track(new OddsUpdatedEvent("e2", "m2", "home", new BigDecimal("2.10"), 102, NOW));
        active.track(new MarketSettledEvent("e1", "m1", "home", 103, NOW));

        assertThat(active.snapshot()).containsExactlyEntriesOf(java.util.Map.of("m2", "e2"));
    }

    @Test
    void aSuspendedMarketStaysTrackedBecauseAGapCouldHideItsReopening() {
        active.track(new MarketOpenedEvent("e1", "m1", 101, NOW));
        active.track(new MarketSuspendedEvent("e1", "m1", 102, NOW));

        assertThat(active.snapshot()).containsKey("m1");
    }

    @Test
    void everyActiveMarketIsSuspendedWithTheSequenceJustBelowTheOneThatRevealedTheGap() {
        active.track(new MarketOpenedEvent("e1", "m1", 101, NOW));
        active.track(new MarketOpenedEvent("e2", "m2", 102, NOW));

        // Last processed 102, expected 103, received 106: 103..105 were lost.
        List<MarketSuspendedEvent> suspensions = GapSuspension.suspendAll(active.snapshot(), 106, NOW);

        assertThat(suspensions)
                .extracting(MarketSuspendedEvent::marketId, MarketSuspendedEvent::eventId, MarketSuspendedEvent::version)
                .containsExactlyInAnyOrder(
                        org.assertj.core.groups.Tuple.tuple("m1", "e1", 105L),
                        org.assertj.core.groups.Tuple.tuple("m2", "e2", 105L));
    }

    @Test
    void suspensionVersionOutranksStoredStateButNotTheProvidersNextMessages() {
        long lastStoredForMarket = 102;
        long received = 106;
        long suspension = GapSuspension.suspendAll(java.util.Map.of("m1", "e1"), received, NOW).get(0).version();

        // The projection's guard applies strictly newer versions only.
        assertThat(suspension).isGreaterThan(lastStoredForMarket);
        // A MARKET_UNLOCK arriving as the gap-revealing message, or later, still reopens.
        assertThat(received).isGreaterThan(suspension);
    }

    @Test
    void afterAGapAPriceForAMarketThisProcessHasNotSeenIsQuarantinedFirst() {
        // Restarted worker: nothing known. A gap was seen; now a price arrives for market-456.
        OddsUpdatedEvent price = new OddsUpdatedEvent("e1", "market-456", "home", new BigDecimal("1.40"), 106, NOW);

        assertThat(GapSuspension.quarantine(price, 106, true, active, NOW))
                .hasValueSatisfying(suspension -> {
                    assertThat(suspension.marketId()).isEqualTo("market-456");
                    assertThat(suspension.version()).isEqualTo(105);
                });
    }

    @Test
    void noQuarantineWithoutAGapOrForAMarketAlreadyKnown() {
        OddsUpdatedEvent price = new OddsUpdatedEvent("e1", "m1", "home", new BigDecimal("1.40"), 106, NOW);
        assertThat(GapSuspension.quarantine(price, 106, false, active, NOW)).isEmpty();

        active.track(new MarketOpenedEvent("e1", "m1", 101, NOW));
        assertThat(GapSuspension.quarantine(price, 106, true, active, NOW)).isEmpty();
    }

    @Test
    void aMarketSeenSettledIsNeverQuarantinedBackToSuspended() {
        active.track(new MarketSettledEvent("e1", "m1", "home", 103, NOW));
        OddsUpdatedEvent latePrice = new OddsUpdatedEvent("e1", "m1", "home", new BigDecimal("1.40"), 106, NOW);

        assertThat(GapSuspension.quarantine(latePrice, 106, true, active, NOW)).isEmpty();
    }

    @Test
    void theProviderStatingTheStatusItselfIsNotQuarantined() {
        assertThat(GapSuspension.quarantine(new MarketOpenedEvent("e1", "m9", 106, NOW), 106, true, active, NOW)).isEmpty();
        assertThat(GapSuspension.quarantine(new MarketSuspendedEvent("e1", "m9", 106, NOW), 106, true, active, NOW)).isEmpty();
    }

    @Test
    void nothingActiveMeansNothingToSuspend() {
        assertThat(GapSuspension.suspendAll(active.snapshot(), 106, NOW)).isEmpty();
    }
}
