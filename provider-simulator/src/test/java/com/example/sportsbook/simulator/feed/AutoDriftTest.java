package com.example.sportsbook.simulator.feed;

import io.reactivex.rxjava3.schedulers.TestScheduler;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Virtual time: TestScheduler lets us "wait" seconds without sleeping. */
class AutoDriftTest {

    private final TestScheduler scheduler = new TestScheduler();
    private final AtomicInteger drifts = new AtomicInteger();
    private final AtomicInteger expiries = new AtomicInteger();

    private AutoDrift start(long minMs, long maxMs, Duration lease) {
        return new AutoDrift(new DriftRange(minMs, maxMs), lease, scheduler,
                drifts::incrementAndGet, expiries::incrementAndGet);
    }

    @Test
    void driftsOncePerGapAtAFixedCadence() {
        start(10, 10, Duration.ofSeconds(5));

        scheduler.advanceTimeBy(1_000, TimeUnit.MILLISECONDS);

        assertThat(drifts.get()).isEqualTo(100);
    }

    @Test
    void randomGapsStayWithinTheRange() {
        start(100, 200, Duration.ofSeconds(5));

        scheduler.advanceTimeBy(2_000, TimeUnit.MILLISECONDS);

        // 2000ms of gaps between 100 and 200ms: at least 10 drifts, at most 20.
        assertThat(drifts.get()).isBetween(10, 20);
    }

    @Test
    void stopsByItselfWhenTheLeaseIsNotRenewed() {
        AutoDrift drift = start(100, 100, Duration.ofSeconds(1));

        scheduler.advanceTimeBy(5, TimeUnit.SECONDS);

        assertThat(drift.isDisposed()).isTrue();
        assertThat(expiries.get()).isEqualTo(1);
        // Drifts at 100..900ms; the lease ends at 1000ms, ahead of the drift due at that instant.
        assertThat(drifts.get()).isEqualTo(9);
    }

    @Test
    void leaseExpiryIsReportedOnTimeEvenWhenTheNextDriftIsFarAway() {
        AutoDrift drift = start(60_000, 60_000, Duration.ofSeconds(5));

        scheduler.advanceTimeBy(5, TimeUnit.SECONDS);

        assertThat(drift.isDisposed()).isTrue();
        assertThat(expiries.get()).isEqualTo(1);
        assertThat(drifts.get()).isZero();
    }

    @Test
    void renewingKeepsItRunningAndAppliesTheNewRange() {
        AutoDrift drift = start(100, 100, Duration.ofSeconds(1));

        scheduler.advanceTimeBy(500, TimeUnit.MILLISECONDS);
        drift.renew(new DriftRange(10, 10));
        scheduler.advanceTimeBy(900, TimeUnit.MILLISECONDS);

        assertThat(drift.isDisposed()).isFalse();
        assertThat(expiries.get()).isZero();
        // 5 at 100ms, then one more 100ms gap already scheduled, then 10ms gaps.
        assertThat(drifts.get()).isGreaterThan(50);
    }

    @Test
    void disposeStopsTheLoopWithoutReportingExpiry() {
        AutoDrift drift = start(10, 10, Duration.ofSeconds(5));
        scheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);

        drift.dispose();
        int before = drifts.get();
        scheduler.advanceTimeBy(1, TimeUnit.SECONDS);

        assertThat(drifts.get()).isEqualTo(before);
        assertThat(expiries.get()).isZero();
    }

    @Test
    void rangeOutsideTheSupportedBoundsIsRejected() {
        assertThatThrownBy(() -> new DriftRange(5, 100)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DriftRange(500, 100)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DriftRange(10, 120_000)).isInstanceOf(IllegalArgumentException.class);
    }
}
