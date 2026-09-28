package com.example.sportsbook.simulator.feed;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Keeps one match's odds moving on its own: wait a random gap from the {@link DriftRange}, drift,
 * repeat.
 *
 * <pre>
 *   defer(timer(random gap)) -> repeat
 * </pre>
 *
 * <p>{@code defer} matters: it re-samples the gap on every repetition. {@code timer(sample())}
 * without it would compute one gap at assembly time and repeat that same gap forever.
 *
 * <p><b>Lease.</b> The loop stops by itself unless {@link #renew} is called within {@code lease}.
 * The dev panel renews while it is open, so closing the tab cannot leave orphaned traffic running.
 * The lease has its own timer, restarted on every renewal, rather than being checked on drift
 * ticks: with a 60s gap a tick-based check would notice an expired lease up to a minute late.
 *
 * <p>Pass the verticle's context scheduler: ticks then run on the same event loop as the
 * {@link ProviderFeed}, which is confined to it. Tests pass a {@code TestScheduler}.
 */
public final class AutoDrift implements Disposable {

    private final Scheduler scheduler;
    private final Duration lease;
    private final Runnable expired;
    private final Disposable loop;

    private DriftRange range;
    private Disposable leaseTimer;

    /**
     * @param drift   one drift, run on each tick
     * @param expired run once if the loop stops because the lease ran out
     */
    public AutoDrift(DriftRange range, Duration lease, Scheduler scheduler, Runnable drift, Runnable expired) {
        this.scheduler = scheduler;
        this.lease = lease;
        this.expired = expired;
        this.range = range;
        this.loop = Flowable.defer(() ->
                        Flowable.timer(this.range.sampleDelayMillis(), TimeUnit.MILLISECONDS, scheduler))
                .repeat()
                .subscribe(tick -> drift.run());
        restartLease();
    }

    /** Extends the lease and applies a new range from the next gap on, without restarting. */
    public void renew(DriftRange newRange) {
        this.range = newRange;
        restartLease();
    }

    private void restartLease() {
        if (leaseTimer != null) {
            leaseTimer.dispose();
        }
        leaseTimer = scheduler.scheduleDirect(this::expire, lease.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void expire() {
        if (!loop.isDisposed()) {
            loop.dispose();
            expired.run();
        }
    }

    public DriftRange range() {
        return range;
    }

    @Override
    public void dispose() {
        leaseTimer.dispose();
        loop.dispose();
    }

    @Override
    public boolean isDisposed() {
        return loop.isDisposed();
    }
}
