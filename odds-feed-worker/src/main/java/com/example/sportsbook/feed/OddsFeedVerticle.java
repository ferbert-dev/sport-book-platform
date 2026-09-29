package com.example.sportsbook.feed;

import com.example.sportsbook.common.MarketSuspendedEvent;
import com.example.sportsbook.common.SportsEvent;
import com.example.sportsbook.feed.config.FeedConfig;
import com.example.sportsbook.feed.messaging.LastPublishedSequence;
import com.example.sportsbook.feed.messaging.SportsEventPublisher;
import com.example.sportsbook.feed.provider.MessageNormalizer;
import com.example.sportsbook.feed.provider.ProviderMessage;
import com.example.sportsbook.feed.provider.ProviderMessageValidator;
import com.example.sportsbook.feed.provider.ProviderStreamClient;
import com.example.sportsbook.feed.provider.ReconnectBackoff;
import com.example.sportsbook.feed.provider.SequenceDecision;
import com.example.sportsbook.feed.provider.SequenceValidator;
import com.example.sportsbook.feed.safety.ActiveMarkets;
import com.example.sportsbook.feed.safety.GapSuspension;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.disposables.Disposable;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.RxHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The reactive ingestion pipeline:
 *
 * <pre>
 *   provider stream -> validate -> sequence check -> normalize -> publish to Kafka
 * </pre>
 *
 * <p>The provider stream is a WebSocket the worker dials out to ({@link ProviderStreamClient}),
 * reconnecting with backoff. Nothing here blocks: socket reads and Kafka sends are asynchronous and
 * every stage runs on this verticle's event loop. {@code concatMapCompletable} takes one message at a
 * time through all stages, which preserves per-partition ordering and propagates backpressure
 * upstream — a slow broker slows consumption of the provider stream rather than growing an
 * unbounded in-flight set.
 *
 * <p>A sequence counts as processed only once Kafka has acknowledged it; that is what the resume
 * cursor sent to the provider is built from.
 */
public class OddsFeedVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(OddsFeedVerticle.class);
    private static final int PUBLISH_ATTEMPTS = 4;
    private static final long PIPELINE_RESTART_MS = 1_000;
    private static final long STARTUP_RETRY_MS = 2_000;

    private final FeedConfig config;
    private final FeedMetrics metrics = new FeedMetrics();
    private final SequenceValidator sequenceValidator = new SequenceValidator();
    private final ActiveMarkets activeMarkets = new ActiveMarkets();

    /** Resume point for a freshly started process, read from Kafka before the first connect. */
    private long lastPublishedAtStartup = -1;
    private SportsEventPublisher publisher;
    private ProviderStreamClient providerClient;
    private Disposable pipeline;
    private Disposable metricsLogger;

    public OddsFeedVerticle(FeedConfig config) {
        this.config = config;
    }

    @Override
    public Completable rxStart() {
        // Blocking Kafka read, so off the event loop. The pipeline starts only once it is known:
        // an unreadable topic is retried, never mistaken for "nothing published yet" — that
        // would make us ask for less than we are missing.
        return vertx.<Long>rxExecuteBlocking(() -> LastPublishedSequence.read(
                        config.kafkaBootstrapServers(), config.sportsEventsTopic()))
                .toSingle()
                .retryWhen(failures -> failures.concatMap(failure -> {
                    log.warn("RESUME_CURSOR_UNAVAILABLE reason={} retryInMs={}",
                            failure.getMessage(), STARTUP_RETRY_MS);
                    return Flowable.timer(STARTUP_RETRY_MS, TimeUnit.MILLISECONDS, RxHelper.scheduler(vertx));
                }))
                .doOnSuccess(sequence -> {
                    lastPublishedAtStartup = sequence;
                    log.info("RESUME_CURSOR_FROM_KAFKA lastPublishedSequence={}", sequence);
                })
                .ignoreElement()
                .andThen(Completable.fromAction(this::startPipeline));
    }

    /**
     * Cursor for the provider: the last sequence Kafka acknowledged — from this process, or before
     * that, from an earlier one. The validator itself is NOT seeded from Kafka: if the provider's
     * numbering ever restarted lower, a seeded validator would discard everything as duplicates.
     */
    private long resumeCursor() {
        long processed = sequenceValidator.lastProcessedSequence();
        return processed >= 0 ? processed : lastPublishedAtStartup;
    }

    private void startPipeline() {
        publisher = new SportsEventPublisher(vertx, config.kafkaBootstrapServers(), config.sportsEventsTopic());
        providerClient = new ProviderStreamClient(vertx, config.providerUrl(), config.providerConnectTimeout(),
                new ReconnectBackoff(config.reconnectInitialDelay(), config.reconnectMaxDelay()),
                metrics.oddsFeedMessagesInvalidTotal,
                this::resumeCursor);
        log.info("ODDS_FEED_PIPELINE_STARTING providerUrl={} topic={} bootstrap={}",
                config.providerUrl(), config.sportsEventsTopic(), config.kafkaBootstrapServers());

        // One message is taken through every stage — validate, sequence, normalize, publish, mark —
        // before the next is looked at. Sequence decisions therefore always see the cursor of the
        // previous message's ACKNOWLEDGED outcome, not of a message still in flight.
        //
        // A publish that keeps failing errors the whole pipeline; retryWhen then resubscribes, which
        // opens a fresh connection resuming from the last acknowledged sequence, so the provider
        // replays the failed message rather than it being skipped.
        pipeline = providerClient.messages()
                .concatMapCompletable(this::process)
                .retryWhen(failures -> failures.concatMap(failure -> {
                    log.warn("ODDS_FEED_PIPELINE_RESTARTING reason={} resumeAfterSequence={} retryInMs={}",
                            failure.getMessage(), resumeCursor(), PIPELINE_RESTART_MS);
                    return Flowable.timer(PIPELINE_RESTART_MS, TimeUnit.MILLISECONDS, RxHelper.scheduler(vertx));
                }))
                .subscribe(
                        () -> log.info("PROVIDER_STREAM_COMPLETED {}", metrics.snapshot()),
                        error -> log.error("PROVIDER_STREAM_FAILED {}", metrics.snapshot(), error));

        metricsLogger = vertx.periodicStream(30_000)
                .toFlowable()
                .subscribe(tick -> log.info("FEED_METRICS {}", metrics.snapshot()));
    }

    private Completable process(ProviderMessage message) {
        metrics.oddsFeedMessagesReceivedTotal.incrementAndGet();
        long sequence = message.sequenceNumber();
        if (!validate(message)) {
            // Skipped, but NOT marked processed: the cursor tracks what Kafka acknowledged, and
            // moving it here would hide any gap in front of this message. The next valid message
            // still goes through the gap check against the last acknowledged sequence.
            return Completable.complete();
        }
        SequenceOutcome outcome = checkSequence(message);
        if (outcome == SequenceOutcome.SKIP) {
            return Completable.complete();
        }
        // FIX 3: after a gap, close every active market BEFORE publishing the message that
        // revealed it. That message still goes out: dropping it would lose real state on top of
        // the gap, and if it is the provider's own MARKET_UNLOCK its higher version reopens.
        Completable suspensions = outcome == SequenceOutcome.PROCESS_AFTER_GAP
                ? suspendActiveMarkets(sequence)
                : Completable.complete();
        return suspensions
                .andThen(Maybe.fromOptional(MessageNormalizer.normalize(message))
                        .flatMapCompletable(event -> publish(event)
                                .doOnComplete(() -> activeMarkets.track(event))))
                // Only now, with Kafka's ack in hand, is the sequence delivered.
                .doOnComplete(() -> sequenceValidator.markProcessed(sequence));
    }

    /**
     * FIX 3: one MARKET_SUSPENDED per active market, versioned just below the message that revealed
     * the gap (see {@link GapSuspension} for why that version is both high and low enough). The
     * snapshot is taken at subscription, so a pipeline retry suspends whatever is active then.
     */
    private Completable suspendActiveMarkets(long receivedSequence) {
        return Completable.defer(() -> {
            List<MarketSuspendedEvent> suspensions =
                    GapSuspension.suspendAll(activeMarkets.snapshot(), receivedSequence, Instant.now());
            if (suspensions.isEmpty()) {
                return Completable.complete();
            }
            log.warn("MARKETS_SUSPENDED_ON_GAP markets={} version={} receivedSequence={}",
                    suspensions.size(), receivedSequence - 1, receivedSequence);
            metrics.oddsFeedGapSuspensionsTotal.addAndGet(suspensions.size());
            return Flowable.fromIterable(suspensions)
                    .concatMapCompletable(suspension -> publish(suspension)
                            .doOnComplete(() -> activeMarkets.track(suspension)));
        });
    }

    private enum SequenceOutcome { SKIP, PROCESS, PROCESS_AFTER_GAP }

    private boolean validate(ProviderMessage message) {
        if (ProviderMessageValidator.isValid(message)) {
            return true;
        }
        metrics.oddsFeedMessagesInvalidTotal.incrementAndGet();
        log.warn("PROVIDER_MESSAGE_INVALID sequenceNumber={} messageType={}",
                message.sequenceNumber(), message.messageType());
        return false;
    }

    private SequenceOutcome checkSequence(ProviderMessage message) {
        long sequence = message.sequenceNumber();
        SequenceDecision decision = sequenceValidator.decide(sequence);

        // First message of a restarted process: the validator is deliberately unseeded, so compare
        // with the cursor we asked the provider to resume from. A jump means the replay could not
        // cover everything we were missing.
        if (sequenceValidator.lastProcessedSequence() < 0 && lastPublishedAtStartup >= 0
                && sequence > lastPublishedAtStartup + 1) {
            reportGap(lastPublishedAtStartup + 1, sequence);
            return SequenceOutcome.PROCESS_AFTER_GAP;
        }

        return switch (decision) {
            case IN_ORDER -> SequenceOutcome.PROCESS;
            case DUPLICATE -> {
                metrics.oddsFeedDuplicatesTotal.incrementAndGet();
                log.info("PROVIDER_DUPLICATE_IGNORED sequenceNumber={} lastProcessedSequence={}",
                        sequence, sequenceValidator.lastProcessedSequence());
                yield SequenceOutcome.SKIP;
            }
            case GAP -> {
                reportGap(sequenceValidator.lastProcessedSequence() + 1, sequence);
                yield SequenceOutcome.PROCESS_AFTER_GAP;
            }
        };
    }

    private void reportGap(long expected, long received) {
        metrics.oddsFeedSequenceGapsTotal.incrementAndGet();
        log.warn("SEQUENCE_GAP_DETECTED expectedSequence={} receivedSequence={} missed={}",
                expected, received, received - expected);
        requestProviderSnapshot(received);
    }

    /**
     * Stands in for the real recovery call. A production worker would request a snapshot or replay
     * from the provider, rebuild state from it, and only then resume streaming.
     */
    private void requestProviderSnapshot(long fromSequence) {
        log.warn("PROVIDER_SNAPSHOT_REQUESTED fromSequence={} note=simulated-resync", fromSequence);
    }

    /**
     * Retries in place first, so a broker blip does not cost a reconnect. If Kafka stays
     * unavailable the error propagates and the pipeline restarts from the last acknowledged sequence.
     */
    private Completable publish(SportsEvent event) {
        return publisher.publish(event)
                .doOnError(error -> {
                    metrics.oddsFeedPublishFailuresTotal.incrementAndGet();
                    log.warn("KAFKA_PUBLISH_FAILED type={} version={} reason={}",
                            event.type(), event.version(), error.getMessage());
                })
                .retryWhen(failures -> failures
                        .zipWith(Flowable.range(1, PUBLISH_ATTEMPTS), (failure, attempt) -> attempt)
                        .concatMap(attempt -> attempt < PUBLISH_ATTEMPTS
                                ? Flowable.timer(200L << attempt, TimeUnit.MILLISECONDS, RxHelper.scheduler(vertx))
                                : Flowable.error(new IllegalStateException(
                                        "Kafka publish failed " + PUBLISH_ATTEMPTS + " times for version "
                                                + event.version()))))
                .doOnComplete(() -> {
                    metrics.oddsFeedEventsPublishedTotal.incrementAndGet();
                    log.info("{} eventId={} marketId={} version={} partitionKey={}",
                            event.type(), event.eventId(), marketIdOf(event),
                            event.version(), event.partitionKey());
                });
    }

    /** Market id for market-scoped events; match-level events genuinely have none. */
    private static String marketIdOf(SportsEvent event) {
        return switch (event) {
            case com.example.sportsbook.common.OddsUpdatedEvent e -> e.marketId();
            case com.example.sportsbook.common.MarketSuspendedEvent e -> e.marketId();
            case com.example.sportsbook.common.MarketOpenedEvent e -> e.marketId();
            case com.example.sportsbook.common.MarketSettledEvent e -> e.marketId();
            case com.example.sportsbook.common.MatchStartedEvent ignored -> "-";
            case com.example.sportsbook.common.MatchFinishedEvent ignored -> "-";
        };
    }

    @Override
    public Completable rxStop() {
        log.info("SPORTS_PROVIDER_DISCONNECTED {}", metrics.snapshot());
        dispose(pipeline);
        dispose(metricsLogger);
        Completable closeClient = providerClient == null ? Completable.complete() : providerClient.close();
        Completable closePublisher = publisher == null ? Completable.complete() : publisher.close();
        return closeClient.onErrorComplete().andThen(closePublisher.onErrorComplete());
    }

    private void dispose(Disposable disposable) {
        if (disposable != null && !disposable.isDisposed()) {
            disposable.dispose();
        }
    }

    FeedMetrics metrics() {
        return metrics;
    }
}
