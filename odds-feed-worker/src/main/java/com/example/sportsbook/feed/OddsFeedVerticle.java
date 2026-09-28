package com.example.sportsbook.feed;

import com.example.sportsbook.common.SportsEvent;
import com.example.sportsbook.feed.config.FeedConfig;
import com.example.sportsbook.feed.messaging.SportsEventPublisher;
import com.example.sportsbook.feed.provider.MessageNormalizer;
import com.example.sportsbook.feed.provider.ProviderMessage;
import com.example.sportsbook.feed.provider.ProviderMessageValidator;
import com.example.sportsbook.feed.provider.ProviderStreamClient;
import com.example.sportsbook.feed.provider.ReconnectBackoff;
import com.example.sportsbook.feed.provider.SequenceDecision;
import com.example.sportsbook.feed.provider.SequenceValidator;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.disposables.Disposable;
import io.vertx.rxjava3.core.AbstractVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The reactive ingestion pipeline:
 *
 * <pre>
 *   provider stream -> validate -> sequence check -> normalize -> publish to Kafka
 * </pre>
 *
 * <p>The provider stream is a WebSocket the worker dials out to ({@link ProviderStreamClient}),
 * reconnecting with backoff. Nothing here blocks: socket reads and Kafka sends are asynchronous and
 * every stage runs on this verticle's event loop. {@code concatMapCompletable} publishes one record at a
 * time, which both preserves per-partition ordering and propagates backpressure upstream — a slow
 * broker slows consumption of the provider stream rather than growing an unbounded in-flight set.
 */
public class OddsFeedVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(OddsFeedVerticle.class);

    private final FeedConfig config;
    private final FeedMetrics metrics = new FeedMetrics();
    private final SequenceValidator sequenceValidator = new SequenceValidator();

    private SportsEventPublisher publisher;
    private ProviderStreamClient providerClient;
    private Disposable pipeline;
    private Disposable metricsLogger;

    public OddsFeedVerticle(FeedConfig config) {
        this.config = config;
    }

    @Override
    public Completable rxStart() {
        publisher = new SportsEventPublisher(vertx, config.kafkaBootstrapServers(), config.sportsEventsTopic());
        providerClient = new ProviderStreamClient(vertx, config.providerUrl(), config.providerConnectTimeout(),
                new ReconnectBackoff(config.reconnectInitialDelay(), config.reconnectMaxDelay()),
                metrics.oddsFeedMessagesInvalidTotal);
        log.info("ODDS_FEED_PIPELINE_STARTING providerUrl={} topic={} bootstrap={}",
                config.providerUrl(), config.sportsEventsTopic(), config.kafkaBootstrapServers());

        pipeline = providerClient.messages()
                .doOnNext(message -> metrics.oddsFeedMessagesReceivedTotal.incrementAndGet())
                .filter(this::validate)
                .filter(this::acceptSequence)
                .concatMapMaybe(message -> Maybe.fromOptional(MessageNormalizer.normalize(message)))
                .concatMapCompletable(this::publish)
                .subscribe(
                        () -> log.info("PROVIDER_STREAM_COMPLETED {}", metrics.snapshot()),
                        error -> log.error("PROVIDER_STREAM_FAILED {}", metrics.snapshot(), error));

        metricsLogger = vertx.periodicStream(30_000)
                .toFlowable()
                .subscribe(tick -> log.info("FEED_METRICS {}", metrics.snapshot()));

        return Completable.complete();
    }

    private boolean validate(ProviderMessage message) {
        if (ProviderMessageValidator.isValid(message)) {
            return true;
        }
        metrics.oddsFeedMessagesInvalidTotal.incrementAndGet();
        log.warn("PROVIDER_MESSAGE_INVALID sequenceNumber={} messageType={}",
                message.sequenceNumber(), message.messageType());
        return false;
    }

    private boolean acceptSequence(ProviderMessage message) {
        long expected = sequenceValidator.lastProcessedSequence() + 1;
        SequenceDecision decision = sequenceValidator.evaluate(message.sequenceNumber());

        return switch (decision) {
            case IN_ORDER -> true;
            case DUPLICATE -> {
                metrics.oddsFeedDuplicatesTotal.incrementAndGet();
                log.info("PROVIDER_DUPLICATE_IGNORED sequenceNumber={} lastProcessedSequence={}",
                        message.sequenceNumber(), sequenceValidator.lastProcessedSequence());
                yield false;
            }
            case GAP -> {
                metrics.oddsFeedSequenceGapsTotal.incrementAndGet();
                log.warn("SEQUENCE_GAP_DETECTED expectedSequence={} receivedSequence={} missed={}",
                        expected, message.sequenceNumber(), message.sequenceNumber() - expected);
                requestProviderSnapshot(message.sequenceNumber());
                // Still process the message: dropping it would lose real state on top of the gap.
                yield true;
            }
        };
    }

    /**
     * Stands in for the real recovery call. A production worker would request a snapshot or replay
     * from the provider, rebuild state from it, and only then resume streaming.
     */
    private void requestProviderSnapshot(long fromSequence) {
        log.warn("PROVIDER_SNAPSHOT_REQUESTED fromSequence={} note=simulated-resync", fromSequence);
    }

    private Completable publish(SportsEvent event) {
        return publisher.publish(event)
                .doOnComplete(() -> {
                    metrics.oddsFeedEventsPublishedTotal.incrementAndGet();
                    log.info("{} eventId={} marketId={} version={} partitionKey={}",
                            event.type(), event.eventId(), marketIdOf(event),
                            event.version(), event.partitionKey());
                })
                .doOnError(error -> {
                    metrics.oddsFeedPublishFailuresTotal.incrementAndGet();
                    log.error("KAFKA_PUBLISH_FAILED type={} version={}", event.type(), event.version(), error);
                })
                // A single failed send must not tear down the long-lived provider stream.
                .onErrorComplete();
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
