package com.example.sportsbook.feed.provider;

import com.example.sportsbook.common.SportsbookJson;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Scheduler;
import io.vertx.core.http.WebSocketClientOptions;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.rxjava3.core.RxHelper;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.http.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Connects out to the provider's WebSocket feed and exposes it as one endless
 * {@code Flowable<ProviderMessage>} that survives disconnects.
 *
 * <pre>
 *   connect -> frames -> decode -> ... socket closes/fails -> wait (backoff) -> connect again
 * </pre>
 *
 * <p>Backpressure is real: {@code WebSocket.toFlowable()} pauses the socket when downstream stops
 * requesting, so a slow Kafka stops us reading and TCP flow control pushes back on the provider,
 * instead of frames piling up in memory. The provider decides what to do with a worker that falls
 * behind (the simulator disconnects it).
 *
 * <p><b>Resume:</b> every (re)connect asks for {@code ?fromSequence=<last delivered + 1>}, so the
 * provider replays what we missed while disconnected before live traffic resumes. With nothing
 * delivered yet it asks for {@code fromSequence=0}: everything the provider still holds. Whatever the
 * provider can no longer replay still shows up in the worker's {@link SequenceValidator} as a gap,
 * which is why the validator must NOT be reset per connection.
 *
 * <p>The cursor lives in memory only: a restarted worker resumes from live, not from where the
 * previous process stopped.
 *
 * <p>Timers run on the Vert.x context scheduler, so every stage stays on the verticle's event loop.
 */
public class ProviderStreamClient {

    private static final Logger log = LoggerFactory.getLogger(ProviderStreamClient.class);

    private final WebSocketClient client;
    private final String providerUrl;
    private final ReconnectBackoff backoff;
    private final Scheduler scheduler;
    private final AtomicLong unparseableFrames;
    private final LongSupplier lastProcessedSequence;

    /**
     * @param connectTimeout bounds each connect attempt. Without it, dialing a provider whose host
     *                       has vanished waits out TCP's default (~60s in Vert.x) before the
     *                       backoff even starts.
     * @param lastProcessedSequence read on every connect to build the resume cursor; negative
     *                              means nothing delivered yet, so replay from the start
     */
    public ProviderStreamClient(Vertx vertx, String providerUrl, Duration connectTimeout,
                                ReconnectBackoff backoff, AtomicLong unparseableFrames,
                                LongSupplier lastProcessedSequence) {
        this.client = vertx.createWebSocketClient(
                new WebSocketClientOptions().setConnectTimeout((int) connectTimeout.toMillis()));
        this.providerUrl = providerUrl;
        this.backoff = backoff;
        this.scheduler = RxHelper.scheduler(vertx);
        this.unparseableFrames = unparseableFrames;
        this.lastProcessedSequence = lastProcessedSequence;
    }

    /** Cold: each subscription opens its own connection and keeps reconnecting until disposed. */
    public Flowable<ProviderMessage> messages() {
        return Flowable.defer(this::connectOnce)
                .retryWhen(failures -> failures.concatMap(failure -> {
                    long delay = backoff.nextDelayMillis();
                    log.warn("PROVIDER_DISCONNECTED url={} reason={} attempt={} retryInMs={}",
                            providerUrl, failure.getMessage(), backoff.consecutiveFailures(), delay);
                    return Flowable.timer(delay, TimeUnit.MILLISECONDS, scheduler);
                }));
    }

    /**
     * One connection's worth of messages. A normal close is turned into an error on purpose:
     * for a feed that should never end, "the provider hung up" is a failure to recover from, and
     * {@code retryWhen} only reacts to errors.
     */
    private Flowable<ProviderMessage> connectOnce() {
        String url = resumeUrl(providerUrl, lastProcessedSequence.getAsLong());
        return client.rxConnect(new WebSocketConnectOptions().setAbsoluteURI(url))
                .doOnSuccess(socket -> {
                    backoff.reset();
                    log.info("PROVIDER_CONNECTED url={}", url);
                })
                // One text frame = one provider message. Messages are far below the max frame
                // size, so the provider never fragments them.
                .flatMapPublisher(socket -> socket.toFlowable()
                        .concatMapMaybe(this::decode)
                        .concatWith(Flowable.error(
                                new IllegalStateException("provider closed the stream")))
                        // Cancelling a ReadStream only stops reading; it does not close the socket.
                        // When the pipeline restarts, drop this connection before the next one opens.
                        .doOnCancel(() -> socket.close().subscribe(() -> { }, error -> { })));
    }

    /**
     * Always carries a cursor. Asking for live-only would silently drop whatever the provider sent
     * before this worker first connected, a settlement included.
     */
    static String resumeUrl(String providerUrl, long lastDelivered) {
        long from = lastDelivered < 0 ? 0 : lastDelivered + 1;
        return providerUrl + (providerUrl.contains("?") ? "&" : "?") + "fromSequence=" + from;
    }

    /** A malformed frame is logged and skipped; it must not tear down the connection. */
    private Maybe<ProviderMessage> decode(Buffer frame) {
        try {
            return Maybe.just(SportsbookJson.mapper().readValue(frame.toString(), ProviderMessage.class));
        } catch (Exception malformed) {
            unparseableFrames.incrementAndGet();
            log.warn("PROVIDER_FRAME_UNPARSEABLE length={}", frame.length());
            return Maybe.empty();
        }
    }

    public Completable close() {
        return client.rxClose();
    }
}
