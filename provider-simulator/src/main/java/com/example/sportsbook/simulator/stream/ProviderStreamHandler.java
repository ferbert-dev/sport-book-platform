package com.example.sportsbook.simulator.stream;

import com.example.sportsbook.simulator.feed.ProviderFeed;
import com.example.sportsbook.simulator.wire.ProviderJson;
import com.example.sportsbook.simulator.wire.ProviderMessage;
import io.reactivex.rxjava3.disposables.Disposable;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.vertx.rxjava3.core.http.ServerWebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streams the {@link ProviderFeed} to every connected worker over WebSocket.
 *
 * <p>Wire format: one {@code ProviderMessage} per text frame, encoded with
 * {@link ProviderJson#encode}.
 *
 * <p><b>Resume:</b> a worker reconnecting with {@code ?sessionEpoch=E&fromSequence=N} first gets
 * every buffered message from position {@code (E, N)} on — the rest of session E and every later
 * session — then the live stream. Without {@code sessionEpoch} the current session is assumed;
 * without {@code fromSequence} it gets live traffic only.
 */
public class ProviderStreamHandler {

    private static final Logger log = LoggerFactory.getLogger(ProviderStreamHandler.class);

    private final String streamPath;
    private final ProviderFeed feed;

    public ProviderStreamHandler(String streamPath, ProviderFeed feed) {
        this.streamPath = streamPath;
        this.feed = feed;
    }

    public void handle(ServerWebSocket socket) {
        // 1 Guard: only our stream path may connect
        if (!streamPath.equals(socket.path())) {
            log.warn("PROVIDER_STREAM_PATH_REJECTED remote={} path={}",
                    socket.remoteAddress(), socket.path());
            socket.reject();
            return;
        }

        String remote = String.valueOf(socket.remoteAddress());
        Long fromSequence = longParam(socket.query(), "fromSequence");
        // A worker that sends no epoch (or a client like websocat) resumes within the current session.
        Long requestedEpoch = longParam(socket.query(), "sessionEpoch");
        long fromEpoch = requestedEpoch != null ? requestedEpoch : feed.sessionEpoch();
        log.info("PROVIDER_STREAM_CONNECTED remote={} sessionEpoch={} fromSequence={} currentEpoch={}",
                remote, requestedEpoch, fromSequence, feed.sessionEpoch());

        // Complete the handshake now, so the replay can be written from inside this handler.
        socket.accept();

        // Replay and live subscription happen in this one event-loop turn: nothing can be
        // emitted in between, so the worker sees missed messages then live ones, no hole, no overlap.
        if (fromSequence != null) {
            ProviderFeed.Replay replay = feed.replayFrom(fromEpoch, fromSequence);
            replay.messages().forEach(message -> write(socket, message, remote));
            if (replay.complete()) {
                log.info("PROVIDER_STREAM_REPLAYED remote={} fromEpoch={} fromSequence={} messages={}",
                        remote, fromEpoch, fromSequence, replay.messages().size());
            } else {
                log.warn("PROVIDER_STREAM_REPLAY_INCOMPLETE remote={} fromEpoch={} fromSequence={} messages={} "
                        + "note=range-left-replay-window", remote, fromEpoch, fromSequence, replay.messages().size());
            }
        }

        // 2 Subscribe THIS socket to the hot feed
        Disposable subscription = feed.messages().subscribe(
                message -> {

                    // 3 Socket already closing? Ignore late messages
                    if (socket.isClosed()) {
                        return;
                    }
                    // 4 Slow worker policy: disconnect, never buffer
                    if (socket.writeQueueFull()) {
                        log.warn("PROVIDER_STREAM_SLOW_CONSUMER_DISCONNECTED remote={} sequence={}",
                                remote, message.sequenceNumber());
                        socket.close((short) 1013, "slow consumer")
                                .subscribe(() -> { }, error -> { });
                        return;
                    }
                    // 5 One message = one text frame
                    write(socket, message, remote);
                },
                // 6 The feed itself failed (should never happen)
                error -> log.error("PROVIDER_STREAM_FEED_FAILED remote={}", remote, error));

        // 7 Cleanup: stop pushing to this socket once it is closed
        socket.closeHandler(closed -> {
            subscription.dispose();
            log.info("PROVIDER_STREAM_DISCONNECTED remote={}", remote);
        });

        // Network errors: log only; Vert.x closes the socket, so 7 runs
        socket.exceptionHandler(error ->
                log.warn("PROVIDER_STREAM_SOCKET_ERROR remote={}", remote, error));
    }

    private void write(ServerWebSocket socket, ProviderMessage message, String remote) {
        socket.writeTextMessage(ProviderJson.encode(message))
                .subscribe(
                        () -> { },
                        error -> log.debug("PROVIDER_STREAM_WRITE_FAILED remote={}", remote, error));
    }

    /** {@code null} when the query parameter is absent or not a number. */
    private static Long longParam(String query, String name) {
        if (query == null) {
            return null;
        }
        var values = new QueryStringDecoder("?" + query).parameters().get(name);
        try {
            return values == null || values.isEmpty() ? null : Long.parseLong(values.get(0));
        } catch (NumberFormatException malformed) {
            return null;
        }
    }
}
