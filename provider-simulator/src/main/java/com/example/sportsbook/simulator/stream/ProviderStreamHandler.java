package com.example.sportsbook.simulator.stream;

import com.example.sportsbook.simulator.feed.ProviderFeed;
import io.reactivex.rxjava3.disposables.Disposable;
import com.example.sportsbook.simulator.wire.ProviderJson;
import io.vertx.core.json.Json;
import io.vertx.rxjava3.core.http.ServerWebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streams the {@link ProviderFeed} to every connected worker over WebSocket.
 *
 * <p>STEP 2 — yours to implement. Until then every connection is closed immediately.
 *
 * <p>Wire format: one {@code ProviderMessage} per text frame, encoded with
 * {@link com.example.sportsbook.simulator.wire.ProviderJson#encode}.
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
        if(!streamPath.equals(socket.path())) {
            log.warn("PROVIDER_STREAM_PATH_REJECTED remote={} path={}",
                    socket.remoteAddress(), socket.path());
            socket.reject();
            return;
        }

        String remote = String.valueOf(socket.remoteAddress());
        log.info("PROVIDER_STREAM_CONNECTED remote={}", remote);

        // 2 Subscribe THIS socket to the hot feed
        Disposable subscription = feed.messages().subscribe(
                message ->{

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
                    socket.writeTextMessage(ProviderJson.encode(message))
                            .subscribe(
                                    () -> { },
                                    error -> log.debug("PROVIDER_STREAM_WRITE_FAILED remote={}", remote, error));
                },
                // 6 The feed itself failed (should never happen)
                error -> log.error("PROVIDER_STREAM_FEED_FAILED remote={}", remote, error));

        // 7 Cleanup: stop pushing to this socket once it is closed
        socket.closeHandler(closed -> {
            subscription.dispose();
            log.info("PROVIDER_STREAM_DISCONNECTED remote={}", remote);
        });

        // Network errors: log only; Vert.x closes the socket, so ⑧ runs
        socket.exceptionHandler(error ->
                log.warn("PROVIDER_STREAM_SOCKET_ERROR remote={}", remote, error));
    }
}
