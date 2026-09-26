package com.example.sportsbook.realtime;

import com.example.sportsbook.common.SportsbookJson;
import com.example.sportsbook.realtime.config.GatewayConfig;
import com.example.sportsbook.realtime.messaging.SportsEventConsumer;
import com.example.sportsbook.realtime.subscription.ClientCommand;
import com.example.sportsbook.realtime.subscription.CommandParser;
import com.example.sportsbook.realtime.subscription.SubscriptionRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.http.ServerWebSocket;
import io.vertx.rxjava3.core.http.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * WebSocket gateway implementing the STREAM half of SNAPSHOT + STREAM.
 *
 * <p>Clients fetch initial state from odds-service over REST, then subscribe here for subsequent
 * updates. Each event is delivered only to the sockets subscribed to that {@code eventId}.
 */
public class RealtimeGatewayVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(RealtimeGatewayVerticle.class);
    private static final String STREAM_PATH = "/api/v1/stream";

    private final GatewayConfig config;
    private final GatewayMetrics metrics = new GatewayMetrics();
    private final SubscriptionRegistry<ServerWebSocket> registry = new SubscriptionRegistry<>();

    private SportsEventConsumer consumer;
    private HttpServer server;
    private Disposable metricsLogger;

    public RealtimeGatewayVerticle(GatewayConfig config) {
        this.config = config;
    }

    @Override
    public Completable rxStart() {
        consumer = new SportsEventConsumer(vertx, config.kafkaBootstrapServers(),
                config.consumerGroupId(), config.sportsEventsTopic());
        consumer.start(this::fanOut);

        metricsLogger = io.reactivex.rxjava3.core.Observable
                .interval(30, 30, TimeUnit.SECONDS)
                .subscribe(tick -> log.info("GATEWAY_METRICS {} subscribedEvents={}",
                        metrics.snapshot(), registry.eventCount()));

        return vertx.createHttpServer()
                .webSocketHandler(this::handleSocket)
                .rxListen(config.port())
                .doOnSuccess(httpServer -> {
                    server = httpServer;
                    log.info("REALTIME_GATEWAY_LISTENING port={} path={}", config.port(), STREAM_PATH);
                })
                .ignoreElement();
    }

    private void handleSocket(ServerWebSocket socket) {
        if (!STREAM_PATH.equals(socket.path())) {
            socket.reject();
            return;
        }
        metrics.websocketConnections.incrementAndGet();
        log.info("WEBSOCKET_CONNECTED remote={} connections={}",
                socket.remoteAddress(), metrics.websocketConnections.get());

        socket.textMessageHandler(frame -> onClientFrame(socket, frame));
        socket.closeHandler(closed -> onClose(socket));
        socket.exceptionHandler(error -> {
            log.warn("WEBSOCKET_ERROR remote={}", socket.remoteAddress(), error);
            onClose(socket);
        });
    }

    private void onClientFrame(ServerWebSocket socket, String frame) {
        Optional<ClientCommand> parsed = CommandParser.parse(frame);
        if (parsed.isEmpty()) {
            metrics.websocketBadFramesTotal.incrementAndGet();
            log.warn("WEBSOCKET_BAD_FRAME remote={}", socket.remoteAddress());
            send(socket, "{\"error\":\"INVALID_COMMAND\"}");
            return;
        }
        ClientCommand command = parsed.get();
        switch (command.action()) {
            case SUBSCRIBE -> {
                registry.subscribe(command.eventId(), socket);
                log.info("SUBSCRIPTION_REGISTERED eventId={} subscribers={}",
                        command.eventId(), registry.subscribersOf(command.eventId()).size());
                send(socket, "{\"action\":\"SUBSCRIBED\",\"eventId\":\"" + command.eventId() + "\"}");
            }
            case UNSUBSCRIBE -> {
                registry.unsubscribe(command.eventId(), socket);
                log.info("SUBSCRIPTION_REMOVED eventId={}", command.eventId());
                send(socket, "{\"action\":\"UNSUBSCRIBED\",\"eventId\":\"" + command.eventId() + "\"}");
            }
        }
    }

    private void onClose(ServerWebSocket socket) {
        registry.remove(socket);
        metrics.websocketConnections.decrementAndGet();
        log.info("WEBSOCKET_DISCONNECTED connections={}", metrics.websocketConnections.get());
    }

    /**
     * Routes one Kafka payload to the sockets subscribed to its event.
     *
     * <p>Only {@code eventId} is extracted; the payload is forwarded verbatim.
     */
    private void fanOut(String payload) {
        metrics.sportsEventsConsumedTotal.incrementAndGet();
        String eventId = extractEventId(payload);
        if (eventId == null) {
            log.warn("EVENT_WITHOUT_EVENT_ID_SKIPPED");
            return;
        }
        var subscribers = registry.subscribersOf(eventId);
        if (subscribers.isEmpty()) {
            return;
        }
        for (ServerWebSocket socket : subscribers) {
            send(socket, payload);
        }
        log.debug("EVENT_FANNED_OUT eventId={} subscribers={}", eventId, subscribers.size());
    }

    private String extractEventId(String payload) {
        try {
            JsonNode node = SportsbookJson.mapper().readTree(payload);
            JsonNode eventId = node.get("eventId");
            return eventId == null || eventId.isNull() ? null : eventId.asText();
        } catch (Exception malformed) {
            log.warn("SPORTS_EVENT_UNPARSEABLE length={}", payload == null ? 0 : payload.length());
            return null;
        }
    }

    private void send(ServerWebSocket socket, String text) {
        if (socket.isClosed()) {
            return;
        }
        socket.writeTextMessage(text)
                .subscribe(
                        () -> metrics.websocketMessagesSentTotal.incrementAndGet(),
                        error -> log.debug("WEBSOCKET_SEND_FAILED", error));
    }

    @Override
    public Completable rxStop() {
        log.info("REALTIME_GATEWAY_STOPPING {}", metrics.snapshot());
        if (metricsLogger != null && !metricsLogger.isDisposed()) {
            metricsLogger.dispose();
        }
        Completable closeConsumer = consumer == null ? Completable.complete() : consumer.close();
        Completable closeServer = server == null ? Completable.complete() : server.rxClose();
        return closeConsumer.onErrorComplete().andThen(closeServer.onErrorComplete());
    }

    SubscriptionRegistry<ServerWebSocket> registry() {
        return registry;
    }
}
