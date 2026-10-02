package com.example.sportsbook.simulator;

import com.example.sportsbook.simulator.config.SimulatorConfig;
import com.example.sportsbook.simulator.feed.AutoDrift;
import com.example.sportsbook.simulator.feed.DriftRange;
import com.example.sportsbook.simulator.feed.ManualMatch;
import com.example.sportsbook.simulator.feed.ManualMatchDirector;
import com.example.sportsbook.simulator.feed.ProviderFeed;
import com.example.sportsbook.simulator.feed.ScriptedMatch;
import com.example.sportsbook.simulator.feed.SequenceReservation;
import com.example.sportsbook.simulator.stream.ProviderStreamHandler;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.RxHelper;
import io.vertx.core.file.CopyOptions;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.http.HttpServer;
import io.vertx.rxjava3.ext.web.Router;
import io.vertx.rxjava3.ext.web.RoutingContext;
import io.vertx.rxjava3.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;

/**
 * The whole simulated provider in one verticle: the outbound WebSocket stream, the scripted
 * looping match, and the {@code /dev} control API, all on one HTTP port.
 *
 * <p>One verticle on purpose. The {@link ProviderFeed} must stamp and emit in strict order, and
 * everything that feeds it — timer ticks, HTTP requests — runs here on this verticle's single
 * context, so no locks are needed anywhere.
 *
 * <p>The {@code /dev} API is unauthenticated: a demo affordance, not an operator API.
 */
public class SimulatorVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(SimulatorVerticle.class);

    /** The dev panel renews every ~2s; a closed tab stops renewing and the drift dies with it. */
    private static final Duration AUTO_DRIFT_LEASE = Duration.ofSeconds(5);

    private final SimulatorConfig config;

    private ProviderFeed feed;
    private ManualMatchDirector director;
    private HttpServer server;
    private SequenceReservation reservation;
    /** The reservation actually on disk; the feed never emits past it. */
    private long durableUpTo;
    /** One write at a time: two overlapping writes could land out of order. */
    private boolean reservationWriteInFlight;
    private Disposable reservationKeeper;
    private ScriptedMatch scripted;
    private Disposable scriptedLoop;
    /** Running auto-drifts by eventId. Plain HashMap: only touched on this verticle's context. */
    private final Map<String, AutoDrift> autoDrifts = new HashMap<>();

    public SimulatorVerticle(SimulatorConfig config) {
        this.config = config;
    }

    @Override
    public Completable rxStart() {
        Clock clock = Clock.systemUTC();
        // The first block is durable BEFORE anything is emitted, so even a crash right after
        // startup restarts above every sequence this run can hand out.
        return readPersistedReservation()
                .map(persisted -> SequenceReservation.startingAt(
                        clock.millis(), persisted, SequenceReservation.DEFAULT_BLOCK))
                .flatMapCompletable(started -> {
                    reservation = started;
                    return persistReservation(started.epoch(), started.reservedUpTo())
                            .doOnComplete(() -> durableUpTo = started.reservedUpTo());
                })
                .andThen(Completable.defer(() -> startFeedAndServer(clock)));
    }

    private Single<Optional<SequenceReservation.Persisted>> readPersistedReservation() {
        return vertx.fileSystem().rxExists(config.sequenceFile())
                .flatMap(exists -> !exists
                        ? Single.just(Optional.<SequenceReservation.Persisted>empty())
                        : vertx.fileSystem().rxReadFile(config.sequenceFile())
                                .map(content -> Optional.of(SequenceReservation.parse(content.toString()))));
    }

    /**
     * Async write to a temp file, then an atomic rename: never blocks the event loop the feed runs
     * on, and a crash mid-write leaves the previous reservation intact instead of a torn file.
     */
    private Completable persistReservation(long epoch, long reservedUpTo) {
        String temp = config.sequenceFile() + ".tmp";
        return vertx.fileSystem()
                .rxWriteFile(temp, Buffer.buffer(SequenceReservation.format(epoch, reservedUpTo)))
                .andThen(vertx.fileSystem().rxMove(temp, config.sequenceFile(),
                        new CopyOptions().setAtomicMove(true).setReplaceExisting(true)))
                .doOnComplete(() -> log.info("SEQUENCE_RESERVED epoch={} reservedUpTo={} file={}",
                        epoch, reservedUpTo, config.sequenceFile()));
    }

    /**
     * Writes the newest proposed reservation if it is ahead of what is on disk, one write at a
     * time, and only then raises the feed's emit limit. A failed write is retried; until one lands
     * the feed keeps emitting inside the old durable block and fails closed at its end.
     */
    private void flushReservation() {
        if (reservationWriteInFlight || reservation.reservedUpTo() <= durableUpTo) {
            return;
        }
        long target = reservation.reservedUpTo();
        reservationWriteInFlight = true;
        persistReservation(reservation.epoch(), target).subscribe(
                () -> {
                    durableUpTo = target;
                    feed.setEmitLimit(target);
                    reservationWriteInFlight = false;
                    flushReservation();
                },
                error -> {
                    reservationWriteInFlight = false;
                    log.error("SEQUENCE_RESERVATION_WRITE_FAILED reservedUpTo={} durableUpTo={} retryInMs=1000",
                            target, durableUpTo, error);
                    vertx.setTimer(1_000, id -> flushReservation());
                });
    }

    private Completable startFeedAndServer(Clock clock) {
        feed = new ProviderFeed(reservation.start(), clock, reservation.epoch());
        feed.setEmitLimit(durableUpTo);
        // Propose the next block while half of the current one is still unused; flushReservation
        // makes it durable before the feed is allowed to use it.
        reservationKeeper = feed.messages().subscribe(message -> {
            reservation.extendIfNeeded(feed.lastSequence());
            flushReservation();
        });
        director = new ManualMatchDirector(feed, java.util.Set.of(config.scriptedMarketId()));
        // Created even with autoplay off, so POST /dev/scripted/start can switch it on later.
        scripted = new ScriptedMatch(config.scriptedEventId(), config.scriptedMarketId());

        if (config.autoplay()) {
            startScriptedLoop();
        } else {
            log.info("SIMULATOR_AUTOPLAY_DISABLED scripted-loop=off dev-panel=on");
        }

        ProviderStreamHandler streamHandler = new ProviderStreamHandler(config.streamPath(), feed);

        return vertx.createHttpServer()
                .requestHandler(devRouter())
                // WebSocket upgrades go here; plain HTTP requests go to the router.
                .webSocketHandler(streamHandler::handle)
                .rxListen(config.port())
                .doOnSuccess(started -> {
                    server = started;
                    log.info("PROVIDER_SIMULATOR_LISTENING port={} streamPath={} sessionEpoch={} firstSequence={}",
                            config.port(), config.streamPath(), feed.sessionEpoch(), feed.lastSequence() + 1);
                })
                .ignoreElement();
    }

    /**
     * {@code periodicStream} fires on this verticle's event loop, so the scripted match and the
     * HTTP handlers never touch the feed concurrently. {@code Flowable.interval} would tick on an
     * RxJava computation thread instead.
     */
    private void startScriptedLoop() {
        scriptedLoop = vertx.periodicStream(config.scriptInterval().toMillis())
                .toFlowable()
                .subscribe(
                        tick -> {
                            try {
                                scripted.playNext(feed);
                            } catch (IllegalStateException refused) {
                                // Keep the loop alive: ScriptedMatch did not advance, so the same
                                // step is retried on the next tick once the feed accepts again.
                                log.warn("SCRIPTED_STEP_DEFERRED reason={}", refused.getMessage());
                            }
                        },
                        error -> log.error("SCRIPTED_LOOP_FAILED", error));
        log.info("SIMULATOR_AUTOPLAY_ENABLED eventId={} marketId={} intervalMs={}",
                config.scriptedEventId(), config.scriptedMarketId(), config.scriptInterval().toMillis());
    }

    private boolean scriptedLoopRunning() {
        return scriptedLoop != null && !scriptedLoop.isDisposed();
    }

    /**
     * Stops whatever is generating traffic for this event id: the scripted loop if it is the
     * scripted event, a running auto-drift if it is a dev match. Stopping is not settling — the
     * event stays LIVE with its last prices, and state-processor's staleness sweep will suspend
     * its market once the prices go stale, exactly as with a real provider that goes quiet.
     */
    private void stopEventTraffic(RoutingContext ctx) {
        String eventId = ctx.pathParam("eventId");
        boolean scriptedEvent = eventId.equals(config.scriptedEventId());
        if (!scriptedEvent && director.find(eventId).isEmpty()) {
            error(ctx, 404, "EVENT_NOT_FOUND", eventId);
            return;
        }
        JsonArray stopped = new JsonArray();
        if (scriptedEvent && scriptedLoopRunning()) {
            scriptedLoop.dispose();
            stopped.add("scripted-loop");
            log.info("SCRIPTED_LOOP_STOPPED eventId={}", eventId);
        }
        if (autoDrifts.containsKey(eventId)) {
            stopAutoDrift(eventId, "event-stop");
            stopped.add("auto-drift");
        }
        ok(ctx, new JsonObject().put("eventId", eventId).put("stopped", stopped));
    }

    /**
     * FIX 4: starts a new provider session. The new session is made durable FIRST and the feed is
     * switched only once that write has landed; switching first and crashing before the write would
     * bring the simulator back on the old epoch after workers had already seen the new one.
     *
     * <p>Refused with 409 while a reservation write is in flight: that write carries the old epoch
     * and could land after ours, putting the old session back on disk. The caller retries.
     */
    private void restartSession(RoutingContext ctx) {
        if (reservationWriteInFlight) {
            ctx.response().setStatusCode(409).putHeader("content-type", "application/json")
                    .end(new JsonObject().put("error", "RESERVATION_WRITE_IN_PROGRESS").encode());
            return;
        }
        long oldEpoch = reservation.epoch();
        SequenceReservation next = reservation.nextSession(SequenceReservation.DEFAULT_BLOCK);
        reservationWriteInFlight = true;
        persistReservation(next.epoch(), next.reservedUpTo()).subscribe(
                () -> {
                    reservation = next;
                    durableUpTo = next.reservedUpTo();
                    feed.startNewSession(next.epoch());
                    feed.setEmitLimit(durableUpTo);
                    reservationWriteInFlight = false;
                    log.warn("PROVIDER_SESSION_RESTARTED oldEpoch={} newEpoch={} nextSequence=1", oldEpoch, next.epoch());
                    ok(ctx, new JsonObject().put("sessionEpoch", next.epoch()).put("nextSequence", 1));
                },
                error -> {
                    reservationWriteInFlight = false;
                    log.error("PROVIDER_SESSION_RESTART_FAILED oldEpoch={}", oldEpoch, error);
                    ctx.response().setStatusCode(500).putHeader("content-type", "application/json")
                            .end(new JsonObject().put("error", "SESSION_RESTART_FAILED").encode());
                });
    }

    private JsonObject scriptedStatus() {
        return new JsonObject()
                .put("eventId", config.scriptedEventId())
                .put("marketId", config.scriptedMarketId())
                .put("running", scriptedLoopRunning())
                .put("intervalMs", config.scriptInterval().toMillis());
    }

    private Router devRouter() {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        router.get("/dev/health").handler(ctx -> ok(ctx, new JsonObject()
                .put("status", "UP")
                .put("sessionEpoch", feed.sessionEpoch())
                .put("lastSequence", feed.lastSequence())
                .put("scripted", scriptedStatus())));

        // FIX 4: act like a provider that restarted — new session epoch, numbering from 1 again.
        router.post("/dev/session/restart").handler(this::restartSession);

        // Stop all generated traffic for one event, scripted or dev match.
        router.post("/dev/events/:eventId/stop").handler(this::stopEventTraffic);
        router.get("/dev/scripted").handler(ctx -> ok(ctx, scriptedStatus()));
        router.post("/dev/scripted/start").handler(ctx -> {
            if (!scriptedLoopRunning()) {
                startScriptedLoop();
            }
            ok(ctx, scriptedStatus());
        });
        router.get("/dev/matches").handler(this::listMatches);
        router.post("/dev/matches").handler(this::createMatch);
        router.post("/dev/matches/:eventId/odds").handler(ctx -> withMatch(ctx, match -> {
            director.driftOdds(match);
            log.info("DEV_ODDS_DRIFTED eventId={}", match.eventId());
            ok(ctx, describe(match));
        }));
        router.post("/dev/matches/:eventId/suspend").handler(ctx -> withMatch(ctx, match -> {
            director.suspend(match);
            ok(ctx, describe(match).put("marketStatus", "SUSPENDED"));
        }));
        router.post("/dev/matches/:eventId/open").handler(ctx -> withMatch(ctx, match -> {
            director.open(match);
            ok(ctx, describe(match).put("marketStatus", "ACTIVE"));
        }));
        router.post("/dev/matches/:eventId/settle").handler(this::settleMatch);
        router.post("/dev/matches/:eventId/auto-drift").handler(this::startAutoDrift);
        router.delete("/dev/matches/:eventId/auto-drift").handler(ctx -> withMatch(ctx, match -> {
            stopAutoDrift(match.eventId(), "requested");
            ok(ctx, describe(match));
        }));
        return router;
    }

    private void listMatches(RoutingContext ctx) {
        JsonArray array = new JsonArray();
        director.all().forEach(match -> array.add(describe(match)));
        ok(ctx, new JsonObject().put("matches", array));
    }

    /**
     * Body is optional: {@code {"eventId": "derby-1"}} creates the match under that id, an absent
     * or blank id generates one. 400 for a malformed id, 409 if the id is already in use —
     * including the scripted event's, which would interleave two scripts on one key.
     */
    private void createMatch(RoutingContext ctx) {
        String requested;
        try {
            JsonObject body = ctx.body() == null || ctx.body().length() == 0 ? null : ctx.body().asJsonObject();
            requested = body == null ? null : body.getString("eventId");
        } catch (RuntimeException malformed) {
            error(ctx, 400, "INVALID_BODY", null);
            return;
        }
        requested = requested == null || requested.isBlank() ? null : requested.trim();

        ManualMatch match;
        try {
            if (requested != null && requested.equals(config.scriptedEventId())) {
                throw new IllegalStateException(requested + " is the scripted event");
            }
            match = requested == null ? director.startRandomMatch() : director.startMatch(requested);
        } catch (IllegalArgumentException invalid) {
            ctx.response().setStatusCode(400).putHeader("content-type", "application/json")
                    .end(new JsonObject().put("error", "INVALID_EVENT_ID")
                            .put("message", invalid.getMessage()).encode());
            return;
        } catch (IllegalStateException taken) {
            ctx.response().setStatusCode(409).putHeader("content-type", "application/json")
                    .end(new JsonObject().put("error", "EVENT_ID_IN_USE").put("eventId", requested)
                            .put("message", taken.getMessage()).encode());
            return;
        }
        log.info("DEV_MATCH_CREATED eventId={} marketId={}", match.eventId(), match.marketId());
        ok(ctx, describe(match));
    }

    private void settleMatch(RoutingContext ctx) {
        withMatch(ctx, match -> {
            JsonObject body = ctx.body() == null ? null : ctx.body().asJsonObject();
            String requested = body == null ? null : body.getString("winningSelectionId");
            // Stop first: a drift after MARKET_RESULT would publish prices above the settlement.
            stopAutoDrift(match.eventId(), "settled");
            String winner = director.settle(match, requested);
            log.info("DEV_MATCH_SETTLED eventId={} winningSelectionId={}", match.eventId(), winner);
            ok(ctx, describe(match).put("winningSelectionId", winner));
        });
    }

    /**
     * Starts auto-drift, or renews it if already running. Renewal extends the lease and applies
     * the range from the next gap on, without restarting the loop, so the panel can call this
     * repeatedly as its heartbeat.
     *
     * <p>Body: {@code {"minMs": 10, "maxMs": 1000}}.
     */
    private void startAutoDrift(RoutingContext ctx) {
        withMatch(ctx, match -> {
            if (match.isSettled()) {
                error(ctx, 409, "MATCH_SETTLED", match.eventId());
                return;
            }
            DriftRange range;
            try {
                JsonObject body = ctx.body() == null ? null : ctx.body().asJsonObject();
                if (body == null) {
                    throw new IllegalArgumentException("body {\"minMs\":..,\"maxMs\":..} is required");
                }
                long min = body.getLong("minMs", 1000L);
                range = new DriftRange(min, body.getLong("maxMs", min));
            } catch (RuntimeException invalid) {
                ctx.response().setStatusCode(400).putHeader("content-type", "application/json")
                        .end(new JsonObject().put("error", "INVALID_DRIFT_RANGE")
                                .put("message", invalid.getMessage()).encode());
                return;
            }

            AutoDrift running = autoDrifts.get(match.eventId());
            if (running != null) {
                running.renew(range);
            } else {
                String eventId = match.eventId();
                autoDrifts.put(eventId, new AutoDrift(range, AUTO_DRIFT_LEASE, RxHelper.scheduler(vertx),
                        () -> director.driftOdds(match),
                        () -> {
                            autoDrifts.remove(eventId);
                            log.info("DEV_AUTO_DRIFT_STOPPED eventId={} reason=lease-expired", eventId);
                        }));
                log.info("DEV_AUTO_DRIFT_STARTED eventId={} minMs={} maxMs={}",
                        eventId, range.minMs(), range.maxMs());
            }
            ok(ctx, describe(match));
        });
    }

    private void stopAutoDrift(String eventId, String reason) {
        AutoDrift running = autoDrifts.remove(eventId);
        if (running != null) {
            running.dispose();
            log.info("DEV_AUTO_DRIFT_STOPPED eventId={} reason={}", eventId, reason);
        }
    }

    /** Resolves the path's match or answers 404, so each handler stays focused. */
    private void withMatch(RoutingContext ctx, Consumer<ManualMatch> action) {
        String eventId = ctx.pathParam("eventId");
        director.find(eventId).ifPresentOrElse(action, () -> error(ctx, 404, "MATCH_NOT_FOUND", eventId));
    }

    private void error(RoutingContext ctx, int status, String code, String eventId) {
        ctx.response().setStatusCode(status).putHeader("content-type", "application/json")
                .end(new JsonObject().put("error", code).put("eventId", eventId).encode());
    }

    private JsonObject describe(ManualMatch match) {
        JsonArray selections = new JsonArray();
        for (Map.Entry<String, BigDecimal> entry : match.odds().entrySet()) {
            selections.add(new JsonObject()
                    .put("selectionId", entry.getKey())
                    .put("odds", entry.getValue()));
        }
        return new JsonObject()
                .put("eventId", match.eventId())
                .put("marketId", match.marketId())
                .put("settled", match.isSettled())
                .put("selections", selections)
                .put("autoDrift", autoDriftOf(match.eventId()));
    }

    private JsonObject autoDriftOf(String eventId) {
        AutoDrift running = autoDrifts.get(eventId);
        return running == null ? null : new JsonObject()
                .put("minMs", running.range().minMs())
                .put("maxMs", running.range().maxMs())
                .put("leaseMs", AUTO_DRIFT_LEASE.toMillis());
    }

    private void ok(RoutingContext ctx, JsonObject body) {
        ctx.response().putHeader("content-type", "application/json").end(body.encode());
    }

    @Override
    public Completable rxStop() {
        if (scriptedLoop != null && !scriptedLoop.isDisposed()) {
            scriptedLoop.dispose();
        }
        List.copyOf(autoDrifts.keySet()).forEach(eventId -> stopAutoDrift(eventId, "shutdown"));
        if (reservationKeeper != null) {
            reservationKeeper.dispose();
        }
        if (feed != null) {
            feed.complete();
        }
        return server == null ? Completable.complete() : server.rxClose().onErrorComplete();
    }
}
