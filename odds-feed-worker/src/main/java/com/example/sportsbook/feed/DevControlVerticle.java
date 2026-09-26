package com.example.sportsbook.feed;

import com.example.sportsbook.feed.config.FeedConfig;
import com.example.sportsbook.feed.messaging.SportsEventPublisher;
import com.example.sportsbook.feed.provider.ManualMatch;
import com.example.sportsbook.feed.provider.ManualMatchDirector;
import io.reactivex.rxjava3.core.Completable;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.http.HttpServer;
import io.vertx.rxjava3.ext.web.Router;
import io.vertx.rxjava3.ext.web.RoutingContext;
import io.vertx.rxjava3.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Control API for the demo panel: create a match, move its odds, suspend/open it, settle it.
 *
 * <p>Exists so the demo does not depend on catching the ~4-second bettable window of the scripted
 * loop. Everything it emits goes onto {@code sports-events} through the normal publisher, so the
 * rest of the system cannot distinguish it from provider traffic.
 *
 * <p>Unauthenticated and clearly namespaced under {@code /dev} — it is a demo affordance, not an
 * operator API, and would not exist in a real deployment.
 */
public class DevControlVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(DevControlVerticle.class);

    private final FeedConfig config;
    private ManualMatchDirector director;
    private SportsEventPublisher publisher;
    private HttpServer server;

    public DevControlVerticle(FeedConfig config) {
        this.config = config;
    }

    @Override
    public Completable rxStart() {
        publisher = new SportsEventPublisher(vertx, config.kafkaBootstrapServers(), config.sportsEventsTopic());
        director = new ManualMatchDirector(publisher);

        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        router.get("/dev/health").handler(ctx -> ok(ctx, new JsonObject().put("status", "UP")));
        router.get("/dev/matches").handler(this::listMatches);
        router.post("/dev/matches").handler(this::createMatch);
        router.post("/dev/matches/:eventId/odds").handler(this::driftOdds);
        router.post("/dev/matches/:eventId/suspend").handler(this::suspendMarket);
        router.post("/dev/matches/:eventId/open").handler(this::openMarket);
        router.post("/dev/matches/:eventId/settle").handler(this::settleMatch);

        return vertx.createHttpServer()
                .requestHandler(router)
                .rxListen(config.devControlPort())
                .doOnSuccess(started -> {
                    server = started;
                    log.info("DEV_CONTROL_LISTENING port={}", config.devControlPort());
                })
                .ignoreElement();
    }

    private void listMatches(RoutingContext ctx) {
        JsonArray array = new JsonArray();
        director.all().forEach(match -> array.add(describe(match)));
        ok(ctx, new JsonObject().put("matches", array));
    }

    private void createMatch(RoutingContext ctx) {
        director.startRandomMatch().subscribe(
                match -> {
                    log.info("DEV_MATCH_CREATED eventId={} marketId={}", match.eventId(), match.marketId());
                    ok(ctx, describe(match));
                },
                error -> fail(ctx, error));
    }

    private void driftOdds(RoutingContext ctx) {
        withMatch(ctx, match -> director.driftOdds(match).subscribe(
                updated -> {
                    log.info("DEV_ODDS_DRIFTED eventId={}", updated.eventId());
                    ok(ctx, describe(updated));
                },
                error -> fail(ctx, error)));
    }

    private void suspendMarket(RoutingContext ctx) {
        withMatch(ctx, match -> director.suspend(match).subscribe(
                () -> ok(ctx, describe(match).put("marketStatus", "SUSPENDED")),
                error -> fail(ctx, error)));
    }

    private void openMarket(RoutingContext ctx) {
        withMatch(ctx, match -> director.open(match).subscribe(
                () -> ok(ctx, describe(match).put("marketStatus", "ACTIVE")),
                error -> fail(ctx, error)));
    }

    private void settleMatch(RoutingContext ctx) {
        withMatch(ctx, match -> {
            JsonObject body = ctx.body() == null ? null : ctx.body().asJsonObject();
            String requested = body == null ? null : body.getString("winningSelectionId");
            director.settle(match, requested).subscribe(
                    winner -> {
                        log.info("DEV_MATCH_SETTLED eventId={} winningSelectionId={}",
                                match.eventId(), winner);
                        ok(ctx, describe(match).put("winningSelectionId", winner));
                    },
                    error -> fail(ctx, error));
        });
    }

    /** Resolves the path's match or answers 404, so each handler stays focused. */
    private void withMatch(RoutingContext ctx, java.util.function.Consumer<ManualMatch> action) {
        String eventId = ctx.pathParam("eventId");
        director.find(eventId).ifPresentOrElse(action, () ->
                ctx.response().setStatusCode(404).putHeader("content-type", "application/json")
                        .end(new JsonObject()
                                .put("error", "MATCH_NOT_FOUND")
                                .put("eventId", eventId).encode()));
    }

    private JsonObject describe(ManualMatch match) {
        JsonArray selections = new JsonArray();
        for (Map.Entry<String, java.math.BigDecimal> entry : match.odds().entrySet()) {
            selections.add(new JsonObject()
                    .put("selectionId", entry.getKey())
                    .put("odds", entry.getValue()));
        }
        return new JsonObject()
                .put("eventId", match.eventId())
                .put("marketId", match.marketId())
                .put("settled", match.isSettled())
                .put("selections", selections);
    }

    private void ok(RoutingContext ctx, JsonObject body) {
        ctx.response().putHeader("content-type", "application/json").end(body.encode());
    }

    private void fail(RoutingContext ctx, Throwable error) {
        log.error("DEV_CONTROL_FAILED path={}", ctx.request().path(), error);
        ctx.response().setStatusCode(500).putHeader("content-type", "application/json")
                .end(new JsonObject().put("error", String.valueOf(error.getMessage())).encode());
    }

    @Override
    public Completable rxStop() {
        Completable closePublisher = publisher == null ? Completable.complete() : publisher.close();
        Completable closeServer = server == null ? Completable.complete() : server.rxClose();
        return closeServer.onErrorComplete().andThen(closePublisher.onErrorComplete());
    }
}
