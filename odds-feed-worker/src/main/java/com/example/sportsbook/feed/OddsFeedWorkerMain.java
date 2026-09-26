package com.example.sportsbook.feed;

import com.example.sportsbook.feed.config.FeedConfig;
import io.vertx.rxjava3.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Entry point for the shaded executable JAR. No Spring anywhere in this module. */
public final class OddsFeedWorkerMain {

    private static final Logger log = LoggerFactory.getLogger(OddsFeedWorkerMain.class);

    private OddsFeedWorkerMain() {
    }

    public static void main(String[] args) {
        FeedConfig config = FeedConfig.fromEnv();
        Vertx vertx = Vertx.vertx();

        vertx.rxDeployVerticle(new OddsFeedVerticle(config)).subscribe(
                deploymentId -> log.info("ODDS_FEED_WORKER_STARTED deploymentId={}", deploymentId),
                error -> {
                    log.error("ODDS_FEED_WORKER_START_FAILED", error);
                    vertx.close();
                });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("ODDS_FEED_WORKER_STOPPING");
            vertx.close().blockingAwait();
        }));
    }
}
