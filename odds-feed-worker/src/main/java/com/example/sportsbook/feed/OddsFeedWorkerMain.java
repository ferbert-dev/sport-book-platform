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

        // The control API always runs; the scripted loop is optional so the dev panel can
        // drive matches without competing background traffic.
        vertx.rxDeployVerticle(new DevControlVerticle(config)).subscribe(
                deploymentId -> log.info("DEV_CONTROL_STARTED deploymentId={}", deploymentId),
                error -> log.error("DEV_CONTROL_START_FAILED", error));

        if (config.autoplay()) {
            vertx.rxDeployVerticle(new OddsFeedVerticle(config)).subscribe(
                    deploymentId -> log.info("ODDS_FEED_WORKER_STARTED deploymentId={}", deploymentId),
                    error -> {
                        log.error("ODDS_FEED_WORKER_START_FAILED", error);
                        vertx.close();
                    });
        } else {
            log.info("FEED_AUTOPLAY_DISABLED scripted-loop=off dev-panel=on");
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("ODDS_FEED_WORKER_STOPPING");
            vertx.close().blockingAwait();
        }));
    }
}
