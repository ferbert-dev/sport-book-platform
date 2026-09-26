package com.example.sportsbook.realtime;

import com.example.sportsbook.realtime.config.GatewayConfig;
import io.vertx.rxjava3.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Entry point for the shaded executable JAR. No Spring anywhere in this module. */
public final class RealtimeGatewayMain {

    private static final Logger log = LoggerFactory.getLogger(RealtimeGatewayMain.class);

    private RealtimeGatewayMain() {
    }

    public static void main(String[] args) {
        GatewayConfig config = GatewayConfig.fromEnv();
        Vertx vertx = Vertx.vertx();

        vertx.rxDeployVerticle(new RealtimeGatewayVerticle(config)).subscribe(
                deploymentId -> log.info("REALTIME_GATEWAY_STARTED deploymentId={}", deploymentId),
                error -> {
                    log.error("REALTIME_GATEWAY_START_FAILED", error);
                    vertx.close();
                });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> vertx.close().blockingAwait()));
    }
}
