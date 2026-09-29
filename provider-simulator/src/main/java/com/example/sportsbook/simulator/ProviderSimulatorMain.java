package com.example.sportsbook.simulator;

import com.example.sportsbook.simulator.config.SimulatorConfig;
import io.vertx.rxjava3.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Entry point for the shaded executable JAR. Dev tooling only; not part of the platform. */
public final class ProviderSimulatorMain {

    private static final Logger log = LoggerFactory.getLogger(ProviderSimulatorMain.class);

    private ProviderSimulatorMain() {
    }

    public static void main(String[] args) {
        SimulatorConfig config = SimulatorConfig.fromEnv();
        Vertx vertx = Vertx.vertx();

        vertx.rxDeployVerticle(new SimulatorVerticle(config)).subscribe(
                deploymentId -> log.info("PROVIDER_SIMULATOR_STARTED deploymentId={}", deploymentId),
                error -> {
                    log.error("PROVIDER_SIMULATOR_START_FAILED", error);
                    vertx.close();
                });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> vertx.close().blockingAwait()));
    }
}
