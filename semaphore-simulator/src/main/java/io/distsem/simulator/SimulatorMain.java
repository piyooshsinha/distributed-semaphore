package io.distsem.simulator;

import io.distsem.client.SemaphoreClient;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs one simulated worker node until the process is stopped. Configured by environment variables. */
public final class SimulatorMain {

    private static final Logger log = LoggerFactory.getLogger(SimulatorMain.class);

    private SimulatorMain() {
    }

    public static void main(String[] args) throws InterruptedException {
        SimulatorConfig config = SimulatorConfig.fromEnv(System.getenv());
        log.info("Starting node {} with {} workers against {}", config.nodeId(), config.workers(), config.endpoints());

        SemaphoreClient client = SemaphoreClient.builder()
                .endpoints(config.endpoints().toArray(String[]::new))
                .apiKey(config.apiKey())
                .build();
        LogProcessorNode node = new LogProcessorNode(config, client);
        CountDownLatch stopped = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().unstarted(() -> {
            log.info("Shutting down node {}: finishing current tasks and releasing permits", config.nodeId());
            try {
                node.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                client.close();
                stopped.countDown();
            }
        }));

        node.start();
        stopped.await();
    }
}
