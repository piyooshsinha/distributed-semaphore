package io.distsem.simulator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Node settings, read from environment variables.
 *
 * @param endpoints       service replica URLs ({@code DISTSEM_ENDPOINTS}, comma-separated)
 * @param workers         initial worker count ({@code WORKERS})
 * @param leaseTtl        permit lease; renewed in the background ({@code LEASE_TTL_MS})
 * @param workMin         shortest flush while holding a permit ({@code WORK_MIN_MS})
 * @param workMax         longest flush ({@code WORK_MAX_MS})
 * @param idleMin         shortest pause between tasks ({@code IDLE_MIN_MS})
 * @param idleMax         longest pause between tasks ({@code IDLE_MAX_MS})
 * @param waitTimeout     how long a worker queues for a permit ({@code WAIT_TIMEOUT_MS})
 * @param failureRate     chance that a flush fails ({@code FAILURE_RATE}, 0..1)
 * @param crashDowntime   how long a crashed worker stays down ({@code CRASH_DOWNTIME_MS})
 * @param heartbeat       heartbeat interval ({@code HEARTBEAT_MS})
 */
public record SimulatorConfig(
        String nodeId,
        List<String> endpoints,
        String apiKey,
        String semaphore,
        int semaphoreCapacity,
        Duration semaphoreDefaultTtl,
        int workers,
        Duration leaseTtl,
        Duration workMin,
        Duration workMax,
        Duration idleMin,
        Duration idleMax,
        Duration waitTimeout,
        double failureRate,
        Duration crashDowntime,
        Duration heartbeat) {

    public static SimulatorConfig fromEnv(Map<String, String> env) {
        return new SimulatorConfig(
                env.getOrDefault("NODE_ID", hostname()),
                Arrays.stream(env.getOrDefault("DISTSEM_ENDPOINTS", "http://localhost:8183").split(","))
                        .map(String::trim).filter(s -> !s.isEmpty()).toList(),
                env.get("DISTSEM_API_KEY"),
                env.getOrDefault("SEMAPHORE", "log-flush"),
                Integer.parseInt(env.getOrDefault("SEMAPHORE_CAPACITY", "3")),
                millis(env, "SEMAPHORE_TTL_MS", 10_000),
                Integer.parseInt(env.getOrDefault("WORKERS", "3")),
                millis(env, "LEASE_TTL_MS", 5_000),
                millis(env, "WORK_MIN_MS", 800),
                millis(env, "WORK_MAX_MS", 2_500),
                millis(env, "IDLE_MIN_MS", 200),
                millis(env, "IDLE_MAX_MS", 1_500),
                millis(env, "WAIT_TIMEOUT_MS", 60_000),
                Double.parseDouble(env.getOrDefault("FAILURE_RATE", "0.03")),
                millis(env, "CRASH_DOWNTIME_MS", 8_000),
                millis(env, "HEARTBEAT_MS", 1_000));
    }

    private static Duration millis(Map<String, String> env, String key, long fallback) {
        return Duration.ofMillis(Long.parseLong(env.getOrDefault(key, Long.toString(fallback))));
    }

    private static String hostname() {
        String fromEnv = System.getenv("HOSTNAME");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "node";
        }
    }
}
