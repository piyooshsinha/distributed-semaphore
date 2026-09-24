package io.distsem.simulator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.distsem.client.Model.EventInfo;
import io.distsem.client.SemaphoreClient;
import io.distsem.core.fleet.CommandType;
import io.distsem.core.fleet.NodeReport;
import io.distsem.core.fleet.NodeStatus;
import io.distsem.core.fleet.TaskOutcome;
import io.distsem.core.fleet.TaskRecord;
import io.distsem.core.fleet.WorkerReport;
import io.distsem.core.fleet.WorkerState;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LogProcessorNodeIT {

    private static final int CAPACITY = 2;

    private SemaphoreClient client;
    private LogProcessorNode node;
    private SimulatorConfig config;

    @BeforeEach
    void startNode() throws InterruptedException {
        String suffix = UUID.randomUUID().toString().substring(0, 6);
        Map<String, String> env = new HashMap<>();
        env.put("NODE_ID", "node-" + suffix);
        env.put("DISTSEM_ENDPOINTS", ServiceReplicas.replicaA());
        env.put("SEMAPHORE", "flush-" + suffix);
        env.put("SEMAPHORE_CAPACITY", Integer.toString(CAPACITY));
        env.put("WORKERS", "5");
        env.put("LEASE_TTL_MS", "1500");
        env.put("WORK_MIN_MS", "150");
        env.put("WORK_MAX_MS", "300");
        env.put("IDLE_MIN_MS", "20");
        env.put("IDLE_MAX_MS", "80");
        env.put("FAILURE_RATE", "0");
        env.put("CRASH_DOWNTIME_MS", "1000");
        env.put("HEARTBEAT_MS", "200");
        config = SimulatorConfig.fromEnv(env);
        client = SemaphoreClient.builder().endpoints(ServiceReplicas.replicaA()).build();
        node = new LogProcessorNode(config, client);
        node.start();
    }

    @AfterEach
    void stopNode() throws InterruptedException {
        node.close();
        client.close();
    }

    @Test
    void workersContendButNeverExceedCapacity() throws Exception {
        AtomicInteger maxHolding = new AtomicInteger();
        long end = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < end) {
            int holding = (int) node.report().workers().stream().filter(w -> w.permitId() != null).count();
            maxHolding.accumulateAndGet(holding, Math::max);
            Thread.sleep(10);
        }
        NodeReport report = node.report();
        long completed = report.workers().stream().mapToLong(WorkerReport::tasksCompleted).sum();

        assertThat(maxHolding.get()).isEqualTo(CAPACITY);
        assertThat(completed).isGreaterThan(15);
        assertThat(report.recentTasks()).extracting(TaskRecord::outcome).containsOnly(TaskOutcome.COMPLETED);
        assertThat(report.recentTasks()).anyMatch(t -> t.waitMs() > 0);

        List<EventInfo> events = client.semaphore(config.semaphore()).events(0, 10_000);
        assertThat(events).extracting(EventInfo::type).contains("QUEUED", "ACQUIRED", "RELEASED").doesNotContain("EXPIRED");
        assertThat(events).filteredOn(e -> e.type().equals("ACQUIRED"))
                .allMatch(e -> e.holderId().startsWith(config.nodeId() + "/w"));
    }

    @Test
    void registersWithFleetAndObeysCommands() throws Exception {
        await().atMost(Duration.ofSeconds(5)).until(() -> nodeStatus().map(NodeStatus::online).orElse(false));
        assertThat(nodeStatus().orElseThrow().report().workers()).hasSize(5);

        client.fleet().command(config.nodeId(), CommandType.SCALE_WORKERS, Map.of("count", "2"));
        await().atMost(Duration.ofSeconds(10)).until(() -> node.report().workers().size() == 2);

        client.fleet().command(config.nodeId(), CommandType.PAUSE, Map.of());
        await().atMost(Duration.ofSeconds(5)).until(() -> node.report().workers().stream()
                .allMatch(w -> w.state() == WorkerState.PAUSED));
        assertThat(nodeStatus().orElseThrow().report().paused()).isTrue();

        client.fleet().command(config.nodeId(), CommandType.RESUME, Map.of());
        await().atMost(Duration.ofSeconds(5)).until(() -> node.report().workers().stream()
                .noneMatch(w -> w.state() == WorkerState.PAUSED));

        client.fleet().command(config.nodeId(), CommandType.SET_WORK_DURATION, Map.of("minMs", "50", "maxMs", "60"));
        await().atMost(Duration.ofSeconds(5)).until(() -> "60".equals(node.report().settings().get("workMaxMs")));
    }

    @Test
    void crashedWorkerLeavesPermitToExpireThenRestarts() throws Exception {
        await().atMost(Duration.ofSeconds(5)).until(() -> nodeStatus().isPresent());
        // Long tasks, so the crash (delivered with the next heartbeat) lands while the permit is held.
        client.fleet().command(config.nodeId(), CommandType.SET_WORK_DURATION, Map.of("minMs", "5000", "maxMs", "5000"));
        await().atMost(Duration.ofSeconds(5)).until(() -> "5000".equals(node.report().settings().get("workMaxMs")));
        // Short tasks last at most 300ms, so after this every worker holding a permit is on a long task.
        Thread.sleep(400);
        WorkerReport victim = await().atMost(Duration.ofSeconds(8)).until(
                () -> node.report().workers().stream()
                        .filter(w -> w.state() == WorkerState.WORKING)
                        .findFirst().orElse(null),
                Objects::nonNull);
        client.fleet().command(config.nodeId(), CommandType.CRASH_WORKER, Map.of("workerId", victim.workerId()));

        await().atMost(Duration.ofSeconds(5)).until(() -> stateOf(victim.workerId()) == WorkerState.CRASHED);
        // Nobody released the permit: the service reclaims it when the 1.5s lease runs out.
        await().atMost(Duration.ofSeconds(10)).until(() -> client.semaphore(config.semaphore()).events(0, 10_000)
                .stream().anyMatch(e -> e.type().equals("EXPIRED") && Objects.equals(e.fencingToken(), victim.fencingToken())));
        await().atMost(Duration.ofSeconds(10)).until(() -> stateOf(victim.workerId()) != WorkerState.CRASHED);

        WorkerReport after = node.report().workers().stream()
                .filter(w -> w.workerId().equals(victim.workerId())).findFirst().orElseThrow();
        assertThat(after.leasesLost()).isEqualTo(1);
        assertThat(node.report().recentTasks()).anyMatch(t -> t.outcome() == TaskOutcome.ABANDONED);
    }

    private WorkerState stateOf(String workerId) {
        return node.report().workers().stream().filter(w -> w.workerId().equals(workerId))
                .map(WorkerReport::state).findFirst().orElseThrow();
    }

    private java.util.Optional<NodeStatus> nodeStatus() throws InterruptedException {
        return client.fleet().nodes().stream().filter(n -> n.report().nodeId().equals(config.nodeId())).findFirst();
    }
}
