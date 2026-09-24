package io.distsem.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.distsem.core.fleet.CommandType;
import io.distsem.core.fleet.HeartbeatResponse;
import io.distsem.core.fleet.NodeCommand;
import io.distsem.core.fleet.NodeReport;
import io.distsem.core.fleet.NodeStatus;
import io.distsem.core.fleet.WorkerReport;
import io.distsem.core.fleet.WorkerState;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

class SemaphoreClientIT {

    private SemaphoreClient client;
    private DistributedSemaphore semaphore;

    @BeforeEach
    void setUp() throws InterruptedException {
        client = SemaphoreClient.builder()
                .endpoints(ServiceReplicas.replicaA(), ServiceReplicas.replicaB())
                .build();
        semaphore = client.semaphore("sem-" + UUID.randomUUID().toString().substring(0, 8));
        semaphore.configure(1, Duration.ofSeconds(30));
    }

    @AfterEach
    void tearDown() {
        client.close();
    }

    @Test
    void acquireAndReleaseRoundTrip() throws Exception {
        try (Lease lease = semaphore.acquire("worker-1", Duration.ZERO)) {
            assertThat(lease.isValid()).isTrue();
            assertThat(lease.fencingToken()).isEqualTo(1);
            assertThat(semaphore.state().holders()).extracting(Model.PermitInfo::holderId).containsExactly("worker-1");
        }
        assertThat(semaphore.state().held()).isZero();
    }

    @Test
    void tryAcquireIsEmptyWhenFull() throws Exception {
        try (Lease held = semaphore.acquire("a", Duration.ZERO)) {
            assertThat(semaphore.tryAcquire("b")).isEmpty();
        }
        Optional<Lease> after = semaphore.tryAcquire("b");
        assertThat(after).isPresent();
        after.get().close();
    }

    @Test
    void waitsInQueueReportingPositionUntilGranted() throws Exception {
        Lease holder = semaphore.acquire("holder", Duration.ZERO);
        List<Integer> positions = new CopyOnWriteArrayList<>();
        CompletableFuture<Lease> waiter = CompletableFuture.supplyAsync(() -> {
            try {
                return semaphore.acquire(AcquireOptions.holder("waiter")
                        .waitUpTo(Duration.ofSeconds(20))
                        .onQueued(positions::add));
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });
        // The service answers "still queued" every second (max-hold=1s); the client keeps polling.
        await().atMost(Duration.ofSeconds(10)).until(() -> positions.size() >= 2);
        assertThat(waiter).isNotDone();

        holder.close();
        try (Lease granted = waiter.get(5, TimeUnit.SECONDS)) {
            assertThat(granted.holderId()).isEqualTo("waiter");
            assertThat(granted.fencingToken()).isEqualTo(2);
        }
        assertThat(positions).containsOnly(0);
    }

    @Test
    void acquireTimesOutAndLeavesTheQueue() throws Exception {
        try (Lease holder = semaphore.acquire("holder", Duration.ZERO)) {
            long start = System.nanoTime();
            assertThatThrownBy(() -> semaphore.acquire("late", Duration.ofMillis(1500)))
                    .isInstanceOfSatisfying(AcquireRejectedException.class,
                            e -> assertThat(e.reason()).isEqualTo(AcquireRejectedException.Reason.WAIT_TIMEOUT));
            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isBetween(1_400L, 4_000L);
            assertThat(semaphore.state().waiting()).isZero();
        }
    }

    @Test
    void autoRenewKeepsLeaseAliveBeyondItsTtl() throws Exception {
        try (Lease lease = semaphore.acquire(AcquireOptions.holder("slow").ttl(Duration.ofMillis(900)))) {
            Instant firstExpiry = lease.expiresAt();
            Thread.sleep(3_000);
            assertThat(lease.isValid()).isTrue();
            assertThat(lease.expiresAt()).isAfter(firstExpiry);
            assertThat(semaphore.state().held()).isEqualTo(1);
            assertThat(lease.release()).isTrue();
        }
    }

    @Test
    void detectsLeaseLostOnTheServer() throws Exception {
        AtomicReference<String> reason = new AtomicReference<>();
        Lease lease = semaphore.acquire(AcquireOptions.holder("victim").ttl(Duration.ofMillis(1500)));
        lease.onLost(reason::set);

        // Someone else releases the permit on the server (e.g. an operator, or a wrong release).
        assertThat(semaphore.releasePermit(lease.permitId())).isTrue();

        await().atMost(Duration.ofSeconds(5)).until(() -> reason.get() != null);
        assertThat(lease.state()).isEqualTo(Lease.State.LOST);
        assertThat(lease.isValid()).isFalse();
        assertThat(lease.release()).isFalse();
    }

    @Test
    void abandonedLeaseExpiresAndFreesTheSlot() throws Exception {
        Lease crashed = semaphore.acquire(AcquireOptions.holder("crashy").ttl(Duration.ofSeconds(1)));
        crashed.abandon();
        assertThat(semaphore.tryAcquire("next")).isEmpty();
        try (Lease next = semaphore.acquire("next", Duration.ofSeconds(10))) {
            assertThat(next.fencingToken()).isGreaterThan(crashed.fencingToken());
        }
    }

    @Test
    void failsOverWhenAReplicaGoesAway() throws Exception {
        ConfigurableApplicationContext doomed = ServiceReplicas.start();
        try (SemaphoreClient failover = SemaphoreClient.builder()
                .endpoints("http://localhost:1", ServiceReplicas.url(doomed), ServiceReplicas.replicaB())
                .connectTimeout(Duration.ofMillis(500))
                .build()) {
            DistributedSemaphore sem = failover.semaphore(semaphore.name());
            Lease first = sem.acquire("w", Duration.ZERO);

            doomed.close();

            assertThat(first.release()).isTrue();
            try (Lease second = sem.acquire("w", Duration.ofSeconds(5))) {
                assertThat(second.fencingToken()).isGreaterThan(first.fencingToken());
            }
        } finally {
            if (doomed.isActive()) {
                doomed.close();
            }
        }
    }

    @Test
    void nodeHeartbeatsCarryCommandsUntilAcknowledged() throws Exception {
        String nodeId = "node-" + UUID.randomUUID().toString().substring(0, 6);
        NodeReport report = report(nodeId, List.of());
        assertThat(client.fleet().heartbeat(report).commands()).isEmpty();

        NodeCommand scale = client.fleet().command(nodeId, CommandType.SCALE_WORKERS, Map.of("count", "5"));
        HeartbeatResponse withCommand = client.fleet().heartbeat(report);
        assertThat(withCommand.commands()).extracting(NodeCommand::type).containsExactly(CommandType.SCALE_WORKERS);
        assertThat(withCommand.commands().getFirst().arg("count")).isEqualTo("5");

        // Delivered again until acknowledged.
        assertThat(client.fleet().heartbeat(report).commands()).hasSize(1);
        assertThat(client.fleet().heartbeat(report(nodeId, List.of(scale.id()))).commands()).isEmpty();

        NodeStatus status = client.fleet().nodes().stream()
                .filter(n -> n.report().nodeId().equals(nodeId)).findFirst().orElseThrow();
        assertThat(status.online()).isTrue();
        assertThat(status.report().workers()).extracting(WorkerReport::state).containsExactly(WorkerState.IDLE);

        await().atMost(Duration.ofSeconds(10)).until(() -> client.fleet().nodes().stream()
                .filter(n -> n.report().nodeId().equals(nodeId)).noneMatch(NodeStatus::online));
    }

    private static NodeReport report(String nodeId, List<Long> acked) {
        WorkerReport worker = new WorkerReport("w1", WorkerState.IDLE, Instant.now(), null, null, null, null, 0, 0, 0);
        return new NodeReport(nodeId, "host", "test", Instant.now(), "sem", false, Map.of(), List.of(worker),
                List.of(), acked);
    }
}
