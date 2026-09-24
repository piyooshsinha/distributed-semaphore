package io.distsem.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import io.distsem.core.AcquireRequest;
import io.distsem.core.AcquireResult;
import io.distsem.core.EventType;
import io.distsem.core.SemaphoreConfig;
import io.distsem.core.SemaphoreEvent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Many workers on two "replicas" (independent connection pools and store instances) hammer one
 * semaphore. Every worker tracks how many permits are held at once in memory; that number must
 * never exceed capacity.
 */
class ConcurrencyIT {

    private static final String SEM = "hot-db";
    private static final int CAPACITY = 3;
    private static final int WORKERS = 24;
    private static final int ROUNDS_PER_WORKER = 15;

    private HikariDataSource replicaA;
    private HikariDataSource replicaB;

    @BeforeEach
    void setUp() {
        PostgresTestSupport.truncateAll();
        replicaA = PostgresTestSupport.newPool("replica-a", 12);
        replicaB = PostgresTestSupport.newPool("replica-b", 12);
    }

    @AfterEach
    void tearDown() {
        replicaA.close();
        replicaB.close();
    }

    @Test
    void holdersNeverExceedCapacityAcrossReplicas() throws Exception {
        PostgresSemaphoreStore storeA = new PostgresSemaphoreStore(replicaA);
        PostgresSemaphoreStore storeB = new PostgresSemaphoreStore(replicaB);
        storeA.create(new SemaphoreConfig(SEM, CAPACITY, Duration.ofSeconds(30)));

        AtomicInteger inCriticalSection = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();
        ConcurrentHashMap<Long, String> tokens = new ConcurrentHashMap<>();
        CountDownLatch start = new CountDownLatch(1);

        List<Future<?>> futures = new ArrayList<>();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int w = 0; w < WORKERS; w++) {
                PostgresSemaphoreStore store = w % 2 == 0 ? storeA : storeB;
                String worker = "worker-" + w;
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int round = 0; round < ROUNDS_PER_WORKER; round++) {
                        AcquireRequest request = AcquireRequest.tryOnce(SEM, worker, UUID.randomUUID().toString())
                                .withWaitTimeout(Duration.ofSeconds(60));
                        AcquireResult result;
                        while (!((result = store.tryAcquire(request)) instanceof AcquireResult.Granted)) {
                            assertThat(result).isInstanceOf(AcquireResult.Queued.class);
                            Thread.sleep(ThreadLocalRandom.current().nextInt(1, 6));
                        }
                        var permit = ((AcquireResult.Granted) result).permit();
                        assertThat(tokens.putIfAbsent(permit.fencingToken(), worker)).isNull();

                        int now = inCriticalSection.incrementAndGet();
                        maxObserved.accumulateAndGet(now, Math::max);
                        Thread.sleep(ThreadLocalRandom.current().nextInt(1, 4));
                        inCriticalSection.decrementAndGet();

                        assertThat(store.release(SEM, permit.permitId())).isTrue();
                        completed.incrementAndGet();
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(3, TimeUnit.MINUTES);
            }
        }

        int total = WORKERS * ROUNDS_PER_WORKER;
        assertThat(completed.get()).isEqualTo(total);
        assertThat(maxObserved.get()).isBetween(2, CAPACITY);
        assertThat(tokens).hasSize(total);
        assertThat(storeA.state(SEM).holders()).isEmpty();
        assertThat(storeA.state(SEM).waiters()).isEmpty();
        assertThat(storeA.state(SEM).lastFencingToken()).isEqualTo(total);

        List<SemaphoreEvent> events = storeA.events(SEM, 0, 10_000);
        assertThat(events).filteredOn(e -> e.type() == EventType.ACQUIRED).hasSize(total);
        assertThat(events).filteredOn(e -> e.type() == EventType.RELEASED).hasSize(total);
        assertThat(events).noneMatch(e -> e.type() == EventType.EXPIRED || e.type() == EventType.WAIT_ABANDONED);
    }
}
