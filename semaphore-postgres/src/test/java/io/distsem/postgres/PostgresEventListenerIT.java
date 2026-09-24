package io.distsem.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.distsem.core.AcquireRequest;
import io.distsem.core.AcquireResult;
import io.distsem.core.EventType;
import io.distsem.core.SemaphoreConfig;
import io.distsem.postgres.PostgresEventListener.Notification;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PostgresEventListenerIT {

    private final PostgresSemaphoreStore store = new PostgresSemaphoreStore(PostgresTestSupport.dataSource());
    private final PostgresEventListener listener = new PostgresEventListener(PostgresTestSupport.dataSource());
    private final List<Notification> received = new CopyOnWriteArrayList<>();
    private final AtomicInteger resyncs = new AtomicInteger();
    private final List<String> nodeChanges = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        PostgresTestSupport.truncateAll();
        listener.subscribe(new PostgresEventListener.Subscriber() {
            @Override
            public void onEvent(Notification notification) {
                received.add(notification);
            }

            @Override
            public void onNodeChange(String nodeId) {
                nodeChanges.add(nodeId);
            }

            @Override
            public void onResync() {
                resyncs.incrementAndGet();
            }
        });
        listener.start();
        await().atMost(Duration.ofSeconds(5)).until(listener::isConnected);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        listener.close();
    }

    @Test
    void deliversCommittedEventsInOrder() {
        store.create(new SemaphoreConfig("jobs", 1, Duration.ofSeconds(30)));
        var permit = ((AcquireResult.Granted) store.tryAcquire(AcquireRequest.tryOnce("jobs", "w1", "r1"))).permit();
        store.release("jobs", permit.permitId());

        await().atMost(Duration.ofSeconds(5)).until(() -> received.size() == 3);
        assertThat(received).extracting(Notification::type)
                .containsExactly(EventType.CREATED, EventType.ACQUIRED, EventType.RELEASED);
        assertThat(received).extracting(Notification::semaphore).containsOnly("jobs");
        assertThat(received).extracting(Notification::eventId).isSorted();
        assertThat(resyncs.get()).isEqualTo(1);
    }

    @Test
    void rolledBackChangesAreNotPublished() throws InterruptedException {
        store.create(new SemaphoreConfig("jobs", 1, Duration.ofSeconds(30)));
        store.tryAcquire(AcquireRequest.tryOnce("jobs", "w1", "r1"));
        // Fails inside the transaction after the semaphore row is locked: nothing commits.
        try {
            store.renew("jobs", java.util.UUID.randomUUID(), Duration.ofSeconds(5));
        } catch (io.distsem.core.PermitNotFoundException expected) {
            // expected
        }
        Thread.sleep(700);
        assertThat(received).extracting(Notification::type).containsExactly(EventType.CREATED, EventType.ACQUIRED);
    }

    @Test
    void deliversNodeChanges() throws Exception {
        try (var c = PostgresTestSupport.dataSource().getConnection(); var s = c.createStatement()) {
            s.execute("INSERT INTO nodes (node_id, report) VALUES ('node-1', '{}')");
            s.execute("UPDATE nodes SET last_seen_at = now() WHERE node_id = 'node-1'");
            s.execute("DELETE FROM nodes WHERE node_id = 'node-1'");
        }
        await().atMost(Duration.ofSeconds(5)).until(() -> nodeChanges.size() == 3);
        assertThat(nodeChanges).containsOnly("node-1");
        assertThat(received).isEmpty();
    }

    @Test
    void parsesPayloads() {
        assertThat(Notification.parse("a.b:c|42|RELEASED")).contains(new Notification("a.b:c", 42, EventType.RELEASED));
        assertThat(Notification.parse("garbage")).isEmpty();
        assertThat(Notification.parse("a|x|RELEASED")).isEmpty();
        assertThat(Notification.parse("a|1|NOPE")).isEmpty();
    }
}
