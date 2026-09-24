package io.distsem.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.distsem.core.AcquireRequest;
import io.distsem.core.AcquireResult;
import io.distsem.core.AcquireResult.Granted;
import io.distsem.core.AcquireResult.Queued;
import io.distsem.core.AcquireResult.Rejected;
import io.distsem.core.EventType;
import io.distsem.core.Permit;
import io.distsem.core.PermitNotFoundException;
import io.distsem.core.SemaphoreAlreadyExistsException;
import io.distsem.core.SemaphoreConfig;
import io.distsem.core.SemaphoreEvent;
import io.distsem.core.SemaphoreNotFoundException;
import io.distsem.core.SemaphoreState;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PostgresSemaphoreStoreIT {

    private static final String SEM = "db-flush";
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Duration WAIT = Duration.ofSeconds(30);

    private final DataSource dataSource = PostgresTestSupport.dataSource();
    private final PostgresSemaphoreStore store = new PostgresSemaphoreStore(dataSource);

    @BeforeEach
    void cleanDatabase() {
        PostgresTestSupport.truncateAll();
    }

    private void createSemaphore(int capacity) {
        store.create(new SemaphoreConfig(SEM, capacity, TTL));
    }

    private AcquireResult tryOnce(String requestId) {
        return store.tryAcquire(AcquireRequest.tryOnce(SEM, "holder-" + requestId, requestId));
    }

    private AcquireResult tryWaiting(String requestId) {
        return store.tryAcquire(AcquireRequest.tryOnce(SEM, "holder-" + requestId, requestId).withWaitTimeout(WAIT));
    }

    private Permit granted(AcquireResult result) {
        assertThat(result).isInstanceOf(Granted.class);
        return ((Granted) result).permit();
    }

    private List<EventType> eventTypes() {
        return store.events(SEM, 0, 1000).stream().map(SemaphoreEvent::type).toList();
    }

    @Nested
    class Registry {

        @Test
        void createIsIdempotentForIdenticalConfig() {
            SemaphoreConfig config = new SemaphoreConfig(SEM, 3, TTL);
            assertThat(store.create(config)).isEqualTo(config);
            assertThat(store.create(config)).isEqualTo(config);
            assertThat(store.list()).containsExactly(config);
            assertThat(eventTypes()).containsExactly(EventType.CREATED);
        }

        @Test
        void createWithDifferentConfigConflicts() {
            createSemaphore(3);
            assertThatThrownBy(() -> store.create(new SemaphoreConfig(SEM, 5, TTL)))
                    .isInstanceOf(SemaphoreAlreadyExistsException.class);
        }

        @Test
        void updateChangesSettingsAndIsAudited() {
            createSemaphore(3);
            store.update(new SemaphoreConfig(SEM, 5, Duration.ofSeconds(10)));
            assertThat(store.find(SEM)).contains(new SemaphoreConfig(SEM, 5, Duration.ofSeconds(10)));
            assertThat(eventTypes()).containsExactly(EventType.CREATED, EventType.UPDATED);
        }

        @Test
        void deleteRemovesPermitsButKeepsAuditTrail() {
            createSemaphore(1);
            granted(tryOnce("r1"));
            assertThat(store.delete(SEM)).isTrue();
            assertThat(store.delete(SEM)).isFalse();
            assertThat(store.find(SEM)).isEmpty();
            assertThat(eventTypes()).containsExactly(EventType.CREATED, EventType.ACQUIRED, EventType.DELETED);
        }

        @Test
        void operationsOnUnknownSemaphoreFail() {
            assertThatThrownBy(() -> tryOnce("r1")).isInstanceOf(SemaphoreNotFoundException.class);
            assertThatThrownBy(() -> store.state(SEM)).isInstanceOf(SemaphoreNotFoundException.class);
            assertThatThrownBy(() -> store.update(new SemaphoreConfig(SEM, 1, TTL)))
                    .isInstanceOf(SemaphoreNotFoundException.class);
        }
    }

    @Nested
    class AcquireAndRelease {

        @Test
        void grantsUpToCapacityThenRejects() {
            createSemaphore(3);
            granted(tryOnce("r1"));
            granted(tryOnce("r2"));
            granted(tryOnce("r3"));

            AcquireResult fourth = tryOnce("r4");
            assertThat(fourth).isEqualTo(new Rejected(0, 0, AcquireResult.Reason.NO_CAPACITY));

            SemaphoreState state = store.state(SEM);
            assertThat(state.holders()).hasSize(3);
            assertThat(state.available()).isZero();
        }

        @Test
        void fencingTokensStrictlyIncrease() {
            createSemaphore(2);
            Permit a = granted(tryOnce("r1"));
            Permit b = granted(tryOnce("r2"));
            store.release(SEM, a.permitId());
            Permit c = granted(tryOnce("r3"));
            assertThat(List.of(a.fencingToken(), b.fencingToken(), c.fencingToken())).containsExactly(1L, 2L, 3L);
            assertThat(store.state(SEM).lastFencingToken()).isEqualTo(3);
        }

        @Test
        void releaseFreesSlotAndIsIdempotent() {
            createSemaphore(1);
            Permit permit = granted(tryOnce("r1"));
            assertThat(tryOnce("r2")).isInstanceOf(Rejected.class);

            assertThat(store.release(SEM, permit.permitId())).isTrue();
            assertThat(store.release(SEM, permit.permitId())).isFalse();
            assertThat(store.release(SEM, UUID.randomUUID())).isFalse();

            granted(tryOnce("r2"));
        }

        @Test
        void retryWithSameRequestIdReturnsSamePermit() {
            createSemaphore(1);
            Permit first = granted(tryOnce("r1"));
            Permit retry = granted(tryOnce("r1"));
            assertThat(retry).isEqualTo(first);
            assertThat(store.state(SEM).holders()).hasSize(1);
        }

        @Test
        void honoursRequestedTtlAndDefault() {
            createSemaphore(2);
            Permit withDefault = granted(tryOnce("r1"));
            Permit custom = granted(store.tryAcquire(
                    AcquireRequest.tryOnce(SEM, "h", "r2").withTtl(Duration.ofSeconds(5))));
            assertThat(Duration.between(withDefault.acquiredAt(), withDefault.expiresAt())).isEqualTo(TTL);
            assertThat(Duration.between(custom.acquiredAt(), custom.expiresAt())).isEqualTo(Duration.ofSeconds(5));
        }

        @Test
        void rejectsTtlAndWaitAboveConfiguredMaximum() {
            createSemaphore(1);
            assertThatIllegalArgumentException().isThrownBy(() -> store.tryAcquire(
                    AcquireRequest.tryOnce(SEM, "h", "r1").withTtl(Duration.ofHours(2))));
            assertThatIllegalArgumentException().isThrownBy(() -> store.tryAcquire(
                    AcquireRequest.tryOnce(SEM, "h", "r1").withWaitTimeout(Duration.ofHours(1))));
        }

        @Test
        void loweringCapacityKeepsHoldersButBlocksNewGrants() {
            createSemaphore(3);
            granted(tryOnce("r1"));
            Permit second = granted(tryOnce("r2"));
            store.update(new SemaphoreConfig(SEM, 1, TTL));

            assertThat(store.state(SEM).holders()).hasSize(2);
            assertThat(tryOnce("r3")).isInstanceOf(Rejected.class);
            store.release(SEM, second.permitId());
            assertThat(tryOnce("r3")).isInstanceOf(Rejected.class);
        }
    }

    @Nested
    class Leases {

        @Test
        void expiredPermitIsEvictedByNextAcquire() {
            createSemaphore(1);
            Permit shortLived = granted(store.tryAcquire(
                    AcquireRequest.tryOnce(SEM, "crashy", "r1").withTtl(Duration.ofMillis(200))));

            await().atMost(Duration.ofSeconds(5)).until(() -> tryOnce("r2") instanceof Granted);

            assertThat(eventTypes()).containsSubsequence(EventType.ACQUIRED, EventType.EXPIRED, EventType.ACQUIRED);
            assertThat(store.release(SEM, shortLived.permitId())).isFalse();
        }

        @Test
        void stateHidesExpiredButUnreapedPermits() throws InterruptedException {
            createSemaphore(1);
            granted(store.tryAcquire(AcquireRequest.tryOnce(SEM, "h", "r1").withTtl(Duration.ofMillis(100))));
            Thread.sleep(300);
            assertThat(store.state(SEM).holders()).isEmpty();
        }

        @Test
        void renewExtendsLease() {
            createSemaphore(1);
            Permit permit = granted(store.tryAcquire(
                    AcquireRequest.tryOnce(SEM, "h", "r1").withTtl(Duration.ofSeconds(1))));
            Permit renewed = store.renew(SEM, permit.permitId(), Duration.ofMinutes(5));

            assertThat(renewed.permitId()).isEqualTo(permit.permitId());
            assertThat(renewed.fencingToken()).isEqualTo(permit.fencingToken());
            assertThat(renewed.expiresAt()).isAfter(permit.expiresAt().plusSeconds(200));
            assertThat(eventTypes()).endsWith(EventType.RENEWED);
        }

        @Test
        void renewAfterExpiryOrReleaseFails() throws InterruptedException {
            createSemaphore(2);
            Permit expiring = granted(store.tryAcquire(
                    AcquireRequest.tryOnce(SEM, "h", "r1").withTtl(Duration.ofMillis(100))));
            Permit released = granted(tryOnce("r2"));
            store.release(SEM, released.permitId());
            Thread.sleep(300);

            assertThatThrownBy(() -> store.renew(SEM, expiring.permitId(), TTL))
                    .isInstanceOf(PermitNotFoundException.class);
            assertThatThrownBy(() -> store.renew(SEM, released.permitId(), TTL))
                    .isInstanceOf(PermitNotFoundException.class);
        }

        @Test
        void reaperEvictsAcrossSemaphores() throws InterruptedException {
            createSemaphore(2);
            store.create(new SemaphoreConfig("other", 1, TTL));
            granted(store.tryAcquire(AcquireRequest.tryOnce(SEM, "h", "r1").withTtl(Duration.ofMillis(100))));
            granted(store.tryAcquire(AcquireRequest.tryOnce("other", "h", "r1").withTtl(Duration.ofMillis(100))));
            granted(tryOnce("live"));
            Thread.sleep(300);

            assertThat(store.reapExpired()).isEqualTo(2);
            assertThat(store.reapExpired()).isZero();
            assertThat(store.state(SEM).holders()).extracting(Permit::requestId).containsExactly("live");
        }
    }

    @Nested
    class Queueing {

        @Test
        void waitersAreGrantedInArrivalOrder() {
            createSemaphore(1);
            Permit holder = granted(tryOnce("a"));

            Queued b = (Queued) tryWaiting("b");
            Queued c = (Queued) tryWaiting("c");
            assertThat(b.position()).isZero();
            assertThat(c.position()).isEqualTo(1);

            store.release(SEM, holder.permitId());

            // c polls first but b is ahead of it, so c must keep waiting.
            assertThat(tryWaiting("c")).isInstanceOf(Queued.class);
            // A newcomer that is not willing to wait cannot jump the queue either.
            assertThat(tryOnce("d")).isEqualTo(new Rejected(1, 2, AcquireResult.Reason.NO_CAPACITY));

            Permit bPermit = granted(tryWaiting("b"));
            assertThat(((Queued) tryWaiting("c")).position()).isZero();

            store.release(SEM, bPermit.permitId());
            granted(tryWaiting("c"));
            assertThat(store.state(SEM).waiters()).isEmpty();
        }

        @Test
        void newcomerMayProceedWhenFreeSlotsExceedQueue() {
            createSemaphore(3);
            granted(tryOnce("a"));
            granted(tryOnce("b"));
            granted(tryOnce("c"));
            tryWaiting("w1");
            store.update(new SemaphoreConfig(SEM, 5, TTL));

            // 2 free slots, 1 waiter: one slot is reserved for w1, the other is up for grabs.
            granted(tryOnce("newcomer"));
            assertThat(tryOnce("another")).isInstanceOf(Rejected.class);
            granted(tryWaiting("w1"));
        }

        @Test
        void waiterTimesOutAfterDeadline() {
            createSemaphore(1);
            granted(tryOnce("a"));
            AcquireRequest request = AcquireRequest.tryOnce(SEM, "h", "b").withWaitTimeout(Duration.ofMillis(300));
            assertThat(store.tryAcquire(request)).isInstanceOf(Queued.class);

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(store.tryAcquire(request))
                    .isEqualTo(new Rejected(0, 0, AcquireResult.Reason.WAIT_TIMEOUT)));
            assertThat(eventTypes()).containsSubsequence(EventType.QUEUED, EventType.WAIT_TIMEOUT);
        }

        @Test
        void abandonedWaiterStopsBlockingTheQueue() throws InterruptedException {
            PostgresSemaphoreStore shortLease = new PostgresSemaphoreStore(
                    dataSource, PostgresStoreOptions.defaults().withWaiterLease(Duration.ofMillis(200)));
            createSemaphore(1);
            Permit holder = granted(tryOnce("a"));
            assertThat(shortLease.tryAcquire(
                    AcquireRequest.tryOnce(SEM, "gone", "abandoned").withWaitTimeout(WAIT))).isInstanceOf(Queued.class);
            assertThat(((Queued) shortLease.tryAcquire(
                    AcquireRequest.tryOnce(SEM, "h", "patient").withWaitTimeout(WAIT))).position()).isEqualTo(1);

            store.release(SEM, holder.permitId());
            Thread.sleep(400);

            granted(shortLease.tryAcquire(AcquireRequest.tryOnce(SEM, "h", "patient").withWaitTimeout(WAIT)));
            assertThat(eventTypes()).contains(EventType.WAIT_ABANDONED);
        }

        @Test
        void latePollerKeepsItsPlaceIfNobodyEvictedIt() throws InterruptedException {
            PostgresSemaphoreStore shortLease = new PostgresSemaphoreStore(
                    dataSource, PostgresStoreOptions.defaults().withWaiterLease(Duration.ofMillis(100)));
            createSemaphore(1);
            granted(tryOnce("a"));
            AcquireRequest request = AcquireRequest.tryOnce(SEM, "h", "late").withWaitTimeout(WAIT);
            Queued first = (Queued) shortLease.tryAcquire(request);
            Thread.sleep(300);

            Queued again = (Queued) shortLease.tryAcquire(request);
            assertThat(again.waiterId()).isEqualTo(first.waiterId());
            assertThat(again.waitDeadline()).isEqualTo(first.waitDeadline());
        }

        @Test
        void cancelWaitRemovesWaiter() {
            createSemaphore(1);
            granted(tryOnce("a"));
            tryWaiting("b");
            assertThat(store.cancelWait(SEM, "b")).isTrue();
            assertThat(store.cancelWait(SEM, "b")).isFalse();
            assertThat(store.state(SEM).waiters()).isEmpty();
            assertThat(eventTypes()).endsWith(EventType.WAIT_CANCELLED);
        }
    }

    @Nested
    class AuditLog {

        @Test
        void eventsArePagedById() {
            createSemaphore(5);
            for (int i = 0; i < 5; i++) {
                granted(tryOnce("r" + i));
            }
            List<SemaphoreEvent> firstPage = store.events(SEM, 0, 3);
            List<SemaphoreEvent> secondPage = store.events(SEM, firstPage.getLast().id(), 3);

            assertThat(firstPage).hasSize(3);
            assertThat(secondPage).hasSize(3);
            assertThat(secondPage.getFirst().id()).isGreaterThan(firstPage.getLast().id());
            assertThat(secondPage.getLast().fencingToken()).isEqualTo(5L);
        }

        @Test
        void pruneRemovesOldEvents() throws Exception {
            createSemaphore(1);
            try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(
                    "UPDATE semaphore_events SET occurred_at = now() - interval '2 days'")) {
                ps.executeUpdate();
            }
            granted(tryOnce("r1"));
            assertThat(store.pruneEvents(Duration.ofDays(1))).isEqualTo(1);
            assertThat(eventTypes()).containsExactly(EventType.ACQUIRED);
        }
    }

    @Nested
    class DatabaseGuard {

        @Test
        void triggerRejectsInsertBeyondCapacity() throws Exception {
            createSemaphore(1);
            granted(tryOnce("r1"));
            try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO semaphore_holders
                        (permit_id, semaphore_name, holder_id, request_id, fencing_token, acquired_at, expires_at)
                    VALUES (gen_random_uuid(), ?, 'rogue', 'rogue', 99, now(), now() + interval '1 minute')""")) {
                ps.setString(1, SEM);
                assertThatThrownBy(ps::executeUpdate).hasMessageContaining("over capacity");
            }
            assertThat(store.state(SEM).holders()).hasSize(1);
        }
    }
}
