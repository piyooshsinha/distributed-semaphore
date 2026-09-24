package io.distsem.client;

import io.distsem.client.Model.PermitInfo;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A held permit. Renewed in the background at a third of its TTL until closed.
 *
 * <p>Check {@link #isValid()} (or register {@link #onLost}) before committing work to the
 * protected resource, and pass {@link #fencingToken()} along so the resource can reject writes
 * from a holder whose lease has lapsed. Validity is judged on this machine's monotonic clock,
 * starting from when the grant or renewal request was <em>sent</em>, so it errs on the side of
 * "expired" and is unaffected by clock skew with the server.
 */
public final class Lease implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Lease.class);
    private static final long RETRY_NANOS = TimeUnit.MILLISECONDS.toNanos(500);

    public enum State { ACTIVE, RELEASED, LOST, ABANDONED }

    private final DistributedSemaphore semaphore;
    private final Duration ttl;
    private final List<Consumer<String>> lostListeners = new CopyOnWriteArrayList<>();

    private volatile PermitInfo permit;
    private volatile long validUntilNanos;
    private volatile State state = State.ACTIVE;
    private volatile String lostReason;
    private ScheduledFuture<?> renewal;

    Lease(DistributedSemaphore semaphore, PermitInfo permit, Duration ttl, long sentAtNanos, boolean autoRenew) {
        this.semaphore = semaphore;
        this.permit = permit;
        this.ttl = ttl;
        this.validUntilNanos = sentAtNanos + ttl.toNanos();
        if (autoRenew) {
            scheduleRenewal(ttl.toNanos() / 3);
        }
    }

    public UUID permitId() {
        return permit.permitId();
    }

    public long fencingToken() {
        return permit.fencingToken();
    }

    public String holderId() {
        return permit.holderId();
    }

    public String requestId() {
        return permit.requestId();
    }

    public String semaphore() {
        return permit.semaphore();
    }

    /** Server-side expiry as of the last grant or renewal. */
    public Instant expiresAt() {
        return permit.expiresAt();
    }

    public State state() {
        return state;
    }

    /** Why the lease was lost, if it was. */
    public String lostReason() {
        return lostReason;
    }

    /** True while the lease is active and, by this machine's clock, not past its TTL. */
    public boolean isValid() {
        return state == State.ACTIVE && System.nanoTime() - validUntilNanos < 0;
    }

    /** Runs {@code listener} (on a background thread) if the lease is lost. Runs immediately if it already was. */
    public Lease onLost(Consumer<String> listener) {
        lostListeners.add(listener);
        if (state == State.LOST) {
            listener.accept(lostReason);
        }
        return this;
    }

    /** Renews now, extending the lease by its TTL. */
    public void renew() throws InterruptedException {
        long sentAt = System.nanoTime();
        PermitInfo renewed = semaphore.renewPermit(permit.permitId(), ttl);
        synchronized (this) {
            if (state == State.ACTIVE) {
                permit = renewed;
                validUntilNanos = sentAt + ttl.toNanos();
            }
        }
    }

    /** Releases the permit. Idempotent. Returns false if the server no longer had it (e.g. it expired). */
    public boolean release() throws InterruptedException {
        synchronized (this) {
            if (state != State.ACTIVE) {
                return false;
            }
            state = State.RELEASED;
            cancelRenewal();
        }
        return semaphore.releasePermit(permit.permitId());
    }

    /**
     * Stops renewing without releasing, as a crashed process would. The server frees the slot when
     * the lease expires. Meant for failure testing.
     */
    public synchronized void abandon() {
        if (state == State.ACTIVE) {
            state = State.ABANDONED;
            cancelRenewal();
        }
    }

    /** Releases the permit, swallowing errors (the lease expires on its own if release fails). */
    @Override
    public void close() {
        try {
            release();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (SemaphoreClientException e) {
            log.warn("Releasing permit {} on '{}' failed; it will expire: {}", permitId(), semaphore(), e.getMessage());
        }
    }

    // ---------------------------------------------------------------- renewal

    private synchronized void scheduleRenewal(long delayNanos) {
        if (state == State.ACTIVE) {
            renewal = semaphore.client().scheduler().schedule(
                    () -> Thread.ofVirtual().name("lease-renew").start(this::renewInBackground),
                    Math.max(delayNanos, TimeUnit.MILLISECONDS.toNanos(50)), TimeUnit.NANOSECONDS);
        }
    }

    private synchronized void cancelRenewal() {
        if (renewal != null) {
            renewal.cancel(false);
        }
    }

    private void renewInBackground() {
        if (state != State.ACTIVE) {
            return;
        }
        try {
            renew();
            scheduleRenewal(ttl.toNanos() / 3);
        } catch (SemaphoreClientException e) {
            if (e.isNotFound()) {
                markLost("permit is no longer held on the server (expired or removed)");
                return;
            }
            long left = validUntilNanos - System.nanoTime();
            if (left <= 0) {
                markLost("could not renew before the lease expired: " + e.getMessage());
            } else {
                log.debug("Renewing {} failed, retrying: {}", permitId(), e.getMessage());
                scheduleRenewal(Math.min(RETRY_NANOS, left / 2));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void markLost(String reason) {
        synchronized (this) {
            if (state != State.ACTIVE) {
                return;
            }
            state = State.LOST;
            lostReason = reason;
            cancelRenewal();
        }
        log.warn("Lost lease {} on '{}': {}", permitId(), semaphore(), reason);
        for (Consumer<String> listener : lostListeners) {
            try {
                listener.accept(reason);
            } catch (RuntimeException e) {
                log.warn("Lease-lost listener failed", e);
            }
        }
    }

}
