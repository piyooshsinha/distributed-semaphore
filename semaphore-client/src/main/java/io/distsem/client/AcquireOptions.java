package io.distsem.client;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.function.IntConsumer;

/**
 * How to acquire a permit.
 *
 * @param holderId    who is asking; shown in the semaphore's state and audit log
 * @param requestId   idempotency key; generate a fresh one per logical acquire
 * @param ttl         lease duration; null for the semaphore's default
 * @param waitTimeout how long to queue; zero to fail fast
 * @param autoRenew   renew in the background until the lease is closed
 * @param onQueued    called with the queue position each time the service says "still queued"
 */
public record AcquireOptions(
        String holderId,
        String requestId,
        Duration ttl,
        Duration waitTimeout,
        boolean autoRenew,
        IntConsumer onQueued) {

    public AcquireOptions {
        Objects.requireNonNull(holderId, "holderId");
        requestId = requestId == null ? UUID.randomUUID().toString() : requestId;
        waitTimeout = waitTimeout == null ? Duration.ZERO : waitTimeout;
        onQueued = onQueued == null ? position -> { } : onQueued;
    }

    public static AcquireOptions holder(String holderId) {
        return new AcquireOptions(holderId, null, null, Duration.ZERO, true, null);
    }

    public AcquireOptions requestId(String requestId) {
        return new AcquireOptions(holderId, requestId, ttl, waitTimeout, autoRenew, onQueued);
    }

    public AcquireOptions ttl(Duration ttl) {
        return new AcquireOptions(holderId, requestId, ttl, waitTimeout, autoRenew, onQueued);
    }

    public AcquireOptions waitUpTo(Duration waitTimeout) {
        return new AcquireOptions(holderId, requestId, ttl, waitTimeout, autoRenew, onQueued);
    }

    public AcquireOptions autoRenew(boolean autoRenew) {
        return new AcquireOptions(holderId, requestId, ttl, waitTimeout, autoRenew, onQueued);
    }

    public AcquireOptions onQueued(IntConsumer onQueued) {
        return new AcquireOptions(holderId, requestId, ttl, waitTimeout, autoRenew, onQueued);
    }
}
