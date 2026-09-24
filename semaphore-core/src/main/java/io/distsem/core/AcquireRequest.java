package io.distsem.core;

import java.time.Duration;
import java.util.Optional;

/**
 * A request for one permit.
 *
 * <p>{@code requestId} is chosen by the caller and must stay the same across retries and re-polls
 * of the same logical acquire. It makes the call idempotent and identifies the caller's place in
 * the wait queue.
 *
 * @param ttl         lease duration; empty to use the semaphore's default
 * @param waitTimeout how long the caller is willing to queue; zero means fail fast
 */
public record AcquireRequest(
        String semaphore,
        String holderId,
        String requestId,
        Optional<Duration> ttl,
        Duration waitTimeout) {

    public AcquireRequest {
        Validation.name(semaphore);
        Validation.identifier(holderId, "holderId", Validation.MAX_HOLDER_ID_LENGTH);
        Validation.identifier(requestId, "requestId", Validation.MAX_REQUEST_ID_LENGTH);
        ttl = ttl == null ? Optional.empty() : ttl;
        ttl.ifPresent(t -> Validation.positive(t, "ttl"));
        Validation.nonNegative(waitTimeout, "waitTimeout");
    }

    public static AcquireRequest tryOnce(String semaphore, String holderId, String requestId) {
        return new AcquireRequest(semaphore, holderId, requestId, Optional.empty(), Duration.ZERO);
    }

    public AcquireRequest withTtl(Duration ttl) {
        return new AcquireRequest(semaphore, holderId, requestId, Optional.of(ttl), waitTimeout);
    }

    public AcquireRequest withWaitTimeout(Duration waitTimeout) {
        return new AcquireRequest(semaphore, holderId, requestId, ttl, waitTimeout);
    }
}
