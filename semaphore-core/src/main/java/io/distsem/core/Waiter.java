package io.distsem.core;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * A caller queued for a permit, in FIFO order.
 *
 * @param waitDeadline when the caller gives up waiting
 * @param expiresAt    when the entry is dropped if the caller stops polling; never after {@code waitDeadline}
 */
public record Waiter(
        UUID waiterId,
        String semaphore,
        String holderId,
        String requestId,
        Instant enqueuedAt,
        Instant waitDeadline,
        Instant expiresAt) {

    public Waiter {
        Objects.requireNonNull(waiterId, "waiterId");
        Objects.requireNonNull(semaphore, "semaphore");
        Objects.requireNonNull(holderId, "holderId");
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(enqueuedAt, "enqueuedAt");
        Objects.requireNonNull(waitDeadline, "waitDeadline");
        Objects.requireNonNull(expiresAt, "expiresAt");
    }
}
