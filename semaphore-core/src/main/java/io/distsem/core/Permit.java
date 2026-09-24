package io.distsem.core;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * A granted slot in a semaphore.
 *
 * <p>The {@code fencingToken} increases strictly with every grant on the same semaphore. Resources
 * protected by the semaphore should remember the highest token they have seen and reject writes
 * carrying a lower one, so a holder whose lease expired while it was paused cannot corrupt state.
 */
public record Permit(
        UUID permitId,
        String semaphore,
        String holderId,
        String requestId,
        long fencingToken,
        Instant acquiredAt,
        Instant expiresAt) {

    public Permit {
        Objects.requireNonNull(permitId, "permitId");
        Objects.requireNonNull(semaphore, "semaphore");
        Objects.requireNonNull(holderId, "holderId");
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(acquiredAt, "acquiredAt");
        Objects.requireNonNull(expiresAt, "expiresAt");
    }
}
