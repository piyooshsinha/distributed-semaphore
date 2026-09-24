package io.distsem.postgres;

import io.distsem.core.Validation;
import java.time.Duration;

/**
 * Tuning for {@link PostgresSemaphoreStore}.
 *
 * @param waiterLease      how long a queued caller keeps its place without polling again
 * @param maxTtl           upper bound on a permit's lease
 * @param maxWaitTimeout   upper bound on how long a caller may queue
 * @param lockTimeout      how long a transaction may wait for a semaphore's row lock
 * @param maxRetries       retries for deadlocks and serialization failures (the transaction is rolled back, so this is safe)
 */
public record PostgresStoreOptions(
        Duration waiterLease,
        Duration maxTtl,
        Duration maxWaitTimeout,
        Duration lockTimeout,
        int maxRetries) {

    public PostgresStoreOptions {
        Validation.positive(waiterLease, "waiterLease");
        Validation.positive(maxTtl, "maxTtl");
        Validation.positive(maxWaitTimeout, "maxWaitTimeout");
        Validation.positive(lockTimeout, "lockTimeout");
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must not be negative");
        }
    }

    public static PostgresStoreOptions defaults() {
        return new PostgresStoreOptions(
                Duration.ofSeconds(10), Duration.ofHours(1), Duration.ofMinutes(10), Duration.ofSeconds(5), 3);
    }

    public PostgresStoreOptions withWaiterLease(Duration waiterLease) {
        return new PostgresStoreOptions(waiterLease, maxTtl, maxWaitTimeout, lockTimeout, maxRetries);
    }
}
