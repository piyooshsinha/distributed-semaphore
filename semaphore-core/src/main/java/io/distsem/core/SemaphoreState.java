package io.distsem.core;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Consistent snapshot of one semaphore.
 *
 * @param holders     live permits, oldest first
 * @param waiters     live waiters in queue order
 * @param lastFencingToken highest fencing token issued so far
 * @param observedAt  store clock at the time of the snapshot
 */
public record SemaphoreState(
        SemaphoreConfig config,
        List<Permit> holders,
        List<Waiter> waiters,
        long lastFencingToken,
        Instant observedAt) {

    public SemaphoreState {
        Objects.requireNonNull(config, "config");
        holders = List.copyOf(holders);
        waiters = List.copyOf(waiters);
        Objects.requireNonNull(observedAt, "observedAt");
    }

    /** Free slots; zero when capacity was lowered below the number of current holders. */
    public int available() {
        return Math.max(0, config.capacity() - holders.size());
    }
}
