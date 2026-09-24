package io.distsem.core;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Durable, shared state of all semaphores. Implementations must be safe to use from many threads
 * and from many processes sharing the same backing store.
 *
 * <p>Invariant: for every semaphore, the number of unexpired permits never exceeds its capacity at
 * the moment each permit is granted. Lowering capacity does not revoke permits already held.
 */
public interface SemaphoreStore {

    /**
     * Creates a semaphore. Creating one that already exists with identical settings is a no-op.
     *
     * @throws SemaphoreAlreadyExistsException if it exists with different settings
     */
    SemaphoreConfig create(SemaphoreConfig config);

    /** Changes capacity and default TTL. Current holders keep their permits. */
    SemaphoreConfig update(SemaphoreConfig config);

    /** Deletes a semaphore with all its permits and waiters. Returns false if it did not exist. */
    boolean delete(String name);

    Optional<SemaphoreConfig> find(String name);

    List<SemaphoreConfig> list();

    /**
     * Makes one non-blocking attempt to acquire a permit. Evicts expired permits and waiters first.
     *
     * <p>Grants are FIFO: a caller is granted only if fewer callers are queued ahead of it than
     * there are free slots. Callers that asked to wait and could not be granted are queued and must
     * poll again with the same {@code requestId} to keep their place.
     *
     * @throws SemaphoreNotFoundException if the semaphore does not exist
     */
    AcquireResult tryAcquire(AcquireRequest request);

    /**
     * Extends a live permit so it expires {@code ttl} from now.
     *
     * @throws PermitNotFoundException if the permit was released or has expired
     */
    Permit renew(String semaphore, UUID permitId, Duration ttl);

    /** Releases a permit. Idempotent: returns false if it was not held (already released or expired). */
    boolean release(String semaphore, UUID permitId);

    /** Removes a queued caller. Returns false if it was not queued. */
    boolean cancelWait(String semaphore, String requestId);

    /**
     * Snapshot of holders and waiters, excluding entries that have expired but not yet been evicted.
     *
     * @throws SemaphoreNotFoundException if the semaphore does not exist
     */
    SemaphoreState state(String name);

    /** Evicts expired permits and waiters across all semaphores. Returns how many were removed. */
    int reapExpired();

    /** Audit events for one semaphore with {@code id > afterId}, oldest first. */
    List<SemaphoreEvent> events(String semaphore, long afterId, int limit);

    /** The newest {@code limit} audit events for one semaphore, oldest first. */
    List<SemaphoreEvent> latestEvents(String semaphore, int limit);

    /** Deletes audit events older than {@code retention}. Returns how many were removed. */
    int pruneEvents(Duration retention);
}
