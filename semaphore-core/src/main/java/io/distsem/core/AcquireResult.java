package io.distsem.core;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Outcome of a single, non-blocking acquire attempt. */
public sealed interface AcquireResult {

    /** A permit was granted (or had already been granted to this {@code requestId}). */
    record Granted(Permit permit) implements AcquireResult {
        public Granted {
            Objects.requireNonNull(permit, "permit");
        }
    }

    /**
     * No slot is free for this caller yet; it holds a place in the queue. The caller should poll
     * again with the same {@code requestId} before {@code expiresAt} to keep its place.
     *
     * @param position number of waiters ahead of this one (0 = head of the queue)
     */
    record Queued(UUID waiterId, int position, Instant waitDeadline, Instant expiresAt) implements AcquireResult {
        public Queued {
            Objects.requireNonNull(waiterId, "waiterId");
            Objects.requireNonNull(waitDeadline, "waitDeadline");
            Objects.requireNonNull(expiresAt, "expiresAt");
        }
    }

    /**
     * No slot is free and the caller did not ask to wait, or its wait deadline has passed.
     *
     * @param available free slots at the time of the attempt
     * @param waiting   callers queued at the time of the attempt
     */
    record Rejected(int available, int waiting, Reason reason) implements AcquireResult {
        public Rejected {
            Objects.requireNonNull(reason, "reason");
        }
    }

    enum Reason {
        /** Every slot is held, or free slots are reserved for callers already queued. */
        NO_CAPACITY,
        /** The caller had queued and its wait deadline passed before a slot became free. */
        WAIT_TIMEOUT
    }
}
