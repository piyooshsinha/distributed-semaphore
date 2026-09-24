package io.distsem.core;

/** Kinds of entries in the audit log. */
public enum EventType {
    CREATED,
    UPDATED,
    DELETED,
    QUEUED,
    ACQUIRED,
    RENEWED,
    RELEASED,
    /** A permit's lease ran out before it was released. */
    EXPIRED,
    /** A waiter's deadline passed before it was granted. */
    WAIT_TIMEOUT,
    /** A waiter stopped polling and its entry lapsed. */
    WAIT_ABANDONED,
    WAIT_CANCELLED
}
