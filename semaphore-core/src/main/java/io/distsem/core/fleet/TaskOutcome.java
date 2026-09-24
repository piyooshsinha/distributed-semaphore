package io.distsem.core.fleet;

public enum TaskOutcome {
    COMPLETED,
    /** The work itself failed; the permit was still released. */
    FAILED,
    /** The lease expired or could not be renewed while working, so the result was discarded. */
    LEASE_LOST,
    /** The worker crashed or stopped before finishing. */
    ABANDONED,
    /** No permit was granted before the wait deadline. */
    TIMED_OUT
}
