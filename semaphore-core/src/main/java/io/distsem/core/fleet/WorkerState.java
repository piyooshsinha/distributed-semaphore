package io.distsem.core.fleet;

/** Lifecycle of a worker as reported by its node. */
public enum WorkerState {
    /** Starting up, or restarting after a simulated crash. */
    STARTING,
    /** Between tasks. */
    IDLE,
    /** Has a task and is queued for a permit. */
    WAITING,
    /** Holds a permit and is flushing its task. */
    WORKING,
    /** Finished its task and is releasing the permit. */
    RELEASING,
    /** Paused by an operator; finishes nothing new until resumed. */
    PAUSED,
    /** Simulated crash: it abandoned its permit without releasing it and will restart. */
    CRASHED,
    /** Shut down. */
    STOPPED
}
