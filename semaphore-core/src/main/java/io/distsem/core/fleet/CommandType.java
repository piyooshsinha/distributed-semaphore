package io.distsem.core.fleet;

/** Operator commands a node picks up with its next heartbeat. */
public enum CommandType {
    /** args: {@code count} - desired number of workers. */
    SCALE_WORKERS,
    PAUSE,
    RESUME,
    /** args: {@code workerId} - crash that worker, abandoning any permit it holds. */
    CRASH_WORKER,
    /** args: {@code minMs}, {@code maxMs} - how long each task holds its permit. */
    SET_WORK_DURATION,
    /** Terminate the node process abruptly, as a machine failure would. */
    KILL_NODE
}
