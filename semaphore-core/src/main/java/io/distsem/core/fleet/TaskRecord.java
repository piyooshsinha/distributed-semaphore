package io.distsem.core.fleet;

import java.time.Instant;
import java.util.Objects;

/**
 * A finished task (one log batch flushed under a permit, or not).
 *
 * @param waitMs time from wanting a permit to getting one (or giving up)
 * @param workMs time spent working while holding the permit
 */
public record TaskRecord(
        String taskId,
        String workerId,
        String description,
        Instant queuedAt,
        Instant finishedAt,
        long waitMs,
        long workMs,
        Long fencingToken,
        TaskOutcome outcome,
        String error) {

    public TaskRecord {
        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(workerId, "workerId");
        Objects.requireNonNull(outcome, "outcome");
    }
}
