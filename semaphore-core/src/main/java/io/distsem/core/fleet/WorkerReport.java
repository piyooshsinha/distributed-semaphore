package io.distsem.core.fleet;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Point-in-time view of one worker. Fields that do not apply to the current state are null.
 *
 * @param queuePosition waiters ahead of this worker while {@link WorkerState#WAITING}
 */
public record WorkerReport(
        String workerId,
        WorkerState state,
        Instant stateSince,
        String currentTask,
        UUID permitId,
        Long fencingToken,
        Integer queuePosition,
        long tasksCompleted,
        long tasksFailed,
        long leasesLost) {

    public WorkerReport {
        Objects.requireNonNull(workerId, "workerId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(stateSince, "stateSince");
    }
}
