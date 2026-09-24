package io.distsem.core.fleet;

import io.distsem.core.Validation;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Everything a node tells the service in a heartbeat.
 *
 * @param settings       current tunables (e.g. work duration), for display
 * @param recentTasks    the node's most recently finished tasks, newest last
 * @param ackedCommands  ids of commands executed since the last heartbeat
 */
public record NodeReport(
        String nodeId,
        String hostname,
        String version,
        Instant startedAt,
        String semaphore,
        boolean paused,
        Map<String, String> settings,
        List<WorkerReport> workers,
        List<TaskRecord> recentTasks,
        List<Long> ackedCommands) {

    public static final int MAX_WORKERS = 256;
    public static final int MAX_RECENT_TASKS = 100;

    public NodeReport {
        Validation.identifier(nodeId, "nodeId", Validation.MAX_NAME_LENGTH);
        Objects.requireNonNull(startedAt, "startedAt");
        settings = settings == null ? Map.of() : Map.copyOf(settings);
        workers = workers == null ? List.of() : List.copyOf(workers);
        recentTasks = recentTasks == null ? List.of() : List.copyOf(recentTasks);
        ackedCommands = ackedCommands == null ? List.of() : List.copyOf(ackedCommands);
        if (workers.size() > MAX_WORKERS || recentTasks.size() > MAX_RECENT_TASKS) {
            throw new IllegalArgumentException(
                    "a report may carry at most " + MAX_WORKERS + " workers and " + MAX_RECENT_TASKS + " tasks");
        }
    }
}
