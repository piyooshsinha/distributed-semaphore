package io.distsem.core.fleet;

import java.time.Instant;
import java.util.Objects;

/** A node as the service sees it: its last report plus liveness. */
public record NodeStatus(NodeReport report, Instant firstSeenAt, Instant lastSeenAt, boolean online) {

    public NodeStatus {
        Objects.requireNonNull(report, "report");
        Objects.requireNonNull(firstSeenAt, "firstSeenAt");
        Objects.requireNonNull(lastSeenAt, "lastSeenAt");
    }
}
