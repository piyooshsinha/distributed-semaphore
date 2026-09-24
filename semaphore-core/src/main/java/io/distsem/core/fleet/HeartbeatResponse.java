package io.distsem.core.fleet;

import java.util.List;

/** Commands waiting for the node, oldest first. */
public record HeartbeatResponse(List<NodeCommand> commands) {

    public HeartbeatResponse {
        commands = commands == null ? List.of() : List.copyOf(commands);
    }
}
