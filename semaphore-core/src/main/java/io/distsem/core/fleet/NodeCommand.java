package io.distsem.core.fleet;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** A queued command. Delivered at least once; nodes must ignore ids they have already executed. */
public record NodeCommand(long id, String nodeId, CommandType type, Map<String, String> args, Instant createdAt) {

    public NodeCommand {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(type, "type");
        args = args == null ? Map.of() : Map.copyOf(args);
    }

    public String arg(String name) {
        String value = args.get(name);
        if (value == null) {
            throw new IllegalArgumentException(type + " requires argument '" + name + "'");
        }
        return value;
    }
}
