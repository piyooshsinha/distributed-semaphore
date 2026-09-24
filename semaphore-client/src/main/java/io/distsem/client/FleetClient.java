package io.distsem.client;

import io.distsem.client.Transport.Retry;
import io.distsem.core.fleet.CommandType;
import io.distsem.core.fleet.HeartbeatResponse;
import io.distsem.core.fleet.NodeCommand;
import io.distsem.core.fleet.NodeReport;
import io.distsem.core.fleet.NodeStatus;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import tools.jackson.core.type.TypeReference;

/** Node registry API: heartbeats for worker nodes, status and commands for operators. */
public final class FleetClient {

    private static final TypeReference<List<NodeStatus>> NODE_LIST = new TypeReference<>() {
    };

    private final SemaphoreClient client;

    FleetClient(SemaphoreClient client) {
        this.client = client;
    }

    /** Reports the node's state; returns commands it has not acknowledged yet. */
    public HeartbeatResponse heartbeat(NodeReport report) throws InterruptedException {
        Transport t = client.transport();
        return t.read(t.send("PUT", nodePath(report.nodeId()) + "/heartbeat", report,
                client.requestTimeout(), Retry.IDEMPOTENT), HeartbeatResponse.class);
    }

    public List<NodeStatus> nodes() throws InterruptedException {
        Transport t = client.transport();
        return t.read(t.get("/v1/nodes"), NODE_LIST);
    }

    /** Queues a command for a node. Not retried after a request may have reached the server. */
    public NodeCommand command(String nodeId, CommandType type, Map<String, String> args) throws InterruptedException {
        Transport t = client.transport();
        return t.read(t.send("POST", nodePath(nodeId) + "/commands", Map.of("type", type, "args", args),
                client.requestTimeout(), Retry.CONNECT_ONLY), NodeCommand.class);
    }

    private static String nodePath(String nodeId) {
        return "/v1/nodes/" + URLEncoder.encode(nodeId, StandardCharsets.UTF_8);
    }
}
