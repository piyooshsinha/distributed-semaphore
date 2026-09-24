package io.distsem.service.fleet;

import io.distsem.core.Validation;
import io.distsem.core.fleet.CommandType;
import io.distsem.core.fleet.HeartbeatResponse;
import io.distsem.core.fleet.NodeCommand;
import io.distsem.core.fleet.NodeReport;
import io.distsem.core.fleet.NodeStatus;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Fleet of worker nodes: heartbeats in, status and operator commands out. */
@RestController
@RequestMapping("/v1/nodes")
@Validated
class NodeController {

    private final NodeRegistry registry;

    NodeController(NodeRegistry registry) {
        this.registry = registry;
    }

    record CommandBody(@NotNull CommandType type, Map<String, String> args) {
    }

    /** Stores the node's report and returns commands it has not acknowledged yet. */
    @PutMapping("/{nodeId}/heartbeat")
    HeartbeatResponse heartbeat(@PathVariable String nodeId, @RequestBody NodeReport report) {
        if (!report.nodeId().equals(nodeId)) {
            throw new IllegalArgumentException("path node id '" + nodeId + "' does not match report '" + report.nodeId() + "'");
        }
        return registry.heartbeat(report);
    }

    @GetMapping
    List<NodeStatus> list() {
        return registry.list();
    }

    @GetMapping("/{nodeId}")
    ResponseEntity<NodeStatus> get(@PathVariable String nodeId) {
        return ResponseEntity.of(registry.find(nodeId));
    }

    /** Queues a command; the node picks it up with its next heartbeat (about once a second). */
    @PostMapping("/{nodeId}/commands")
    ResponseEntity<NodeCommand> command(@PathVariable String nodeId, @RequestBody @jakarta.validation.Valid CommandBody body) {
        Validation.identifier(nodeId, "nodeId", Validation.MAX_NAME_LENGTH);
        if (registry.find(nodeId).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(registry.enqueue(nodeId, body.type(), body.args()));
    }

    @DeleteMapping("/{nodeId}")
    ResponseEntity<Void> forget(@PathVariable String nodeId) {
        return registry.forget(nodeId) ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }
}
