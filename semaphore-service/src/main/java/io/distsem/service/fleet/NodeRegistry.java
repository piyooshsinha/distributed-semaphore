package io.distsem.service.fleet;

import io.distsem.core.fleet.CommandType;
import io.distsem.core.fleet.HeartbeatResponse;
import io.distsem.core.fleet.NodeCommand;
import io.distsem.core.fleet.NodeReport;
import io.distsem.core.fleet.NodeStatus;
import io.distsem.service.config.DistsemProperties;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * Worker nodes and the commands queued for them. Nodes report in with heartbeats; the response
 * carries any commands they have not acknowledged yet (at-least-once delivery).
 */
@Repository
public class NodeRegistry {

    private static final TypeReference<Map<String, String>> ARGS = new TypeReference<>() {
    };

    private final JdbcClient jdbc;
    private final TransactionTemplate tx;
    private final JsonMapper json;
    private final DistsemProperties.Fleet settings;

    public NodeRegistry(JdbcClient jdbc, TransactionTemplate tx, JsonMapper json, DistsemProperties properties) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.json = json;
        this.settings = properties.fleet();
    }

    public HeartbeatResponse heartbeat(NodeReport report) {
        return tx.execute(status -> {
            jdbc.sql("""
                    INSERT INTO nodes (node_id, report) VALUES (:id, :report::jsonb)
                    ON CONFLICT (node_id) DO UPDATE SET report = EXCLUDED.report, last_seen_at = now()""")
                    .param("id", report.nodeId())
                    .param("report", json.writeValueAsString(report))
                    .update();
            if (!report.ackedCommands().isEmpty()) {
                jdbc.sql("""
                        UPDATE node_commands SET acked_at = now()
                        WHERE node_id = :id AND id IN (:ids) AND acked_at IS NULL""")
                        .param("id", report.nodeId())
                        .param("ids", report.ackedCommands())
                        .update();
            }
            List<NodeCommand> pending = jdbc.sql("""
                    SELECT id, node_id, command_type, args, created_at FROM node_commands
                    WHERE node_id = :id AND acked_at IS NULL AND created_at > now() - :ttl::bigint * interval '1 millisecond'
                    ORDER BY id""")
                    .param("id", report.nodeId())
                    .param("ttl", settings.commandTtl().toMillis())
                    .query(this::command)
                    .list();
            return new HeartbeatResponse(pending);
        });
    }

    public List<NodeStatus> list() {
        return jdbc.sql(STATUS_SELECT + " ORDER BY node_id")
                .param("offline", settings.offlineAfter().toMillis())
                .query(this::status)
                .list();
    }

    public Optional<NodeStatus> find(String nodeId) {
        return jdbc.sql(STATUS_SELECT + " WHERE node_id = :id")
                .param("offline", settings.offlineAfter().toMillis())
                .param("id", nodeId)
                .query(this::status)
                .optional();
    }

    public NodeCommand enqueue(String nodeId, CommandType type, Map<String, String> args) {
        return jdbc.sql("""
                INSERT INTO node_commands (node_id, command_type, args) VALUES (:id, :type, :args::jsonb)
                RETURNING id, node_id, command_type, args, created_at""")
                .param("id", nodeId)
                .param("type", type.name())
                .param("args", json.writeValueAsString(args == null ? Map.of() : args))
                .query(this::command)
                .single();
    }

    public boolean forget(String nodeId) {
        return Boolean.TRUE.equals(tx.execute(status -> {
            jdbc.sql("DELETE FROM node_commands WHERE node_id = :id").param("id", nodeId).update();
            return jdbc.sql("DELETE FROM nodes WHERE node_id = :id").param("id", nodeId).update() > 0;
        }));
    }

    /** Removes long-silent nodes and old commands. Returns the number of nodes removed. */
    public int prune() {
        jdbc.sql("DELETE FROM node_commands WHERE created_at < now() - interval '1 day'").update();
        return jdbc.sql("DELETE FROM nodes WHERE last_seen_at < now() - :after::bigint * interval '1 millisecond'")
                .param("after", settings.forgetAfter().toMillis())
                .update();
    }

    private static final String STATUS_SELECT = """
            SELECT node_id, report, first_seen_at, last_seen_at,
                   last_seen_at > now() - :offline::bigint * interval '1 millisecond' AS online
            FROM nodes""";

    private NodeStatus status(ResultSet rs, int row) throws SQLException {
        return new NodeStatus(
                json.readValue(rs.getString("report"), NodeReport.class),
                instant(rs, "first_seen_at"),
                instant(rs, "last_seen_at"),
                rs.getBoolean("online"));
    }

    private NodeCommand command(ResultSet rs, int row) throws SQLException {
        return new NodeCommand(
                rs.getLong("id"),
                rs.getString("node_id"),
                CommandType.valueOf(rs.getString("command_type")),
                json.readValue(rs.getString("args"), ARGS),
                instant(rs, "created_at"));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getObject(column, OffsetDateTime.class).toInstant();
    }

}
