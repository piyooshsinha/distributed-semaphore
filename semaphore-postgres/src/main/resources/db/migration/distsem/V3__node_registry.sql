-- Worker nodes report their state in heartbeats; operators queue commands for them.
CREATE TABLE nodes (
    node_id       VARCHAR(128) PRIMARY KEY,
    report        JSONB        NOT NULL,
    first_seen_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_seen_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE node_commands (
    id           BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    node_id      VARCHAR(128) NOT NULL,
    command_type VARCHAR(32)  NOT NULL,
    args         JSONB        NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    acked_at     TIMESTAMPTZ
);

CREATE INDEX node_commands_pending_idx ON node_commands (node_id, id) WHERE acked_at IS NULL;

CREATE FUNCTION node_notify_change() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    PERFORM pg_notify('distsem_nodes', COALESCE(NEW.node_id, OLD.node_id));
    RETURN NULL;
END;
$$;

CREATE TRIGGER nodes_notify_trg
    AFTER INSERT OR UPDATE OR DELETE ON nodes
    FOR EACH ROW EXECUTE FUNCTION node_notify_change();
