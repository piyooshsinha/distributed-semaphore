-- Publish every audit event on a LISTEN/NOTIFY channel so that waiters and event streams on any
-- replica wake up as soon as the change commits. Notifications are delivered only on commit.
-- Payload: <semaphore>|<event id>|<event type>. Semaphore names cannot contain '|'.
CREATE FUNCTION semaphore_notify_event() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    PERFORM pg_notify('distsem_events', NEW.semaphore_name || '|' || NEW.id || '|' || NEW.event_type);
    RETURN NULL;
END;
$$;

CREATE TRIGGER semaphore_events_notify_trg
    AFTER INSERT ON semaphore_events
    FOR EACH ROW EXECUTE FUNCTION semaphore_notify_event();
