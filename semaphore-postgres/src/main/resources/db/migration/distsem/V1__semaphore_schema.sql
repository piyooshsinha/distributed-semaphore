-- Registry of semaphores. The row is also the per-semaphore mutex: every state-changing
-- operation on a semaphore starts with SELECT ... FOR UPDATE on it.
CREATE TABLE semaphores (
    name           VARCHAR(128) PRIMARY KEY,
    capacity       INTEGER      NOT NULL CHECK (capacity > 0),
    default_ttl_ms BIGINT       NOT NULL CHECK (default_ttl_ms > 0),
    fencing_seq    BIGINT       NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Active permits. A row whose expires_at has passed is logically gone and is deleted by the next
-- operation on the semaphore or by the reaper.
CREATE TABLE semaphore_holders (
    permit_id      UUID         PRIMARY KEY,
    semaphore_name VARCHAR(128) NOT NULL REFERENCES semaphores (name) ON DELETE CASCADE,
    holder_id      VARCHAR(256) NOT NULL,
    request_id     VARCHAR(128) NOT NULL,
    fencing_token  BIGINT       NOT NULL,
    acquired_at    TIMESTAMPTZ  NOT NULL,
    expires_at     TIMESTAMPTZ  NOT NULL,
    CONSTRAINT semaphore_holders_request_uq UNIQUE (semaphore_name, request_id)
);

CREATE INDEX semaphore_holders_expiry_idx ON semaphore_holders (semaphore_name, expires_at);
CREATE INDEX semaphore_holders_global_expiry_idx ON semaphore_holders (expires_at);

-- FIFO wait queue. seq is assigned while the semaphore row is locked, so it matches arrival order.
-- expires_at is a short lease the waiting caller keeps extending by polling; it never exceeds
-- wait_deadline.
CREATE TABLE semaphore_waiters (
    waiter_id      UUID         PRIMARY KEY,
    seq            BIGINT       GENERATED ALWAYS AS IDENTITY,
    semaphore_name VARCHAR(128) NOT NULL REFERENCES semaphores (name) ON DELETE CASCADE,
    holder_id      VARCHAR(256) NOT NULL,
    request_id     VARCHAR(128) NOT NULL,
    enqueued_at    TIMESTAMPTZ  NOT NULL,
    wait_deadline  TIMESTAMPTZ  NOT NULL,
    expires_at     TIMESTAMPTZ  NOT NULL,
    CONSTRAINT semaphore_waiters_request_uq UNIQUE (semaphore_name, request_id),
    CONSTRAINT semaphore_waiters_lease_ck CHECK (expires_at <= wait_deadline)
);

CREATE INDEX semaphore_waiters_queue_idx ON semaphore_waiters (semaphore_name, seq);
CREATE INDEX semaphore_waiters_global_expiry_idx ON semaphore_waiters (expires_at);

-- Append-only audit log. No foreign key, so history survives deletion of the semaphore.
CREATE TABLE semaphore_events (
    id             BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    semaphore_name VARCHAR(128) NOT NULL,
    event_type     VARCHAR(32)  NOT NULL,
    holder_id      VARCHAR(256),
    request_id     VARCHAR(128),
    permit_id      UUID,
    fencing_token  BIGINT,
    detail         TEXT,
    occurred_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX semaphore_events_semaphore_idx ON semaphore_events (semaphore_name, id);
CREATE INDEX semaphore_events_time_idx ON semaphore_events (occurred_at);

-- Defence in depth: refuse any insert that would push the holder count above capacity, even if
-- application code has a bug. Locking the semaphore row serialises concurrent inserts, so the count
-- below always sees every committed holder. The application already holds this lock, so for it the
-- lock is re-entrant and free.
CREATE FUNCTION semaphore_enforce_capacity() RETURNS trigger
    LANGUAGE plpgsql AS
$$
DECLARE
    cap  INTEGER;
    held INTEGER;
BEGIN
    SELECT capacity INTO cap FROM semaphores WHERE name = NEW.semaphore_name FOR UPDATE;
    SELECT count(*) INTO held FROM semaphore_holders WHERE semaphore_name = NEW.semaphore_name;
    IF held > cap THEN
        RAISE EXCEPTION 'semaphore % over capacity: % holders, capacity %', NEW.semaphore_name, held, cap
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER semaphore_holders_capacity_trg
    AFTER INSERT ON semaphore_holders
    FOR EACH ROW EXECUTE FUNCTION semaphore_enforce_capacity();
