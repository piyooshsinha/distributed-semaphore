package io.distsem.postgres;

import io.distsem.core.AcquireRequest;
import io.distsem.core.AcquireResult;
import io.distsem.core.EventType;
import io.distsem.core.Permit;
import io.distsem.core.PermitNotFoundException;
import io.distsem.core.SemaphoreAlreadyExistsException;
import io.distsem.core.SemaphoreConfig;
import io.distsem.core.SemaphoreEvent;
import io.distsem.core.SemaphoreNotFoundException;
import io.distsem.core.SemaphoreState;
import io.distsem.core.SemaphoreStore;
import io.distsem.core.Validation;
import io.distsem.core.Waiter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SemaphoreStore} on PostgreSQL.
 *
 * <p>Every state-changing operation on a semaphore runs in one transaction that first locks the
 * semaphore's registry row ({@code SELECT ... FOR UPDATE}). That serialises changes per semaphore
 * across all processes sharing the database, so "evict expired, count, insert if below capacity"
 * is atomic. It also means a semaphore's audit events commit in id order, so readers can tail the
 * log with {@code id > lastSeen} without missing entries.
 *
 * <p>All timestamps come from the database clock, so replicas with skewed clocks agree on expiry.
 */
public final class PostgresSemaphoreStore implements SemaphoreStore {

    private static final Logger log = LoggerFactory.getLogger(PostgresSemaphoreStore.class);

    private static final String HOLDER_COLUMNS =
            "permit_id, semaphore_name, holder_id, request_id, fencing_token, acquired_at, expires_at";
    private static final String WAITER_COLUMNS =
            "waiter_id, semaphore_name, holder_id, request_id, enqueued_at, wait_deadline, expires_at";
    private static final String EVENT_SELECT = """
            SELECT id, semaphore_name, event_type, holder_id, request_id, permit_id, fencing_token, detail, occurred_at
            FROM semaphore_events""";
    private static final String MILLIS = "(?::bigint * interval '1 millisecond')";

    private final Transactions tx;
    private final PostgresStoreOptions options;

    public PostgresSemaphoreStore(DataSource dataSource) {
        this(dataSource, PostgresStoreOptions.defaults());
    }

    public PostgresSemaphoreStore(DataSource dataSource, PostgresStoreOptions options) {
        this.options = Objects.requireNonNull(options, "options");
        this.tx = new Transactions(Objects.requireNonNull(dataSource, "dataSource"), options);
    }

    // ---------------------------------------------------------------- registry

    @Override
    public SemaphoreConfig create(SemaphoreConfig requested) {
        SemaphoreConfig config = normalise(requested);
        return tx.write(c -> {
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO semaphores (name, capacity, default_ttl_ms) VALUES (?, ?, ?)
                    ON CONFLICT (name) DO NOTHING""")) {
                ps.setString(1, config.name());
                ps.setInt(2, config.capacity());
                ps.setLong(3, config.defaultTtl().toMillis());
                if (ps.executeUpdate() == 1) {
                    insertEvent(c, config.name(), EventType.CREATED, null, null, null, null, describe(config));
                    return config;
                }
            }
            SemaphoreConfig existing = findConfig(c, config.name(), false)
                    .orElseThrow(() -> new SemaphoreNotFoundException(config.name()));
            if (!existing.equals(config)) {
                throw new SemaphoreAlreadyExistsException(existing);
            }
            return existing;
        });
    }

    @Override
    public SemaphoreConfig update(SemaphoreConfig requested) {
        SemaphoreConfig config = normalise(requested);
        return tx.write(c -> {
            SemaphoreConfig before = lock(c, config.name()).config();
            try (PreparedStatement ps = c.prepareStatement("""
                    UPDATE semaphores SET capacity = ?, default_ttl_ms = ?, updated_at = now() WHERE name = ?""")) {
                ps.setInt(1, config.capacity());
                ps.setLong(2, config.defaultTtl().toMillis());
                ps.setString(3, config.name());
                ps.executeUpdate();
            }
            if (!before.equals(config)) {
                insertEvent(c, config.name(), EventType.UPDATED, null, null, null, null,
                        describe(before) + " -> " + describe(config));
            }
            return config;
        });
    }

    @Override
    public boolean delete(String name) {
        Validation.name(name);
        return tx.write(c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM semaphores WHERE name = ?")) {
                ps.setString(1, name);
                if (ps.executeUpdate() == 0) {
                    return false;
                }
            }
            insertEvent(c, name, EventType.DELETED, null, null, null, null, null);
            return true;
        });
    }

    @Override
    public Optional<SemaphoreConfig> find(String name) {
        Validation.name(name);
        return tx.read(c -> findConfig(c, name, false));
    }

    @Override
    public List<SemaphoreConfig> list() {
        return tx.read(c -> {
            List<SemaphoreConfig> result = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT name, capacity, default_ttl_ms FROM semaphores ORDER BY name");
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(config(rs));
                }
            }
            return result;
        });
    }

    // ---------------------------------------------------------------- permits

    @Override
    public AcquireResult tryAcquire(AcquireRequest request) {
        Duration requestedTtl = request.ttl().map(this::checkTtl).orElse(null);
        checkWaitTimeout(request.waitTimeout());
        String name = request.semaphore();

        return tx.write(c -> {
            Locked sem = lock(c, name);

            // A retry of a request that was already granted gets the same permit back.
            Optional<Permit> alreadyHeld = findLiveHolderByRequest(c, name, request.requestId());
            if (alreadyHeld.isPresent()) {
                return new AcquireResult.Granted(alreadyHeld.get());
            }

            evictExpiredHolders(c, name);
            // The caller's own lapsed waiter entry is kept so it does not lose its place by polling late.
            evictExpiredWaiters(c, name, request.requestId());

            Optional<QueuedEntry> own = findWaiter(c, name, request.requestId());
            if (own.isPresent() && own.get().deadlinePassed()) {
                deleteWaiter(c, own.get().waiter(), EventType.WAIT_TIMEOUT);
                return new AcquireResult.Rejected(
                        available(sem, countHolders(c, name)), countWaiters(c, name), AcquireResult.Reason.WAIT_TIMEOUT);
            }

            int held = countHolders(c, name);
            int available = available(sem, held);
            int ahead = own.isPresent() ? countWaitersAhead(c, name, own.get().seq()) : countWaiters(c, name);

            if (ahead < available) {
                if (own.isPresent()) {
                    deleteWaiterById(c, own.get().waiter().waiterId());
                }
                Duration ttl = requestedTtl != null ? requestedTtl : sem.config().defaultTtl();
                Permit permit = grant(c, request, ttl);
                insertEvent(c, name, EventType.ACQUIRED, permit.holderId(), permit.requestId(), permit.permitId(),
                        permit.fencingToken(), "ttl=" + ttl.toMillis() + "ms");
                return new AcquireResult.Granted(permit);
            }

            if (own.isPresent()) {
                Waiter refreshed = refreshWaiterLease(c, own.get().waiter().waiterId());
                return new AcquireResult.Queued(refreshed.waiterId(), ahead, refreshed.waitDeadline(), refreshed.expiresAt());
            }
            if (request.waitTimeout().isZero()) {
                return new AcquireResult.Rejected(available, ahead, AcquireResult.Reason.NO_CAPACITY);
            }
            Waiter waiter = enqueue(c, request);
            insertEvent(c, name, EventType.QUEUED, waiter.holderId(), waiter.requestId(), null, null,
                    "position=" + ahead + ", waitTimeout=" + request.waitTimeout().toMillis() + "ms");
            return new AcquireResult.Queued(waiter.waiterId(), ahead, waiter.waitDeadline(), waiter.expiresAt());
        });
    }

    @Override
    public Permit renew(String semaphore, UUID permitId, Duration ttl) {
        Validation.name(semaphore);
        Objects.requireNonNull(permitId, "permitId");
        Duration checkedTtl = checkTtl(ttl);
        return tx.write(c -> {
            if (lockIfExists(c, semaphore).isEmpty()) {
                throw new PermitNotFoundException(semaphore, permitId);
            }
            Permit permit;
            try (PreparedStatement ps = c.prepareStatement("UPDATE semaphore_holders SET expires_at = now() + " + MILLIS
                    + " WHERE semaphore_name = ? AND permit_id = ? AND expires_at > now() RETURNING " + HOLDER_COLUMNS)) {
                ps.setLong(1, checkedTtl.toMillis());
                ps.setString(2, semaphore);
                ps.setObject(3, permitId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new PermitNotFoundException(semaphore, permitId);
                    }
                    permit = permit(rs);
                }
            }
            insertEvent(c, semaphore, EventType.RENEWED, permit.holderId(), permit.requestId(), permitId,
                    permit.fencingToken(), "ttl=" + checkedTtl.toMillis() + "ms");
            return permit;
        });
    }

    @Override
    public boolean release(String semaphore, UUID permitId) {
        Validation.name(semaphore);
        Objects.requireNonNull(permitId, "permitId");
        return tx.write(c -> {
            if (lockIfExists(c, semaphore).isEmpty()) {
                return false;
            }
            try (PreparedStatement ps = c.prepareStatement("""
                    WITH gone AS (
                        DELETE FROM semaphore_holders
                        WHERE semaphore_name = ? AND permit_id = ? AND expires_at > now()
                        RETURNING *)
                    INSERT INTO semaphore_events
                        (semaphore_name, event_type, holder_id, request_id, permit_id, fencing_token, detail)
                    SELECT semaphore_name, 'RELEASED', holder_id, request_id, permit_id, fencing_token,
                           'held ' || floor(extract(epoch FROM now() - acquired_at) * 1000)::bigint || 'ms'
                    FROM gone""")) {
                ps.setString(1, semaphore);
                ps.setObject(2, permitId);
                return ps.executeUpdate() > 0;
            }
        });
    }

    @Override
    public boolean cancelWait(String semaphore, String requestId) {
        Validation.name(semaphore);
        Validation.identifier(requestId, "requestId", Validation.MAX_REQUEST_ID_LENGTH);
        return tx.write(c -> {
            if (lockIfExists(c, semaphore).isEmpty()) {
                return false;
            }
            try (PreparedStatement ps = c.prepareStatement("""
                    WITH gone AS (
                        DELETE FROM semaphore_waiters WHERE semaphore_name = ? AND request_id = ? RETURNING *)
                    INSERT INTO semaphore_events (semaphore_name, event_type, holder_id, request_id)
                    SELECT semaphore_name, 'WAIT_CANCELLED', holder_id, request_id FROM gone""")) {
                ps.setString(1, semaphore);
                ps.setString(2, requestId);
                return ps.executeUpdate() > 0;
            }
        });
    }

    // ---------------------------------------------------------------- observation & housekeeping

    @Override
    public SemaphoreState state(String name) {
        Validation.name(name);
        return tx.read(c -> {
            SemaphoreConfig config;
            long fencingSeq;
            Instant now;
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT name, capacity, default_ttl_ms, fencing_seq, now() AS db_now FROM semaphores WHERE name = ?")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new SemaphoreNotFoundException(name);
                    }
                    config = config(rs);
                    fencingSeq = rs.getLong("fencing_seq");
                    now = instant(rs, "db_now");
                }
            }
            List<Permit> holders = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT " + HOLDER_COLUMNS + """
                     FROM semaphore_holders WHERE semaphore_name = ? AND expires_at > now()
                    ORDER BY fencing_token""")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        holders.add(permit(rs));
                    }
                }
            }
            List<Waiter> waiters = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT " + WAITER_COLUMNS + """
                     FROM semaphore_waiters WHERE semaphore_name = ? AND expires_at > now()
                    ORDER BY seq""")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        waiters.add(waiter(rs));
                    }
                }
            }
            return new SemaphoreState(config, holders, waiters, fencingSeq, now);
        });
    }

    @Override
    public int reapExpired() {
        List<String> candidates = tx.read(c -> {
            List<String> names = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("""
                    SELECT semaphore_name FROM semaphore_holders WHERE expires_at <= now()
                    UNION
                    SELECT semaphore_name FROM semaphore_waiters WHERE expires_at <= now()""");
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    names.add(rs.getString(1));
                }
            }
            return names;
        });
        int removed = 0;
        for (String name : candidates) {
            removed += tx.write(c -> {
                // A semaphore that is busy right now is skipped: its own acquires evict as they go.
                try (PreparedStatement ps = c.prepareStatement(
                        "SELECT 1 FROM semaphores WHERE name = ? FOR UPDATE SKIP LOCKED")) {
                    ps.setString(1, name);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            return 0;
                        }
                    }
                }
                return evictExpiredHolders(c, name) + evictExpiredWaiters(c, name, null);
            });
        }
        if (removed > 0) {
            log.debug("Reaped {} expired permits/waiters across {} semaphores", removed, candidates.size());
        }
        return removed;
    }

    @Override
    public List<SemaphoreEvent> events(String semaphore, long afterId, int limit) {
        Validation.name(semaphore);
        checkLimit(limit);
        return tx.read(c -> queryEvents(c, EVENT_SELECT + " WHERE semaphore_name = ? AND id > ? ORDER BY id LIMIT ?",
                ps -> {
                    ps.setString(1, semaphore);
                    ps.setLong(2, afterId);
                    ps.setInt(3, limit);
                }));
    }

    @Override
    public List<SemaphoreEvent> latestEvents(String semaphore, int limit) {
        Validation.name(semaphore);
        checkLimit(limit);
        return tx.read(c -> queryEvents(c, "SELECT * FROM (" + EVENT_SELECT
                + " WHERE semaphore_name = ? ORDER BY id DESC LIMIT ?) newest ORDER BY id", ps -> {
                    ps.setString(1, semaphore);
                    ps.setInt(2, limit);
                }));
    }

    @Override
    public int pruneEvents(Duration retention) {
        Validation.positive(retention, "retention");
        return tx.write(c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "DELETE FROM semaphore_events WHERE occurred_at < now() - " + MILLIS)) {
                ps.setLong(1, retention.toMillis());
                return ps.executeUpdate();
            }
        });
    }

    // ---------------------------------------------------------------- SQL helpers

    private record Locked(SemaphoreConfig config) {
    }

    private record QueuedEntry(Waiter waiter, long seq, boolean deadlinePassed) {
    }

    private static Locked lock(Connection c, String name) throws SQLException {
        return lockIfExists(c, name).orElseThrow(() -> new SemaphoreNotFoundException(name));
    }

    private static Optional<Locked> lockIfExists(Connection c, String name) throws SQLException {
        return findConfig(c, name, true).map(Locked::new);
    }

    private static Optional<SemaphoreConfig> findConfig(Connection c, String name, boolean forUpdate)
            throws SQLException {
        String sql = "SELECT name, capacity, default_ttl_ms FROM semaphores WHERE name = ?"
                + (forUpdate ? " FOR UPDATE" : "");
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(config(rs)) : Optional.empty();
            }
        }
    }

    private static Optional<Permit> findLiveHolderByRequest(Connection c, String name, String requestId)
            throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT " + HOLDER_COLUMNS
                + " FROM semaphore_holders WHERE semaphore_name = ? AND request_id = ? AND expires_at > now()")) {
            ps.setString(1, name);
            ps.setString(2, requestId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(permit(rs)) : Optional.empty();
            }
        }
    }

    private static int evictExpiredHolders(Connection c, String name) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                WITH gone AS (
                    DELETE FROM semaphore_holders WHERE semaphore_name = ? AND expires_at <= now() RETURNING *)
                INSERT INTO semaphore_events
                    (semaphore_name, event_type, holder_id, request_id, permit_id, fencing_token, detail)
                SELECT semaphore_name, 'EXPIRED', holder_id, request_id, permit_id, fencing_token,
                       'lease expired at ' || expires_at
                FROM gone""")) {
            ps.setString(1, name);
            return ps.executeUpdate();
        }
    }

    /** Evicts lapsed waiters, except the one for {@code keepRequestId} when it is non-null. */
    private static int evictExpiredWaiters(Connection c, String name, String keepRequestId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                WITH gone AS (
                    DELETE FROM semaphore_waiters
                    WHERE semaphore_name = ? AND expires_at <= now() AND request_id IS DISTINCT FROM ?::varchar
                    RETURNING *)
                INSERT INTO semaphore_events (semaphore_name, event_type, holder_id, request_id, detail)
                SELECT semaphore_name,
                       CASE WHEN wait_deadline <= now() THEN 'WAIT_TIMEOUT' ELSE 'WAIT_ABANDONED' END,
                       holder_id, request_id,
                       'waited ' || floor(extract(epoch FROM now() - enqueued_at) * 1000)::bigint || 'ms'
                FROM gone""")) {
            ps.setString(1, name);
            if (keepRequestId == null) {
                ps.setNull(2, Types.VARCHAR);
            } else {
                ps.setString(2, keepRequestId);
            }
            return ps.executeUpdate();
        }
    }

    private static Optional<QueuedEntry> findWaiter(Connection c, String name, String requestId)
            throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT " + WAITER_COLUMNS
                + ", seq, wait_deadline <= now() AS deadline_passed"
                + " FROM semaphore_waiters WHERE semaphore_name = ? AND request_id = ?")) {
            ps.setString(1, name);
            ps.setString(2, requestId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next()
                        ? Optional.of(new QueuedEntry(waiter(rs), rs.getLong("seq"), rs.getBoolean("deadline_passed")))
                        : Optional.empty();
            }
        }
    }

    private static int countHolders(Connection c, String name) throws SQLException {
        return count(c, "SELECT count(*) FROM semaphore_holders WHERE semaphore_name = ?", name, null);
    }

    private static int countWaiters(Connection c, String name) throws SQLException {
        return count(c, "SELECT count(*) FROM semaphore_waiters WHERE semaphore_name = ?", name, null);
    }

    private static int countWaitersAhead(Connection c, String name, long seq) throws SQLException {
        return count(c, "SELECT count(*) FROM semaphore_waiters WHERE semaphore_name = ? AND seq < ?", name, seq);
    }

    private static int count(Connection c, String sql, String name, Long seq) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, name);
            if (seq != null) {
                ps.setLong(2, seq);
            }
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private static int available(Locked sem, int held) {
        return Math.max(0, sem.config().capacity() - held);
    }

    private static Permit grant(Connection c, AcquireRequest request, Duration ttl) throws SQLException {
        long token;
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE semaphores SET fencing_seq = fencing_seq + 1 WHERE name = ? RETURNING fencing_seq")) {
            ps.setString(1, request.semaphore());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                token = rs.getLong(1);
            }
        }
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO semaphore_holders (" + HOLDER_COLUMNS
                + ") VALUES (?, ?, ?, ?, ?, now(), now() + " + MILLIS + ") RETURNING " + HOLDER_COLUMNS)) {
            ps.setObject(1, UUID.randomUUID());
            ps.setString(2, request.semaphore());
            ps.setString(3, request.holderId());
            ps.setString(4, request.requestId());
            ps.setLong(5, token);
            ps.setLong(6, ttl.toMillis());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return permit(rs);
            }
        }
    }

    private Waiter enqueue(Connection c, AcquireRequest request) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO semaphore_waiters ("
                + "waiter_id, semaphore_name, holder_id, request_id, enqueued_at, wait_deadline, expires_at)"
                + " VALUES (?, ?, ?, ?, now(), now() + " + MILLIS + ", now() + LEAST(" + MILLIS + ", " + MILLIS + "))"
                + " RETURNING " + WAITER_COLUMNS)) {
            ps.setObject(1, UUID.randomUUID());
            ps.setString(2, request.semaphore());
            ps.setString(3, request.holderId());
            ps.setString(4, request.requestId());
            ps.setLong(5, request.waitTimeout().toMillis());
            ps.setLong(6, request.waitTimeout().toMillis());
            ps.setLong(7, options.waiterLease().toMillis());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return waiter(rs);
            }
        }
    }

    private Waiter refreshWaiterLease(Connection c, UUID waiterId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("UPDATE semaphore_waiters"
                + " SET expires_at = LEAST(wait_deadline, now() + " + MILLIS + ")"
                + " WHERE waiter_id = ? RETURNING " + WAITER_COLUMNS)) {
            ps.setLong(1, options.waiterLease().toMillis());
            ps.setObject(2, waiterId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return waiter(rs);
            }
        }
    }

    private static void deleteWaiter(Connection c, Waiter waiter, EventType reason) throws SQLException {
        deleteWaiterById(c, waiter.waiterId());
        insertEvent(c, waiter.semaphore(), reason, waiter.holderId(), waiter.requestId(), null, null, null);
    }

    private static void deleteWaiterById(Connection c, UUID waiterId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM semaphore_waiters WHERE waiter_id = ?")) {
            ps.setObject(1, waiterId);
            ps.executeUpdate();
        }
    }

    private static void insertEvent(Connection c, String semaphore, EventType type, String holderId,
            String requestId, UUID permitId, Long fencingToken, String detail) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO semaphore_events
                    (semaphore_name, event_type, holder_id, request_id, permit_id, fencing_token, detail)
                VALUES (?, ?, ?, ?, ?, ?, ?)""")) {
            ps.setString(1, semaphore);
            ps.setString(2, type.name());
            ps.setString(3, holderId);
            ps.setString(4, requestId);
            ps.setObject(5, permitId, Types.OTHER);
            if (fencingToken == null) {
                ps.setNull(6, Types.BIGINT);
            } else {
                ps.setLong(6, fencingToken);
            }
            ps.setString(7, detail);
            ps.executeUpdate();
        }
    }

    @FunctionalInterface
    private interface Binder {
        void bind(PreparedStatement ps) throws SQLException;
    }

    private static List<SemaphoreEvent> queryEvents(Connection c, String sql, Binder binder) throws SQLException {
        List<SemaphoreEvent> result = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            binder.bind(ps);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long token = rs.getLong("fencing_token");
                    Long fencingToken = rs.wasNull() ? null : token;
                    result.add(new SemaphoreEvent(
                            rs.getLong("id"),
                            rs.getString("semaphore_name"),
                            EventType.valueOf(rs.getString("event_type")),
                            rs.getString("holder_id"),
                            rs.getString("request_id"),
                            rs.getObject("permit_id", UUID.class),
                            fencingToken,
                            rs.getString("detail"),
                            instant(rs, "occurred_at")));
                }
            }
        }
        return result;
    }

    private static void checkLimit(int limit) {
        if (limit < 1 || limit > 10_000) {
            throw new IllegalArgumentException("limit must be in [1, 10000]");
        }
    }

    // ---------------------------------------------------------------- mapping & validation

    private static SemaphoreConfig config(ResultSet rs) throws SQLException {
        return new SemaphoreConfig(
                rs.getString("name"), rs.getInt("capacity"), Duration.ofMillis(rs.getLong("default_ttl_ms")));
    }

    private static Permit permit(ResultSet rs) throws SQLException {
        return new Permit(
                rs.getObject("permit_id", UUID.class),
                rs.getString("semaphore_name"),
                rs.getString("holder_id"),
                rs.getString("request_id"),
                rs.getLong("fencing_token"),
                instant(rs, "acquired_at"),
                instant(rs, "expires_at"));
    }

    private static Waiter waiter(ResultSet rs) throws SQLException {
        return new Waiter(
                rs.getObject("waiter_id", UUID.class),
                rs.getString("semaphore_name"),
                rs.getString("holder_id"),
                rs.getString("request_id"),
                instant(rs, "enqueued_at"),
                instant(rs, "wait_deadline"),
                instant(rs, "expires_at"));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getObject(column, OffsetDateTime.class).toInstant();
    }

    /** The store keeps millisecond precision; normalise so round-trips compare equal. */
    private SemaphoreConfig normalise(SemaphoreConfig config) {
        return new SemaphoreConfig(config.name(), config.capacity(), checkTtl(config.defaultTtl()));
    }

    private Duration checkTtl(Duration ttl) {
        Duration millis = Duration.ofMillis(Validation.positive(ttl, "ttl").toMillis());
        if (millis.isZero()) {
            throw new IllegalArgumentException("ttl must be at least 1ms");
        }
        if (millis.compareTo(options.maxTtl()) > 0) {
            throw new IllegalArgumentException("ttl " + ttl + " exceeds the maximum of " + options.maxTtl());
        }
        return millis;
    }

    private void checkWaitTimeout(Duration waitTimeout) {
        if (waitTimeout.compareTo(options.maxWaitTimeout()) > 0) {
            throw new IllegalArgumentException(
                    "waitTimeout " + waitTimeout + " exceeds the maximum of " + options.maxWaitTimeout());
        }
    }

    private static String describe(SemaphoreConfig config) {
        return "capacity=" + config.capacity() + ", defaultTtl=" + config.defaultTtl().toMillis() + "ms";
    }
}
