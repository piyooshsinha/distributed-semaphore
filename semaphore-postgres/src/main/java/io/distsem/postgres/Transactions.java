package io.distsem.postgres;

import io.distsem.core.SemaphoreException;
import io.distsem.core.StoreUnavailableException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs units of work in JDBC transactions, retrying the ones Postgres asks us to retry. */
final class Transactions {

    private static final Logger log = LoggerFactory.getLogger(Transactions.class);

    private static final String SERIALIZATION_FAILURE = "40001";
    private static final String DEADLOCK_DETECTED = "40P01";
    private static final String CHECK_VIOLATION = "23514";
    private static final Set<String> RETRYABLE = Set.of(SERIALIZATION_FAILURE, DEADLOCK_DETECTED);

    @FunctionalInterface
    interface Work<T> {
        T run(Connection connection) throws SQLException;
    }

    private final DataSource dataSource;
    private final PostgresStoreOptions options;

    Transactions(DataSource dataSource, PostgresStoreOptions options) {
        this.dataSource = dataSource;
        this.options = options;
    }

    /** READ COMMITTED read-write transaction. Correctness relies on explicit row locks. */
    <T> T write(Work<T> work) {
        return execute(work, Connection.TRANSACTION_READ_COMMITTED, false);
    }

    /** REPEATABLE READ read-only transaction, for consistent multi-query snapshots. */
    <T> T read(Work<T> work) {
        return execute(work, Connection.TRANSACTION_REPEATABLE_READ, true);
    }

    private <T> T execute(Work<T> work, int isolation, boolean readOnly) {
        for (int attempt = 0; ; attempt++) {
            try {
                return once(work, isolation, readOnly);
            } catch (SQLException e) {
                if (RETRYABLE.contains(e.getSQLState()) && attempt < options.maxRetries()) {
                    log.debug("Retrying transaction after SQLState {} (attempt {})", e.getSQLState(), attempt + 1);
                    backoff(attempt);
                    continue;
                }
                throw translate(e);
            }
        }
    }

    private <T> T once(Work<T> work, int isolation, boolean readOnly) throws SQLException {
        try (Connection c = dataSource.getConnection()) {
            boolean autoCommit = c.getAutoCommit();
            int previousIsolation = c.getTransactionIsolation();
            boolean previousReadOnly = c.isReadOnly();
            try {
                c.setAutoCommit(false);
                c.setTransactionIsolation(isolation);
                c.setReadOnly(readOnly);
                try (Statement s = c.createStatement()) {
                    s.execute("SET LOCAL lock_timeout = '" + options.lockTimeout().toMillis() + "ms'");
                }
                T result = work.run(c);
                c.commit();
                return result;
            } catch (SQLException | RuntimeException | Error e) {
                rollbackQuietly(c, e);
                throw e;
            } finally {
                c.setAutoCommit(autoCommit);
                c.setTransactionIsolation(previousIsolation);
                c.setReadOnly(previousReadOnly);
            }
        }
    }

    private static void rollbackQuietly(Connection c, Throwable cause) {
        try {
            c.rollback();
        } catch (SQLException e) {
            cause.addSuppressed(e);
        }
    }

    private static void backoff(int attempt) {
        long maxMillis = 5L << Math.min(attempt, 6);
        LockSupport.parkNanos(ThreadLocalRandom.current().nextLong(1, maxMillis + 1) * 1_000_000L);
    }

    private static SemaphoreException translate(SQLException e) {
        if (CHECK_VIOLATION.equals(e.getSQLState())) {
            return new SemaphoreException("store rejected a write that violates an invariant: " + e.getMessage(), e);
        }
        return new StoreUnavailableException("semaphore store failure (SQLState " + e.getSQLState() + ")", e);
    }
}
