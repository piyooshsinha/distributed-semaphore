package io.distsem.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** One Postgres container shared by every integration test in the module. */
final class PostgresTestSupport {

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine")
            .withCommand("postgres", "-c", "max_connections=200");

    private static HikariDataSource shared;

    private PostgresTestSupport() {
    }

    static synchronized DataSource dataSource() {
        if (shared == null) {
            shared = newPool("shared", 40);
            PostgresSchema.migrate(shared);
        }
        return shared;
    }

    /** An independent pool, as a separate service replica would have. */
    static synchronized HikariDataSource newPool(String name, int size) {
        if (!POSTGRES.isRunning()) {
            POSTGRES.start();
        }
        HikariConfig config = new HikariConfig();
        config.setPoolName(name);
        config.setJdbcUrl(POSTGRES.getJdbcUrl());
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        config.setMaximumPoolSize(size);
        return new HikariDataSource(config);
    }

    static void truncateAll() {
        try (Connection c = dataSource().getConnection(); Statement s = c.createStatement()) {
            s.execute("TRUNCATE semaphore_events, semaphore_waiters, semaphore_holders, semaphores, nodes, node_commands RESTART IDENTITY");
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
