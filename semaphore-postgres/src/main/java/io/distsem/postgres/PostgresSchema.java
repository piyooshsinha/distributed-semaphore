package io.distsem.postgres;

import javax.sql.DataSource;
import org.flywaydb.core.Flyway;

/** Applies the semaphore schema migrations. Safe to call from every replica on startup. */
public final class PostgresSchema {

    public static final String MIGRATION_LOCATION = "classpath:db/migration/distsem";
    public static final String HISTORY_TABLE = "distsem_schema_history";

    private PostgresSchema() {
    }

    public static void migrate(DataSource dataSource) {
        flyway(dataSource).migrate();
    }

    public static Flyway flyway(DataSource dataSource) {
        return Flyway.configure()
                .dataSource(dataSource)
                .locations(MIGRATION_LOCATION)
                .table(HISTORY_TABLE)
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load();
    }
}
