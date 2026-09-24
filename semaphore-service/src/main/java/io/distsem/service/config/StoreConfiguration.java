package io.distsem.service.config;

import io.distsem.core.SemaphoreStore;
import io.distsem.postgres.PostgresEventListener;
import io.distsem.postgres.PostgresSchema;
import io.distsem.postgres.PostgresSemaphoreStore;
import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class StoreConfiguration {

    /** Runs the schema migrations before anything touches the store. */
    @Bean
    SchemaMigration schemaMigration(DataSource dataSource) {
        PostgresSchema.migrate(dataSource);
        return new SchemaMigration();
    }

    @Bean
    SemaphoreStore semaphoreStore(DataSource dataSource, DistsemProperties properties, SchemaMigration migrated) {
        return new PostgresSemaphoreStore(dataSource, properties.store().toOptions());
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    PostgresEventListener postgresEventListener(DataSource dataSource, SchemaMigration migrated) {
        return new PostgresEventListener(dataSource);
    }

    /** Marker proving migrations have run. */
    static final class SchemaMigration {
    }
}
