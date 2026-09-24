package io.distsem.simulator;

import io.distsem.service.SemaphoreServiceApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Runs real service replicas in-process against one PostgreSQL container. */
final class ServiceReplicas {

    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    private static ConfigurableApplicationContext a;
    private static ConfigurableApplicationContext b;

    private ServiceReplicas() {
    }

    static synchronized String replicaA() {
        if (a == null) {
            a = start();
        }
        return url(a);
    }

    static synchronized String replicaB() {
        if (b == null) {
            b = start();
        }
        return url(b);
    }

    /** A replica the caller owns and may shut down. */
    static ConfigurableApplicationContext start() {
        if (!POSTGRES.isRunning()) {
            POSTGRES.start();
        }
        // Command-line arguments override the service's application.yml (builder properties would not).
        return new SpringApplicationBuilder(SemaphoreServiceApplication.class).run(
                "--server.port=0",
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--spring.datasource.hikari.maximum-pool-size=8",
                // Short slices so tests exercise the client's re-polling.
                "--distsem.long-poll.max-hold=1s",
                "--distsem.fleet.offline-after=3s",
                "--spring.main.banner-mode=off",
                "--logging.level.root=WARN");
    }

    static String url(ConfigurableApplicationContext context) {
        return "http://localhost:" + context.getEnvironment().getProperty("local.server.port");
    }
}
