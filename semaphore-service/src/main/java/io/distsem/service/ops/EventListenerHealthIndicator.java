package io.distsem.service.ops;

import io.distsem.postgres.PostgresEventListener;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Reports whether the LISTEN connection is up. While it is down the service still works, but
 * waiters fall back to periodic polling, so this is reported as degraded rather than down.
 */
@Component("eventListener")
class EventListenerHealthIndicator implements HealthIndicator {

    private final PostgresEventListener listener;

    EventListenerHealthIndicator(PostgresEventListener listener) {
        this.listener = listener;
    }

    @Override
    public Health health() {
        return listener.isConnected()
                ? Health.up().withDetail("channel", PostgresEventListener.CHANNEL).build()
                : Health.status("DEGRADED").withDetail("reason", "not listening; waiters are polling").build();
    }
}
