package io.distsem.service.config;

import io.distsem.postgres.PostgresStoreOptions;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Service settings under the {@code distsem} prefix. */
@ConfigurationProperties("distsem")
public record DistsemProperties(
        @DefaultValue Store store,
        @DefaultValue LongPoll longPoll,
        @DefaultValue Housekeeping housekeeping,
        @DefaultValue Fleet fleet,
        @DefaultValue Security security) {

    public DistsemProperties {
        if (store.waiterLease().compareTo(longPoll.fallbackPoll().multipliedBy(3)) < 0) {
            throw new IllegalArgumentException("distsem.store.waiter-lease must be at least 3x distsem.long-poll.fallback-poll");
        }
    }

    /**
     * @param waiterLease    how long a queued caller keeps its place between polls
     * @param maxTtl         longest permit lease a caller may request
     * @param maxWaitTimeout longest a caller may ask to queue
     * @param lockTimeout    how long a transaction waits for a semaphore's row lock
     * @param maxRetries     retries on deadlock / serialization failure
     */
    public record Store(
            @DefaultValue("10s") Duration waiterLease,
            @DefaultValue("1h") Duration maxTtl,
            @DefaultValue("10m") Duration maxWaitTimeout,
            @DefaultValue("5s") Duration lockTimeout,
            @DefaultValue("3") int maxRetries) {

        public PostgresStoreOptions toOptions() {
            return new PostgresStoreOptions(waiterLease, maxTtl, maxWaitTimeout, lockTimeout, maxRetries);
        }
    }

    /**
     * @param maxHold      longest a single HTTP acquire call blocks before answering "still queued"
     * @param fallbackPoll re-check interval while waiting, in case a notification was missed
     */
    public record LongPoll(
            @DefaultValue("25s") Duration maxHold,
            @DefaultValue("1s") Duration fallbackPoll) {
    }

    /**
     * @param reapInterval   how often expired permits and waiters are swept
     * @param eventRetention how long audit events are kept
     * @param pruneInterval  how often old audit events are deleted
     */
    public record Housekeeping(
            @DefaultValue("1s") Duration reapInterval,
            @DefaultValue("7d") Duration eventRetention,
            @DefaultValue("1h") Duration pruneInterval) {
    }

    /**
     * @param offlineAfter  a node that has not sent a heartbeat for this long is shown as offline
     * @param forgetAfter   nodes silent for this long are removed from the registry
     * @param commandTtl    undelivered commands older than this are dropped
     */
    public record Fleet(
            @DefaultValue("5s") Duration offlineAfter,
            @DefaultValue("1h") Duration forgetAfter,
            @DefaultValue("5m") Duration commandTtl) {
    }

    /** @param apiKey when non-blank, every /v1 request must send it in the {@code X-API-Key} header */
    public record Security(@DefaultValue("") String apiKey) {

        public boolean enabled() {
            return apiKey != null && !apiKey.isBlank();
        }
    }
}
