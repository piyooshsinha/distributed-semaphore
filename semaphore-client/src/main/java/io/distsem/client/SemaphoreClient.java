package io.distsem.client;

import io.distsem.client.Model.SemaphoreInfo;
import io.distsem.core.Validation;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import tools.jackson.core.type.TypeReference;

/**
 * Entry point of the SDK. One instance per application; thread-safe.
 *
 * <pre>{@code
 * try (SemaphoreClient client = SemaphoreClient.builder()
 *         .endpoints("http://sem-a:8183", "http://sem-b:8183")
 *         .build()) {
 *     DistributedSemaphore flush = client.semaphore("db-flush");
 *     try (Lease lease = flush.acquire("worker-7", Duration.ofMinutes(1))) {
 *         writeBatch(lease.fencingToken());
 *     }
 * }
 * }</pre>
 */
public final class SemaphoreClient implements AutoCloseable {

    private static final TypeReference<List<SemaphoreInfo>> SEMAPHORE_LIST = new TypeReference<>() {
    };

    private final Transport transport;
    private final Duration requestTimeout;
    private final Duration longPollTimeout;
    private final ScheduledThreadPoolExecutor scheduler;
    private final FleetClient fleet;

    private SemaphoreClient(Builder b) {
        this.requestTimeout = b.requestTimeout;
        this.longPollTimeout = b.longPollTimeout;
        this.transport = new Transport(b.endpoints, b.apiKey, b.connectTimeout, b.requestTimeout, b.maxRetryRounds);
        this.scheduler = new ScheduledThreadPoolExecutor(1, r -> Thread.ofPlatform()
                .name("distsem-client-scheduler").daemon().unstarted(r));
        this.scheduler.setRemoveOnCancelPolicy(true);
        this.fleet = new FleetClient(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public DistributedSemaphore semaphore(String name) {
        return new DistributedSemaphore(this, Validation.name(name));
    }

    public List<SemaphoreInfo> semaphores() throws InterruptedException {
        return transport.read(transport.get("/v1/semaphores"), SEMAPHORE_LIST);
    }

    public FleetClient fleet() {
        return fleet;
    }

    /** Stops background lease renewal. Leases still held are not released; they expire. */
    @Override
    public void close() {
        scheduler.shutdownNow();
    }

    Transport transport() {
        return transport;
    }

    Duration requestTimeout() {
        return requestTimeout;
    }

    Duration longPollTimeout() {
        return longPollTimeout;
    }

    ScheduledExecutorService scheduler() {
        return scheduler;
    }

    public static final class Builder {
        private final List<URI> endpoints = new ArrayList<>();
        private String apiKey;
        private Duration connectTimeout = Duration.ofSeconds(2);
        private Duration requestTimeout = Duration.ofSeconds(10);
        private Duration longPollTimeout = Duration.ofSeconds(40);
        private int maxRetryRounds = 3;

        private Builder() {
        }

        /** Base URLs of service replicas (or a load balancer). Tried in order, sticking to the last one that worked. */
        public Builder endpoints(String... urls) {
            for (String url : urls) {
                endpoints.add(URI.create(url.endsWith("/") ? url : url + "/"));
            }
            return this;
        }

        public Builder apiKey(String apiKey) {
            this.apiKey = apiKey;
            return this;
        }

        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = Objects.requireNonNull(connectTimeout);
            return this;
        }

        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = Objects.requireNonNull(requestTimeout);
            return this;
        }

        /** HTTP timeout for one acquire call; must exceed the server's long-poll hold (25s by default). */
        public Builder longPollTimeout(Duration longPollTimeout) {
            this.longPollTimeout = Objects.requireNonNull(longPollTimeout);
            return this;
        }

        /** How many times to cycle through all endpoints before giving up on a request. */
        public Builder maxRetryRounds(int rounds) {
            if (rounds < 1) {
                throw new IllegalArgumentException("rounds must be at least 1");
            }
            this.maxRetryRounds = rounds;
            return this;
        }

        public SemaphoreClient build() {
            if (endpoints.isEmpty()) {
                throw new IllegalStateException("at least one endpoint is required");
            }
            return new SemaphoreClient(this);
        }
    }
}
