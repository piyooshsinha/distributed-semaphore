package io.distsem.client;

import io.distsem.client.Model.AcquireReply;
import io.distsem.client.Model.EventInfo;
import io.distsem.client.Model.PermitInfo;
import io.distsem.client.Model.ReleaseReply;
import io.distsem.client.Model.SemaphoreInfo;
import io.distsem.client.Model.SemaphoreSnapshot;
import io.distsem.client.Transport.Response;
import io.distsem.client.Transport.Retry;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import tools.jackson.core.type.TypeReference;

/** Handle to one named semaphore. Thread-safe; obtain from {@link SemaphoreClient#semaphore}. */
public final class DistributedSemaphore {

    private static final TypeReference<List<EventInfo>> EVENT_LIST = new TypeReference<>() {
    };

    private final SemaphoreClient client;
    private final String name;
    private final String path;

    DistributedSemaphore(SemaphoreClient client, String name) {
        this.client = client;
        this.name = name;
        this.path = "/v1/semaphores/" + URLEncoder.encode(name, StandardCharsets.UTF_8);
    }

    public String name() {
        return name;
    }

    /** Creates the semaphore, or updates its capacity and default TTL if it exists. */
    public SemaphoreInfo configure(int capacity, Duration defaultTtl) throws InterruptedException {
        Response r = transport().send("PUT", path,
                Map.of("capacity", capacity, "defaultTtlMs", defaultTtl.toMillis()),
                client.requestTimeout(), Retry.IDEMPOTENT);
        return transport().read(r, SemaphoreInfo.class);
    }

    /** Creates the semaphore only if it does not exist yet; an existing one keeps its settings. */
    public SemaphoreInfo createIfAbsent(int capacity, Duration defaultTtl) throws InterruptedException {
        Optional<SemaphoreSnapshot> existing = find();
        if (existing.isPresent()) {
            SemaphoreSnapshot s = existing.get();
            return new SemaphoreInfo(s.name(), s.capacity(), s.defaultTtlMs());
        }
        return configure(capacity, defaultTtl);
    }

    public Optional<SemaphoreSnapshot> find() throws InterruptedException {
        Response r = transport().get(path);
        return r.status() == 404 ? Optional.empty() : Optional.of(transport().read(r, SemaphoreSnapshot.class));
    }

    public SemaphoreSnapshot state() throws InterruptedException {
        return transport().read(transport().get(path), SemaphoreSnapshot.class);
    }

    /** Audit events with {@code id > afterId}, oldest first. */
    public List<EventInfo> events(long afterId, int limit) throws InterruptedException {
        return transport().read(transport().get(path + "/events?after=" + afterId + "&limit=" + limit), EVENT_LIST);
    }

    /** Fails fast: a permit if one is free right now, otherwise empty. */
    public Optional<Lease> tryAcquire(String holderId) throws InterruptedException {
        try {
            return Optional.of(acquire(AcquireOptions.holder(holderId)));
        } catch (AcquireRejectedException e) {
            return Optional.empty();
        }
    }

    /** Waits up to {@code waitTimeout} for a permit with the default TTL and auto-renewal. */
    public Lease acquire(String holderId, Duration waitTimeout) throws InterruptedException {
        return acquire(AcquireOptions.holder(holderId).waitUpTo(waitTimeout));
    }

    /**
     * Acquires a permit, queueing up to {@code options.waitTimeout()}. The service answers a long
     * wait in slices ("still queued"); this keeps polling with the same requestId, which also keeps
     * the caller's place in the queue.
     *
     * @throws AcquireRejectedException if no permit was granted in time
     * @throws InterruptedException     if interrupted while waiting (the queue entry is withdrawn)
     */
    public Lease acquire(AcquireOptions options) throws InterruptedException {
        long deadline = System.nanoTime() + options.waitTimeout().toNanos();
        boolean queued = false;
        try {
            while (true) {
                long remainingMs = Math.max(0, (deadline - System.nanoTime()) / 1_000_000);
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("holderId", options.holderId());
                body.put("requestId", options.requestId());
                if (options.ttl() != null) {
                    body.put("ttlMs", options.ttl().toMillis());
                }
                body.put("waitTimeoutMs", remainingMs);

                long sentAt = System.nanoTime();
                Response r = transport().send("POST", path + "/acquire", body,
                        client.longPollTimeout(), Retry.IDEMPOTENT);
                switch (r.status()) {
                    case 200 -> {
                        PermitInfo permit = transport().parse(r, AcquireReply.class).permit();
                        Duration ttl = Duration.between(permit.acquiredAt(), permit.expiresAt());
                        return new Lease(this, permit, ttl, sentAt, options.autoRenew());
                    }
                    case 202 -> {
                        queued = true;
                        options.onQueued().accept(transport().parse(r, AcquireReply.class).position());
                        if (System.nanoTime() - deadline >= 0) {
                            cancelWait(options.requestId());
                            throw new AcquireRejectedException(name, AcquireRejectedException.Reason.WAIT_TIMEOUT);
                        }
                    }
                    case 409 -> {
                        AcquireReply reply = transport().parse(r, AcquireReply.class);
                        throw new AcquireRejectedException(name, AcquireRejectedException.Reason.valueOf(reply.reason()));
                    }
                    default -> throw transport().error(r);
                }
            }
        } catch (InterruptedException e) {
            if (queued) {
                withdrawQuietly(options.requestId());
            }
            throw e;
        }
    }

    /** Leaves the wait queue. Returns false if the request was not queued. */
    public boolean cancelWait(String requestId) throws InterruptedException {
        Response r = transport().send("DELETE", path + "/waiters/" + URLEncoder.encode(requestId, StandardCharsets.UTF_8),
                null, client.requestTimeout(), Retry.IDEMPOTENT);
        if (r.status() == 404) {
            return false;
        }
        if (!r.ok()) {
            throw transport().error(r);
        }
        return true;
    }

    PermitInfo renewPermit(UUID permitId, Duration ttl) throws InterruptedException {
        Response r = transport().send("POST", path + "/permits/" + permitId + "/renew",
                Map.of("ttlMs", ttl.toMillis()), client.requestTimeout(), Retry.IDEMPOTENT);
        return transport().read(r, PermitInfo.class);
    }

    boolean releasePermit(UUID permitId) throws InterruptedException {
        Response r = transport().send("DELETE", path + "/permits/" + permitId, null,
                client.requestTimeout(), Retry.IDEMPOTENT);
        return transport().read(r, ReleaseReply.class).released();
    }

    private void withdrawQuietly(String requestId) {
        // Called while handling an interrupt: clear the flag for the HTTP call, then restore it.
        boolean interrupted = Thread.interrupted();
        try {
            cancelWait(requestId);
        } catch (InterruptedException | SemaphoreClientException ignored) {
            // The entry lapses on its own once we stop polling.
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    SemaphoreClient client() {
        return client;
    }

    private Transport transport() {
        return client.transport();
    }
}
