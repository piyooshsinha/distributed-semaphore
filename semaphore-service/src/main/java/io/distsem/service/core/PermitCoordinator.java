package io.distsem.service.core;

import io.distsem.core.AcquireRequest;
import io.distsem.core.AcquireResult;
import io.distsem.core.Permit;
import io.distsem.core.SemaphoreStore;
import io.distsem.service.config.DistsemProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.UUID;
import org.springframework.stereotype.Service;

/**
 * Turns the store's single-shot {@code tryAcquire} into a bounded long poll: keep re-trying while
 * queued, sleeping until a database notification says the semaphore changed (or a fallback
 * interval passes), until granted, rejected, or the hold time for this HTTP call runs out.
 */
@Service
public class PermitCoordinator {

    private final SemaphoreStore store;
    private final ChangeSignals signals;
    private final DistsemProperties.LongPoll longPoll;
    private final MeterRegistry meters;

    public PermitCoordinator(SemaphoreStore store, ChangeSignals signals, DistsemProperties properties,
            MeterRegistry meters) {
        this.store = store;
        this.signals = signals;
        this.longPoll = properties.longPoll();
        this.meters = meters;
    }

    public AcquireResult acquire(AcquireRequest request) throws InterruptedException {
        Timer.Sample sample = Timer.start(meters);
        AcquireResult result = pollUntilSettled(request);
        String outcome = outcome(result);
        sample.stop(Timer.builder("distsem.acquire.duration")
                .description("Time an acquire call spent in the service, including long-poll waiting")
                .tag("semaphore", request.semaphore())
                .tag("outcome", outcome)
                .register(meters));
        counter("distsem.acquire", request.semaphore(), outcome).increment();
        return result;
    }

    private AcquireResult pollUntilSettled(AcquireRequest request) throws InterruptedException {
        long holdNanos = request.waitTimeout().compareTo(longPoll.maxHold()) < 0
                ? request.waitTimeout().toNanos()
                : longPoll.maxHold().toNanos();
        long deadline = System.nanoTime() + holdNanos;
        while (true) {
            long seen = signals.version(request.semaphore());
            AcquireResult result = store.tryAcquire(request);
            if (!(result instanceof AcquireResult.Queued)) {
                return result;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return result;
            }
            Duration pause = Duration.ofNanos(Math.min(remaining, longPoll.fallbackPoll().toNanos()));
            signals.awaitChange(request.semaphore(), seen, pause);
        }
    }

    public Permit renew(String semaphore, UUID permitId, Duration ttl) {
        Permit permit = store.renew(semaphore, permitId, ttl);
        counter("distsem.renew", semaphore, "renewed").increment();
        return permit;
    }

    public boolean release(String semaphore, UUID permitId) {
        boolean released = store.release(semaphore, permitId);
        counter("distsem.release", semaphore, released ? "released" : "not_held").increment();
        return released;
    }

    public boolean cancelWait(String semaphore, String requestId) {
        return store.cancelWait(semaphore, requestId);
    }

    private Counter counter(String name, String semaphore, String outcome) {
        return Counter.builder(name).tag("semaphore", semaphore).tag("outcome", outcome).register(meters);
    }

    private static String outcome(AcquireResult result) {
        return switch (result) {
            case AcquireResult.Granted g -> "granted";
            case AcquireResult.Queued q -> "queued";
            case AcquireResult.Rejected r -> switch (r.reason()) {
                case NO_CAPACITY -> "no_capacity";
                case WAIT_TIMEOUT -> "wait_timeout";
            };
        };
    }
}
