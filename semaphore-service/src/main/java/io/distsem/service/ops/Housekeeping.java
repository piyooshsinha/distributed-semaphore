package io.distsem.service.ops;

import io.distsem.core.SemaphoreStore;
import io.distsem.service.config.DistsemProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

/**
 * Sweeps expired permits and waiters, and prunes old audit events. Every replica runs this; the
 * store skips semaphores another replica is already working on, so they do not contend.
 */
@Component
class Housekeeping implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(Housekeeping.class);

    private final SemaphoreStore store;
    private final TaskScheduler scheduler;
    private final DistsemProperties.Housekeeping settings;
    private final List<ScheduledFuture<?>> tasks = new ArrayList<>();
    private volatile boolean running;

    Housekeeping(SemaphoreStore store, TaskScheduler scheduler, DistsemProperties properties) {
        this.store = store;
        this.scheduler = scheduler;
        this.settings = properties.housekeeping();
    }

    void reap() {
        try {
            int removed = store.reapExpired();
            if (removed > 0) {
                log.info("Reaped {} expired permits/waiters", removed);
            }
        } catch (RuntimeException e) {
            log.warn("Reaper run failed: {}", e.toString());
        }
    }

    void prune() {
        try {
            int removed = store.pruneEvents(settings.eventRetention());
            if (removed > 0) {
                log.info("Pruned {} audit events older than {}", removed, settings.eventRetention());
            }
        } catch (RuntimeException e) {
            log.warn("Event pruning failed: {}", e.toString());
        }
    }

    @Override
    public synchronized void start() {
        tasks.add(scheduler.scheduleWithFixedDelay(this::reap, settings.reapInterval()));
        tasks.add(scheduler.scheduleWithFixedDelay(this::prune, settings.pruneInterval()));
        running = true;
    }

    @Override
    public synchronized void stop() {
        tasks.forEach(t -> t.cancel(false));
        tasks.clear();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
