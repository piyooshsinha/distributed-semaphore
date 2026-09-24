package io.distsem.service.core;

import io.distsem.postgres.PostgresEventListener;
import io.distsem.postgres.PostgresEventListener.Notification;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.springframework.stereotype.Component;

/**
 * Per-semaphore change counters fed by database notifications. Threads waiting for a semaphore to
 * change read its version, re-check the store, then block until the version moves on or a timeout
 * passes. Reading the version before checking the store means no wake-up can slip in between.
 */
@Component
public class ChangeSignals implements PostgresEventListener.Subscriber {

    private final ConcurrentHashMap<String, Signal> signals = new ConcurrentHashMap<>();
    private final Signal global = new Signal();
    private final Signal nodes = new Signal();

    public ChangeSignals(PostgresEventListener listener) {
        listener.subscribe(this);
    }

    public long version(String semaphore) {
        return signal(semaphore).version();
    }

    public long globalVersion() {
        return global.version();
    }

    /** Changes whenever any node sends a heartbeat or is removed. */
    public long nodesVersion() {
        return nodes.version();
    }

    /** Blocks until the semaphore's version differs from {@code seen}, or {@code timeout} passes. */
    public void awaitChange(String semaphore, long seen, Duration timeout) throws InterruptedException {
        signal(semaphore).await(seen, timeout);
    }

    /** Blocks until any semaphore changes after {@code seen}, or {@code timeout} passes. */
    public void awaitAnyChange(long seen, Duration timeout) throws InterruptedException {
        global.await(seen, timeout);
    }

    @Override
    public void onEvent(Notification notification) {
        signal(notification.semaphore()).bump();
        global.bump();
    }

    @Override
    public void onNodeChange(String nodeId) {
        nodes.bump();
        global.bump();
    }

    @Override
    public void onResync() {
        signals.values().forEach(Signal::bump);
        nodes.bump();
        global.bump();
    }

    private Signal signal(String semaphore) {
        return signals.computeIfAbsent(semaphore, k -> new Signal());
    }

    private static final class Signal {
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition changed = lock.newCondition();
        private long version;

        long version() {
            lock.lock();
            try {
                return version;
            } finally {
                lock.unlock();
            }
        }

        void bump() {
            lock.lock();
            try {
                version++;
                changed.signalAll();
            } finally {
                lock.unlock();
            }
        }

        void await(long seen, Duration timeout) throws InterruptedException {
            long remaining = timeout.toNanos();
            lock.lock();
            try {
                while (version == seen && remaining > 0) {
                    remaining = changed.awaitNanos(remaining);
                }
            } finally {
                lock.unlock();
            }
        }
    }

}
