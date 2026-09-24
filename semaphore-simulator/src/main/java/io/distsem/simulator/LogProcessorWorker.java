package io.distsem.simulator;

import io.distsem.client.AcquireOptions;
import io.distsem.client.AcquireRejectedException;
import io.distsem.client.Lease;
import io.distsem.client.SemaphoreClientException;
import io.distsem.core.fleet.TaskOutcome;
import io.distsem.core.fleet.TaskRecord;
import io.distsem.core.fleet.WorkerReport;
import io.distsem.core.fleet.WorkerState;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One simulated log processor. It repeatedly takes a batch of log lines, waits for a permit on the
 * shared semaphore, "flushes" the batch while holding the permit, and releases it.
 */
final class LogProcessorWorker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(LogProcessorWorker.class);
    private static final Duration SLICE = Duration.ofMillis(100);
    private static final String[] APPS = {"checkout", "search", "payments", "auth", "catalog", "inventory"};

    private final LogProcessorNode node;
    private final String workerId;
    private final AtomicLong taskSeq = new AtomicLong();
    private final AtomicLong completed = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong leasesLost = new AtomicLong();

    private volatile WorkerState state = WorkerState.STARTING;
    private volatile Instant stateSince = Instant.now();
    private volatile String currentTask;
    private volatile Lease lease;
    private volatile Integer queuePosition;
    private volatile boolean stopRequested;
    private volatile boolean crashRequested;
    private volatile Thread thread;

    LogProcessorWorker(LogProcessorNode node, String workerId) {
        this.node = node;
        this.workerId = workerId;
    }

    String workerId() {
        return workerId;
    }

    void start() {
        thread = Thread.ofVirtual().name(node.config().nodeId() + "/" + workerId).start(this);
    }

    /** Finish the current task, then stop. */
    void stopGracefully() {
        stopRequested = true;
        if (state == WorkerState.IDLE || state == WorkerState.PAUSED || state == WorkerState.WAITING) {
            interrupt();
        }
    }

    /** Die abruptly: abandon any permit without releasing it, stay down a while, then restart. */
    void crash() {
        crashRequested = true;
        interrupt();
    }

    void join(Duration timeout) throws InterruptedException {
        Thread t = thread;
        if (t != null) {
            t.join(timeout);
        }
    }

    boolean isStopped() {
        return state == WorkerState.STOPPED;
    }

    WorkerReport report() {
        Lease current = lease;
        return new WorkerReport(workerId, state, stateSince, currentTask,
                current == null ? null : current.permitId(),
                current == null ? null : current.fencingToken(),
                state == WorkerState.WAITING ? queuePosition : null,
                completed.get(), failed.get(), leasesLost.get());
    }

    @Override
    public void run() {
        try {
            while (!stopRequested) {
                if (crashRequested) {
                    crashAndRestart(null, null);
                    continue;
                }
                if (node.isPaused()) {
                    setState(WorkerState.PAUSED);
                    pause(Duration.ofMillis(250));
                    continue;
                }
                processOneTask();
                if (!stopRequested && !crashRequested) {
                    setState(WorkerState.IDLE);
                    pause(randomBetween(node.config().idleMin(), node.config().idleMax()));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            Lease held = lease;
            if (held != null) {
                held.close();
            }
            lease = null;
            currentTask = null;
            setState(WorkerState.STOPPED);
        }
    }

    private void processOneTask() throws InterruptedException {
        long seq = taskSeq.incrementAndGet();
        String taskId = node.config().nodeId() + "-" + node.runId() + "-" + workerId + "-" + seq;
        int lines = ThreadLocalRandom.current().nextInt(200, 5_000);
        String description = "flush " + lines + " log lines from " + APPS[(int) (seq % APPS.length)];
        Instant queuedAt = Instant.now();
        currentTask = taskId;
        queuePosition = null;
        setState(WorkerState.WAITING);

        Lease granted;
        try {
            granted = node.semaphore().acquire(AcquireOptions.holder(node.config().nodeId() + "/" + workerId)
                    .requestId(taskId)
                    .ttl(node.config().leaseTtl())
                    .waitUpTo(node.config().waitTimeout())
                    .onQueued(position -> queuePosition = position));
        } catch (AcquireRejectedException e) {
            finish(taskId, description, queuedAt, 0, null, TaskOutcome.TIMED_OUT, e.getMessage());
            return;
        } catch (SemaphoreClientException e) {
            log.warn("{}: acquire failed: {}", workerId, e.getMessage());
            finish(taskId, description, queuedAt, 0, null, TaskOutcome.FAILED, e.getMessage());
            pause(Duration.ofSeconds(1));
            return;
        } catch (InterruptedException e) {
            if (crashRequested) {
                crashAndRestart(taskId, description);
                return;
            }
            if (stopRequested) {
                finish(taskId, description, queuedAt, 0, null, TaskOutcome.ABANDONED, "stopped while waiting");
                return;
            }
            throw e;
        }

        lease = granted;
        long waitMs = Duration.between(queuedAt, Instant.now()).toMillis();
        setState(WorkerState.WORKING);
        Duration work = randomBetween(node.workMin(), node.workMax());
        TaskOutcome outcome = TaskOutcome.COMPLETED;
        String error = null;
        try {
            long end = System.nanoTime() + work.toNanos();
            while (System.nanoTime() < end) {
                if (crashRequested) {
                    crashAndRestart(taskId, description);
                    return;
                }
                if (!granted.isValid()) {
                    outcome = TaskOutcome.LEASE_LOST;
                    error = granted.lostReason() != null ? granted.lostReason() : "lease expired";
                    break;
                }
                sleepSlice();
            }
        } catch (InterruptedException e) {
            if (crashRequested) {
                crashAndRestart(taskId, description);
                return;
            }
            throw e;
        }
        if (outcome == TaskOutcome.COMPLETED && ThreadLocalRandom.current().nextDouble() < node.config().failureRate()) {
            outcome = TaskOutcome.FAILED;
            error = "database rejected the batch (simulated)";
        }
        setState(WorkerState.RELEASING);
        granted.close();
        lease = null;
        finish(taskId, description, queuedAt, waitMs, granted.fencingToken(), outcome, error);
    }

    private void crashAndRestart(String taskId, String description) throws InterruptedException {
        Lease held = lease;
        if (held != null) {
            held.abandon();
            leasesLost.incrementAndGet();
        }
        if (taskId != null) {
            finish(taskId, description, Instant.now(), 0, held == null ? null : held.fencingToken(),
                    TaskOutcome.ABANDONED, "worker crashed" + (held == null ? "" : "; permit left to expire"));
        }
        lease = null;
        currentTask = null;
        setState(WorkerState.CRASHED);
        log.warn("{}: crashed{}; restarting in {}", workerId, held == null ? "" : " holding permit " + held.fencingToken(),
                node.config().crashDowntime());
        Thread.interrupted();
        long until = System.nanoTime() + node.config().crashDowntime().toNanos();
        while (System.nanoTime() < until) {
            try {
                Thread.sleep(Duration.ofNanos(until - System.nanoTime()));
            } catch (InterruptedException e) {
                if (stopRequested) {
                    throw e;
                }
            }
        }
        crashRequested = false;
        setState(WorkerState.STARTING);
    }

    private void finish(String taskId, String description, Instant queuedAt, long waitMs, Long token,
            TaskOutcome outcome, String error) {
        switch (outcome) {
            case COMPLETED -> completed.incrementAndGet();
            case LEASE_LOST -> {
                leasesLost.incrementAndGet();
                failed.incrementAndGet();
            }
            case ABANDONED -> { }
            default -> failed.incrementAndGet();
        }
        Instant now = Instant.now();
        long total = Duration.between(queuedAt, now).toMillis();
        node.recordTask(new TaskRecord(taskId, workerId, description, queuedAt, now, waitMs,
                Math.max(0, total - waitMs), token, outcome, error));
        currentTask = null;
    }

    private void setState(WorkerState next) {
        if (state != next) {
            state = next;
            stateSince = Instant.now();
        }
    }

    private void pause(Duration duration) throws InterruptedException {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            if (!crashRequested && !stopRequested) {
                throw e;
            }
        }
    }

    private static void sleepSlice() throws InterruptedException {
        Thread.sleep(SLICE);
    }

    private void interrupt() {
        Thread t = thread;
        if (t != null) {
            t.interrupt();
        }
    }

    static Duration randomBetween(Duration min, Duration max) {
        long lo = min.toMillis();
        long hi = Math.max(lo, max.toMillis());
        return Duration.ofMillis(lo == hi ? lo : ThreadLocalRandom.current().nextLong(lo, hi + 1));
    }
}
