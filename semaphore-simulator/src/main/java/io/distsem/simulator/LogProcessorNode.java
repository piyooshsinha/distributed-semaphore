package io.distsem.simulator;

import io.distsem.client.DistributedSemaphore;
import io.distsem.client.SemaphoreClient;
import io.distsem.client.SemaphoreClientException;
import io.distsem.core.fleet.HeartbeatResponse;
import io.distsem.core.fleet.NodeCommand;
import io.distsem.core.fleet.NodeReport;
import io.distsem.core.fleet.TaskRecord;
import io.distsem.core.fleet.WorkerReport;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A machine running several {@link LogProcessorWorker}s. It reports to the service's node registry
 * every heartbeat and carries out operator commands that come back in the reply.
 */
public final class LogProcessorNode implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LogProcessorNode.class);
    private static final int RECENT_TASKS = 30;
    private static final int REMEMBERED_COMMANDS = 1_000;
    static final String VERSION = "0.1.0";

    private final SimulatorConfig config;
    private final SemaphoreClient client;
    private final DistributedSemaphore semaphore;
    private final String runId = Integer.toHexString(ThreadLocalRandom.current().nextInt(0x1000, 0x10000));
    private final Instant startedAt = Instant.now();
    private final ConcurrentSkipListMap<Integer, LogProcessorWorker> workers = new ConcurrentSkipListMap<>();
    private final Deque<TaskRecord> recentTasks = new ArrayDeque<>();
    private final Set<Long> executedCommands = new LinkedHashSet<>();
    private final List<Long> pendingAcks = new ArrayList<>();

    private volatile boolean paused;
    private volatile Duration workMin;
    private volatile Duration workMax;
    private volatile boolean running;
    private Thread heartbeatThread;

    public LogProcessorNode(SimulatorConfig config, SemaphoreClient client) {
        this.config = config;
        this.client = client;
        this.semaphore = client.semaphore(config.semaphore());
        this.workMin = config.workMin();
        this.workMax = config.workMax();
    }

    /** Ensures the semaphore exists, starts the workers and the heartbeat loop. */
    public void start() throws InterruptedException {
        while (true) {
            try {
                var info = semaphore.createIfAbsent(config.semaphoreCapacity(), config.semaphoreDefaultTtl());
                log.info("Node {} (run {}) using semaphore '{}' capacity {}", config.nodeId(), runId, info.name(), info.capacity());
                break;
            } catch (SemaphoreClientException e) {
                log.warn("Semaphore service not ready ({}); retrying", e.getMessage());
                Thread.sleep(2_000);
            }
        }
        running = true;
        scaleTo(config.workers());
        heartbeatThread = Thread.ofVirtual().name("heartbeat").start(this::heartbeatLoop);
    }

    // ---------------------------------------------------------------- accessors used by workers

    SimulatorConfig config() {
        return config;
    }

    DistributedSemaphore semaphore() {
        return semaphore;
    }

    String runId() {
        return runId;
    }

    boolean isPaused() {
        return paused;
    }

    Duration workMin() {
        return workMin;
    }

    Duration workMax() {
        return workMax;
    }

    void recordTask(TaskRecord task) {
        synchronized (recentTasks) {
            recentTasks.addLast(task);
            while (recentTasks.size() > RECENT_TASKS) {
                recentTasks.removeFirst();
            }
        }
    }

    // ---------------------------------------------------------------- reporting & commands

    NodeReport report() {
        List<WorkerReport> workerReports = workers.values().stream().map(LogProcessorWorker::report).toList();
        List<TaskRecord> tasks;
        synchronized (recentTasks) {
            tasks = List.copyOf(recentTasks);
        }
        List<Long> acks;
        synchronized (pendingAcks) {
            acks = List.copyOf(pendingAcks);
        }
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("workMinMs", Long.toString(workMin.toMillis()));
        settings.put("workMaxMs", Long.toString(workMax.toMillis()));
        settings.put("leaseTtlMs", Long.toString(config.leaseTtl().toMillis()));
        settings.put("failureRate", Double.toString(config.failureRate()));
        settings.put("runId", runId);
        return new NodeReport(config.nodeId(), hostname(), VERSION, startedAt, config.semaphore(), paused, settings,
                workerReports, tasks, acks);
    }

    private void heartbeatLoop() {
        while (running) {
            try {
                NodeReport report = report();
                HeartbeatResponse response = client.fleet().heartbeat(report);
                synchronized (pendingAcks) {
                    pendingAcks.removeAll(report.ackedCommands());
                }
                for (NodeCommand command : response.commands()) {
                    execute(command);
                }
                workers.entrySet().removeIf(e -> e.getValue().isStopped());
            } catch (SemaphoreClientException e) {
                log.warn("Heartbeat failed: {}", e.getMessage());
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                log.error("Heartbeat loop error", e);
            }
            try {
                Thread.sleep(config.heartbeat());
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    void execute(NodeCommand command) throws InterruptedException {
        synchronized (executedCommands) {
            if (!executedCommands.add(command.id())) {
                acknowledge(command.id());
                return;
            }
            if (executedCommands.size() > REMEMBERED_COMMANDS) {
                executedCommands.remove(executedCommands.iterator().next());
            }
        }
        log.info("Executing {} {}", command.type(), command.args());
        try {
            switch (command.type()) {
                case SCALE_WORKERS -> scaleTo(Integer.parseInt(command.arg("count")));
                case PAUSE -> paused = true;
                case RESUME -> paused = false;
                case CRASH_WORKER -> crashWorker(command.arg("workerId"));
                case SET_WORK_DURATION -> {
                    Duration min = Duration.ofMillis(Long.parseLong(command.arg("minMs")));
                    Duration max = Duration.ofMillis(Long.parseLong(command.arg("maxMs")));
                    if (min.isNegative() || max.compareTo(min) < 0) {
                        throw new IllegalArgumentException("need 0 <= minMs <= maxMs");
                    }
                    workMin = min;
                    workMax = max;
                }
                case KILL_NODE -> {
                    // Acknowledge first, or the command would be redelivered and kill the restarted node.
                    acknowledge(command.id());
                    client.fleet().heartbeat(report());
                    log.error("KILL_NODE received: terminating abruptly without releasing permits");
                    Runtime.getRuntime().halt(137);
                }
            }
        } catch (IllegalArgumentException e) {
            log.warn("Ignoring invalid command {}: {}", command, e.getMessage());
        }
        acknowledge(command.id());
    }

    private void acknowledge(long commandId) {
        synchronized (pendingAcks) {
            if (!pendingAcks.contains(commandId)) {
                pendingAcks.add(commandId);
            }
        }
    }

    private void crashWorker(String workerId) {
        workers.values().stream()
                .filter(w -> w.workerId().equals(workerId))
                .findFirst()
                .ifPresentOrElse(LogProcessorWorker::crash,
                        () -> log.warn("CRASH_WORKER: no worker {}", workerId));
    }

    synchronized void scaleTo(int count) {
        if (count < 0 || count > NodeReport.MAX_WORKERS) {
            throw new IllegalArgumentException("worker count must be in [0, " + NodeReport.MAX_WORKERS + "]");
        }
        List<LogProcessorWorker> active = workers.values().stream().filter(w -> !w.isStopped()).toList();
        for (int i = active.size(); i < count; i++) {
            int index = workers.isEmpty() ? 1 : workers.lastKey() + 1;
            LogProcessorWorker worker = new LogProcessorWorker(this, "w" + index);
            workers.put(index, worker);
            worker.start();
        }
        for (int i = active.size() - 1; i >= count; i--) {
            active.get(i).stopGracefully();
        }
    }

    /** Stops workers (each finishes its current task and releases its permit), then heartbeats one last time. */
    @Override
    public void close() throws InterruptedException {
        workers.values().forEach(LogProcessorWorker::stopGracefully);
        for (LogProcessorWorker worker : workers.values()) {
            worker.join(config.workMax().plusSeconds(5));
        }
        running = false;
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
            heartbeatThread.join(Duration.ofSeconds(2));
        }
        try {
            client.fleet().heartbeat(report());
        } catch (SemaphoreClientException e) {
            log.debug("Final heartbeat failed: {}", e.getMessage());
        }
    }

    private static String hostname() {
        String host = System.getenv("HOSTNAME");
        return host != null ? host : "localhost";
    }
}
