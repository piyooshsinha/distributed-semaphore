package io.distsem.service.web;

import io.distsem.core.EventType;
import io.distsem.core.SemaphoreConfig;
import io.distsem.core.SemaphoreEvent;
import io.distsem.core.SemaphoreNotFoundException;
import io.distsem.core.SemaphoreStore;
import io.distsem.core.Validation;
import io.distsem.service.core.ChangeSignals;
import io.distsem.service.web.ApiModels.EventView;
import io.distsem.service.web.ApiModels.StateView;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Server-sent event streams of the audit log, for dashboards and debugging.
 *
 * <p>Each stream runs on its own virtual thread: it reads new events past its cursor, sends them,
 * then sleeps until a database notification says something changed. Because a semaphore's events
 * commit in id order, following {@code id > cursor} never skips an event.
 *
 * <ul>
 *   <li>{@code event: semaphore-event} carries one audit event (the SSE id is the event id);
 *   <li>{@code event: state} carries a fresh snapshot of the semaphore after a batch of events;
 *   <li>comments are sent every 15s as a heartbeat.
 * </ul>
 */
@RestController
class EventStreams implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(EventStreams.class);
    private static final Duration HEARTBEAT = Duration.ofSeconds(15);
    private static final int PAGE = 500;
    private static final int MAX_BACKFILL = 1000;

    private final SemaphoreStore store;
    private final ChangeSignals signals;
    private final Set<Thread> streams = ConcurrentHashMap.newKeySet();
    private final AtomicLong streamIds = new AtomicLong();
    private volatile boolean running;

    EventStreams(SemaphoreStore store, ChangeSignals signals) {
        this.store = store;
        this.signals = signals;
    }

    /** One semaphore. Resumes after {@code Last-Event-ID} if given, else sends the last {@code backfill} events. */
    @GetMapping(path = "/v1/semaphores/{name}/events/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    SseEmitter semaphoreStream(@PathVariable String name,
            @RequestHeader(name = "Last-Event-ID", required = false) Long lastEventId,
            @RequestParam(defaultValue = "50") int backfill) {
        Validation.name(name);
        store.find(name).orElseThrow(() -> new SemaphoreNotFoundException(name));
        return open(sink -> {
            long cursor = lastEventId != null ? lastEventId : backfillCursor(sink, name, backfill);
            sink.state(name);
            while (sink.open()) {
                long seen = signals.version(name);
                List<SemaphoreEvent> batch = store.events(name, cursor, PAGE);
                if (batch.isEmpty()) {
                    sink.idle(() -> signals.awaitChange(name, seen, HEARTBEAT));
                    continue;
                }
                for (SemaphoreEvent e : batch) {
                    sink.event(e);
                    cursor = e.id();
                }
                if (batch.getLast().type() == EventType.DELETED) {
                    sink.complete();
                    return;
                }
                if (batch.size() < PAGE) {
                    sink.state(name);
                }
            }
        });
    }

    /** Every semaphore, multiplexed. Sends the last {@code backfill} events of each, then live changes. */
    @GetMapping(path = "/v1/events/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    SseEmitter allStream(@RequestParam(defaultValue = "20") int backfill) {
        return open(sink -> {
            Map<String, Long> cursors = new HashMap<>();
            Map<String, Long> versions = new HashMap<>();
            for (SemaphoreConfig config : store.list()) {
                versions.put(config.name(), signals.version(config.name()));
                cursors.put(config.name(), backfillCursor(sink, config.name(), backfill));
                sink.state(config.name());
            }
            while (sink.open()) {
                long seenGlobal = signals.globalVersion();
                Set<String> names = new LinkedHashSet<>(cursors.keySet());
                store.list().forEach(c -> names.add(c.name()));
                boolean sentAny = false;
                for (String name : names) {
                    long version = signals.version(name);
                    if (versions.containsKey(name) && versions.get(name) == version) {
                        continue;
                    }
                    versions.put(name, version);
                    long cursor = cursors.getOrDefault(name, 0L);
                    boolean deleted = false;
                    List<SemaphoreEvent> batch;
                    do {
                        batch = store.events(name, cursor, PAGE);
                        for (SemaphoreEvent e : batch) {
                            sink.event(e);
                            cursor = e.id();
                            deleted = e.type() == EventType.DELETED;
                        }
                    } while (batch.size() == PAGE);
                    sentAny = true;
                    if (deleted) {
                        cursors.remove(name);
                        versions.remove(name);
                    } else {
                        cursors.put(name, cursor);
                        sink.state(name);
                    }
                }
                if (!sentAny) {
                    sink.idle(() -> signals.awaitAnyChange(seenGlobal, HEARTBEAT));
                }
            }
        });
    }

    private long backfillCursor(Sink sink, String name, int backfill) throws IOException {
        int n = Math.clamp(backfill, 0, MAX_BACKFILL);
        List<SemaphoreEvent> recent = store.latestEvents(name, Math.max(n, 1));
        if (recent.isEmpty()) {
            return 0;
        }
        for (SemaphoreEvent e : recent.subList(recent.size() - Math.min(n, recent.size()), recent.size())) {
            sink.event(e);
        }
        return recent.getLast().id();
    }

    // ---------------------------------------------------------------- plumbing

    @FunctionalInterface
    private interface StreamBody {
        void run(Sink sink) throws IOException, InterruptedException;
    }

    @FunctionalInterface
    private interface Wait {
        void await() throws InterruptedException;
    }

    private SseEmitter open(StreamBody body) {
        SseEmitter emitter = new SseEmitter(0L);
        Sink sink = new Sink(emitter);
        Thread thread = Thread.ofVirtual().name("sse-" + streamIds.incrementAndGet()).unstarted(() -> {
            try {
                body.run(sink);
            } catch (IOException | IllegalStateException e) {
                log.debug("Event stream closed by client: {}", e.toString());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                log.warn("Event stream failed", e);
                emitter.completeWithError(e);
            } finally {
                sink.complete();
                streams.remove(Thread.currentThread());
            }
        });
        Runnable stop = () -> {
            sink.closed = true;
            thread.interrupt();
        };
        emitter.onCompletion(stop);
        emitter.onTimeout(stop);
        emitter.onError(e -> stop.run());
        streams.add(thread);
        thread.start();
        return emitter;
    }

    private final class Sink {
        private final SseEmitter emitter;
        private volatile boolean closed;
        private long lastSend = System.nanoTime();

        Sink(SseEmitter emitter) {
            this.emitter = emitter;
        }

        boolean open() {
            return !closed && running && !Thread.currentThread().isInterrupted();
        }

        void event(SemaphoreEvent e) throws IOException {
            send(SseEmitter.event().id(Long.toString(e.id())).name("semaphore-event").data(EventView.of(e)));
        }

        void state(String name) throws IOException {
            try {
                send(SseEmitter.event().name("state").data(StateView.of(store.state(name))));
            } catch (SemaphoreNotFoundException gone) {
                // Deleted between the event and the snapshot; the DELETED event says so.
            }
        }

        /** Waits for a change; sends a heartbeat if nothing was sent for a while. */
        void idle(Wait wait) throws IOException, InterruptedException {
            wait.await();
            if (System.nanoTime() - lastSend >= HEARTBEAT.toNanos()) {
                send(SseEmitter.event().comment("keep-alive"));
            }
        }

        private void send(SseEmitter.SseEventBuilder event) throws IOException {
            emitter.send(event);
            lastSend = System.nanoTime();
        }

        void complete() {
            if (!closed) {
                closed = true;
                emitter.complete();
            }
        }
    }

    // ---------------------------------------------------------------- lifecycle

    @Override
    public void start() {
        running = true;
    }

    /** Close streams before the web server's graceful shutdown waits for in-flight requests. */
    @Override
    public void stop() {
        running = false;
        streams.forEach(Thread::interrupt);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return SmartLifecycle.DEFAULT_PHASE - 512;
    }
}
