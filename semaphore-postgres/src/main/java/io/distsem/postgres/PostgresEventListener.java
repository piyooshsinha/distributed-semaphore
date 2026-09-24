package io.distsem.postgres;

import io.distsem.core.EventType;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receives the {@code distsem_events} notifications published by the schema's event trigger and
 * fans them out to in-process subscribers.
 *
 * <p>Notifications sent while the listener is disconnected are lost, so after every (re)connect
 * subscribers get {@link Subscriber#onResync()} and should re-read whatever state they track.
 * Subscribers must return quickly; they are called on the listener thread.
 */
public final class PostgresEventListener implements AutoCloseable {

    public static final String CHANNEL = "distsem_events";

    private static final Logger log = LoggerFactory.getLogger(PostgresEventListener.class);
    private static final Duration POLL = Duration.ofMillis(500);
    private static final Duration LIVENESS_CHECK = Duration.ofSeconds(15);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(10);

    /** One notification: an audit event was committed. */
    public record Notification(String semaphore, long eventId, EventType type) {

        static Optional<Notification> parse(String payload) {
            String[] parts = payload.split("\\|", 3);
            if (parts.length != 3) {
                return Optional.empty();
            }
            try {
                return Optional.of(new Notification(parts[0], Long.parseLong(parts[1]), EventType.valueOf(parts[2])));
            } catch (IllegalArgumentException e) {
                return Optional.empty();
            }
        }
    }

    public interface Subscriber {
        void onEvent(Notification notification);

        /** The listener (re)connected; notifications may have been missed. */
        void onResync();
    }

    private final DataSource dataSource;
    private final CopyOnWriteArrayList<Subscriber> subscribers = new CopyOnWriteArrayList<>();
    private volatile boolean running;
    private volatile boolean connected;
    private Thread thread;

    public PostgresEventListener(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        thread = Thread.ofPlatform().name("distsem-listener").daemon().start(this::run);
    }

    /** Registers a subscriber. Closing the returned handle unregisters it. */
    public AutoCloseable subscribe(Subscriber subscriber) {
        subscribers.add(Objects.requireNonNull(subscriber, "subscriber"));
        return () -> subscribers.remove(subscriber);
    }

    public boolean isConnected() {
        return connected;
    }

    @Override
    public synchronized void close() throws InterruptedException {
        running = false;
        if (thread != null) {
            thread.interrupt();
            thread.join(TimeUnit.SECONDS.toMillis(5));
        }
    }

    private void run() {
        long backoffMillis = 100;
        while (running) {
            try (Connection connection = dataSource.getConnection()) {
                connection.setAutoCommit(true);
                try (Statement s = connection.createStatement()) {
                    s.execute("LISTEN " + CHANNEL);
                }
                connected = true;
                backoffMillis = 100;
                log.info("Listening for semaphore events on channel {}", CHANNEL);
                forEachSubscriber(Subscriber::onResync);
                pump(connection);
            } catch (SQLException | RuntimeException e) {
                if (running) {
                    log.warn("Event listener connection lost; reconnecting in {}ms: {}", backoffMillis, e.toString());
                }
            } finally {
                connected = false;
            }
            if (running) {
                sleep(backoffMillis);
                backoffMillis = Math.min(backoffMillis * 2, MAX_BACKOFF.toMillis());
            }
        }
    }

    private void pump(Connection connection) throws SQLException {
        PGConnection pg = connection.unwrap(PGConnection.class);
        long nextLivenessCheck = System.nanoTime() + LIVENESS_CHECK.toNanos();
        while (running) {
            PGNotification[] notifications = pg.getNotifications((int) POLL.toMillis());
            if (notifications != null) {
                for (PGNotification n : notifications) {
                    Notification.parse(n.getParameter()).ifPresentOrElse(
                            parsed -> forEachSubscriber(s -> s.onEvent(parsed)),
                            () -> log.warn("Ignoring malformed notification payload '{}'", n.getParameter()));
                }
            }
            // A half-open socket never errors on its own; a round trip detects it.
            if (System.nanoTime() - nextLivenessCheck > 0) {
                try (Statement s = connection.createStatement()) {
                    s.execute("SELECT 1");
                }
                nextLivenessCheck = System.nanoTime() + LIVENESS_CHECK.toNanos();
            }
        }
    }

    private void forEachSubscriber(java.util.function.Consumer<Subscriber> action) {
        for (Subscriber subscriber : subscribers) {
            try {
                action.accept(subscriber);
            } catch (RuntimeException e) {
                log.warn("Event subscriber {} failed", subscriber, e);
            }
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }
}
