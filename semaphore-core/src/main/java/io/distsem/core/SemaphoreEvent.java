package io.distsem.core;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** One audit-log entry. Fields that do not apply to the event type are null. */
public record SemaphoreEvent(
        long id,
        String semaphore,
        EventType type,
        String holderId,
        String requestId,
        UUID permitId,
        Long fencingToken,
        String detail,
        Instant occurredAt) {

    public SemaphoreEvent {
        Objects.requireNonNull(semaphore, "semaphore");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(occurredAt, "occurredAt");
    }
}
