package io.distsem.client;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Response shapes of the service API, as seen by clients. */
public final class Model {

    private Model() {
    }

    public record SemaphoreInfo(String name, int capacity, long defaultTtlMs) {
    }

    public record PermitInfo(
            UUID permitId, String semaphore, String holderId, String requestId, long fencingToken,
            Instant acquiredAt, Instant expiresAt) {
    }

    public record WaiterInfo(
            int position, String holderId, String requestId, Instant enqueuedAt, Instant waitDeadline,
            Instant expiresAt) {
    }

    public record SemaphoreSnapshot(
            String name, int capacity, long defaultTtlMs, int held, int available, int waiting,
            long lastFencingToken, Instant observedAt, List<PermitInfo> holders, List<WaiterInfo> waiters) {
    }

    public record EventInfo(
            long id, String semaphore, String type, String holderId, String requestId, UUID permitId,
            Long fencingToken, String detail, Instant occurredAt) {
    }

    record AcquireReply(
            String status, PermitInfo permit, Integer position, Instant waitDeadline, Instant pollBefore,
            String reason, Integer available, Integer waiting) {
    }

    record Problem(String title, int status, String detail, String code) {
    }

    record ReleaseReply(boolean released) {
    }
}
