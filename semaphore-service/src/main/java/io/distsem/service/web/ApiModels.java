package io.distsem.service.web;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.distsem.core.AcquireResult;
import io.distsem.core.Permit;
import io.distsem.core.SemaphoreConfig;
import io.distsem.core.SemaphoreEvent;
import io.distsem.core.SemaphoreState;
import io.distsem.core.Validation;
import io.distsem.core.Waiter;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Request and response bodies of the HTTP API. Durations are milliseconds. */
public final class ApiModels {

    private ApiModels() {
    }

    public record SemaphoreBody(
            @NotNull @Min(1) @Max(Validation.MAX_CAPACITY) Integer capacity,
            @NotNull @Positive Long defaultTtlMs) {
    }

    public record SemaphoreView(String name, int capacity, long defaultTtlMs) {
        static SemaphoreView of(SemaphoreConfig config) {
            return new SemaphoreView(config.name(), config.capacity(), config.defaultTtl().toMillis());
        }
    }

    public record PermitView(
            UUID permitId, String semaphore, String holderId, String requestId, long fencingToken,
            Instant acquiredAt, Instant expiresAt) {
        static PermitView of(Permit p) {
            return new PermitView(p.permitId(), p.semaphore(), p.holderId(), p.requestId(), p.fencingToken(),
                    p.acquiredAt(), p.expiresAt());
        }
    }

    public record WaiterView(
            int position, String holderId, String requestId, Instant enqueuedAt, Instant waitDeadline,
            Instant expiresAt) {
        static WaiterView of(int position, Waiter w) {
            return new WaiterView(position, w.holderId(), w.requestId(), w.enqueuedAt(), w.waitDeadline(), w.expiresAt());
        }
    }

    public record StateView(
            String name, int capacity, long defaultTtlMs, int held, int available, int waiting,
            long lastFencingToken, Instant observedAt, List<PermitView> holders, List<WaiterView> waiters) {
        static StateView of(SemaphoreState s) {
            List<Waiter> waiters = s.waiters();
            return new StateView(
                    s.config().name(), s.config().capacity(), s.config().defaultTtl().toMillis(),
                    s.holders().size(), s.available(), waiters.size(), s.lastFencingToken(), s.observedAt(),
                    s.holders().stream().map(PermitView::of).toList(),
                    java.util.stream.IntStream.range(0, waiters.size())
                            .mapToObj(i -> WaiterView.of(i, waiters.get(i))).toList());
        }
    }

    /**
     * @param ttlMs         lease; the semaphore's default when absent
     * @param waitTimeoutMs how long to queue in total; 0 or absent means fail fast. A single call
     *                      blocks for at most the server's long-poll limit and then answers QUEUED;
     *                      call again with the same requestId to keep waiting.
     */
    public record AcquireBody(
            @NotBlank @Size(max = Validation.MAX_HOLDER_ID_LENGTH) String holderId,
            @NotBlank @Size(max = Validation.MAX_REQUEST_ID_LENGTH) String requestId,
            @Positive Long ttlMs,
            @PositiveOrZero Long waitTimeoutMs) {
    }

    public enum AcquireStatus { GRANTED, QUEUED, REJECTED }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record AcquireResponse(
            AcquireStatus status,
            PermitView permit,
            Integer position,
            Instant waitDeadline,
            Instant pollBefore,
            AcquireResult.Reason reason,
            Integer available,
            Integer waiting) {

        static AcquireResponse of(AcquireResult result) {
            return switch (result) {
                case AcquireResult.Granted g ->
                        new AcquireResponse(AcquireStatus.GRANTED, PermitView.of(g.permit()), null, null, null, null, null, null);
                case AcquireResult.Queued q ->
                        new AcquireResponse(AcquireStatus.QUEUED, null, q.position(), q.waitDeadline(), q.expiresAt(), null, null, null);
                case AcquireResult.Rejected r ->
                        new AcquireResponse(AcquireStatus.REJECTED, null, null, null, null, r.reason(), r.available(), r.waiting());
            };
        }
    }

    public record RenewBody(@Positive Long ttlMs) {
    }

    public record ReleaseResponse(boolean released) {
    }

    public record EventView(
            long id, String semaphore, String type, String holderId, String requestId, UUID permitId,
            Long fencingToken, String detail, Instant occurredAt) {
        static EventView of(SemaphoreEvent e) {
            return new EventView(e.id(), e.semaphore(), e.type().name(), e.holderId(), e.requestId(), e.permitId(),
                    e.fencingToken(), e.detail(), e.occurredAt());
        }
    }
}
