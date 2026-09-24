package io.distsem.service.web;

import io.distsem.core.AcquireRequest;
import io.distsem.core.AcquireResult;
import io.distsem.core.SemaphoreConfig;
import io.distsem.core.SemaphoreNotFoundException;
import io.distsem.core.SemaphoreStore;
import io.distsem.core.Validation;
import io.distsem.service.core.PermitCoordinator;
import io.distsem.service.web.ApiModels.AcquireBody;
import io.distsem.service.web.ApiModels.AcquireResponse;
import io.distsem.service.web.ApiModels.EventView;
import io.distsem.service.web.ApiModels.PermitView;
import io.distsem.service.web.ApiModels.ReleaseResponse;
import io.distsem.service.web.ApiModels.RenewBody;
import io.distsem.service.web.ApiModels.SemaphoreBody;
import io.distsem.service.web.ApiModels.SemaphoreView;
import io.distsem.service.web.ApiModels.StateView;
import jakarta.validation.Valid;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/semaphores")
class SemaphoreController {

    private final SemaphoreStore store;
    private final PermitCoordinator permits;

    SemaphoreController(SemaphoreStore store, PermitCoordinator permits) {
        this.store = store;
        this.permits = permits;
    }

    @GetMapping
    List<SemaphoreView> list() {
        return store.list().stream().map(SemaphoreView::of).toList();
    }

    /** Creates the semaphore, or updates its settings if it exists. Current holders keep their permits. */
    @PutMapping("/{name}")
    ResponseEntity<SemaphoreView> put(@PathVariable String name, @Valid @RequestBody SemaphoreBody body) {
        SemaphoreConfig config = new SemaphoreConfig(
                Validation.name(name), body.capacity(), Duration.ofMillis(body.defaultTtlMs()));
        if (store.find(name).isPresent()) {
            return ResponseEntity.ok(SemaphoreView.of(store.update(config)));
        }
        try {
            return ResponseEntity.status(HttpStatus.CREATED).body(SemaphoreView.of(store.create(config)));
        } catch (io.distsem.core.SemaphoreAlreadyExistsException raced) {
            return ResponseEntity.ok(SemaphoreView.of(store.update(config)));
        }
    }

    @GetMapping("/{name}")
    StateView state(@PathVariable String name) {
        return StateView.of(store.state(Validation.name(name)));
    }

    @DeleteMapping("/{name}")
    ResponseEntity<Void> delete(@PathVariable String name) {
        return store.delete(Validation.name(name))
                ? ResponseEntity.noContent().build()
                : ResponseEntity.notFound().build();
    }

    /**
     * 200 GRANTED with the permit; 202 QUEUED (call again with the same requestId before
     * {@code pollBefore}); 409 REJECTED when no slot is free and the caller did not wait, or its
     * wait deadline passed.
     */
    @PostMapping("/{name}/acquire")
    ResponseEntity<AcquireResponse> acquire(@PathVariable String name, @Valid @RequestBody AcquireBody body)
            throws InterruptedException {
        AcquireRequest request = new AcquireRequest(
                Validation.name(name), body.holderId(), body.requestId(),
                Optional.ofNullable(body.ttlMs()).map(Duration::ofMillis),
                Duration.ofMillis(Optional.ofNullable(body.waitTimeoutMs()).orElse(0L)));
        AcquireResult result = permits.acquire(request);
        HttpStatus status = switch (result) {
            case AcquireResult.Granted g -> HttpStatus.OK;
            case AcquireResult.Queued q -> HttpStatus.ACCEPTED;
            case AcquireResult.Rejected r -> HttpStatus.CONFLICT;
        };
        return ResponseEntity.status(status).body(AcquireResponse.of(result));
    }

    @DeleteMapping("/{name}/waiters/{requestId}")
    ResponseEntity<Void> cancelWait(@PathVariable String name, @PathVariable String requestId) {
        return permits.cancelWait(Validation.name(name), requestId)
                ? ResponseEntity.noContent().build()
                : ResponseEntity.notFound().build();
    }

    @PostMapping("/{name}/permits/{permitId}/renew")
    PermitView renew(@PathVariable String name, @PathVariable UUID permitId,
            @Valid @RequestBody(required = false) RenewBody body) {
        Validation.name(name);
        Duration ttl = body != null && body.ttlMs() != null
                ? Duration.ofMillis(body.ttlMs())
                : store.find(name).orElseThrow(() -> new SemaphoreNotFoundException(name)).defaultTtl();
        return PermitView.of(permits.renew(name, permitId, ttl));
    }

    /** Idempotent. {@code released=false} means the permit was not held: already released, or its lease expired. */
    @DeleteMapping("/{name}/permits/{permitId}")
    ReleaseResponse release(@PathVariable String name, @PathVariable UUID permitId) {
        return new ReleaseResponse(permits.release(Validation.name(name), permitId));
    }

    @GetMapping("/{name}/events")
    List<EventView> events(@PathVariable String name,
            @RequestParam(defaultValue = "0") long after,
            @RequestParam(defaultValue = "100") int limit) {
        return store.events(Validation.name(name), after, limit).stream().map(EventView::of).toList();
    }
}
