package io.distsem.core;

import java.util.UUID;

/** The permit was never granted, was released, or its lease expired. */
public class PermitNotFoundException extends SemaphoreException {

    private final String semaphore;
    private final UUID permitId;

    public PermitNotFoundException(String semaphore, UUID permitId) {
        super("permit " + permitId + " is not held on semaphore '" + semaphore + "' (released or expired)");
        this.semaphore = semaphore;
        this.permitId = permitId;
    }

    public String semaphore() {
        return semaphore;
    }

    public UUID permitId() {
        return permitId;
    }
}
