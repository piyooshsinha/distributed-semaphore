package io.distsem.core;

public class SemaphoreAlreadyExistsException extends SemaphoreException {

    private final SemaphoreConfig existing;

    public SemaphoreAlreadyExistsException(SemaphoreConfig existing) {
        super("semaphore '" + existing.name() + "' already exists with a different configuration: " + existing);
        this.existing = existing;
    }

    public SemaphoreConfig existing() {
        return existing;
    }
}
