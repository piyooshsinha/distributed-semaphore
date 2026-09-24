package io.distsem.core;

public class SemaphoreNotFoundException extends SemaphoreException {

    private final String semaphore;

    public SemaphoreNotFoundException(String semaphore) {
        super("semaphore '" + semaphore + "' does not exist");
        this.semaphore = semaphore;
    }

    public String semaphore() {
        return semaphore;
    }
}
