package io.distsem.core;

/** Base type for all semaphore errors. */
public class SemaphoreException extends RuntimeException {

    public SemaphoreException(String message) {
        super(message);
    }

    public SemaphoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
