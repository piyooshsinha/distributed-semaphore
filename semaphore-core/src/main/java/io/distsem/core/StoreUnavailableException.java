package io.distsem.core;

/**
 * The backing store failed. For writes the outcome is unknown; callers may retry with the same
 * {@code requestId}, which is idempotent.
 */
public class StoreUnavailableException extends SemaphoreException {

    public StoreUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
