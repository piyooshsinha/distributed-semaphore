package io.distsem.client;

/** No permit was granted: the semaphore was full and the caller did not wait, or its wait ran out. */
public class AcquireRejectedException extends SemaphoreClientException {

    public enum Reason { NO_CAPACITY, WAIT_TIMEOUT }

    private final Reason reason;

    public AcquireRejectedException(String semaphore, Reason reason) {
        super("could not acquire '" + semaphore + "': " + reason, 409, reason.name());
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}
