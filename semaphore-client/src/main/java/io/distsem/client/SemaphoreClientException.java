package io.distsem.client;

/**
 * A request failed. {@code status} is the HTTP status (0 if the service could not be reached) and
 * {@code code} the service's machine-readable error code, if any.
 */
public class SemaphoreClientException extends RuntimeException {

    private final int status;
    private final String code;

    public SemaphoreClientException(String message, int status, String code) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public SemaphoreClientException(String message, Throwable cause) {
        super(message, cause);
        this.status = 0;
        this.code = "UNREACHABLE";
    }

    public int status() {
        return status;
    }

    public String code() {
        return code;
    }

    public boolean isNotFound() {
        return status == 404;
    }
}
