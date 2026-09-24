package io.distsem.service.web;

import io.distsem.core.PermitNotFoundException;
import io.distsem.core.SemaphoreAlreadyExistsException;
import io.distsem.core.SemaphoreException;
import io.distsem.core.SemaphoreNotFoundException;
import io.distsem.core.StoreUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/** Maps domain errors to RFC 9457 problem details with a stable machine-readable {@code code}. */
@RestControllerAdvice
class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(SemaphoreNotFoundException.class)
    ResponseEntity<ProblemDetail> semaphoreNotFound(SemaphoreNotFoundException e) {
        return problem(HttpStatus.NOT_FOUND, "SEMAPHORE_NOT_FOUND", e.getMessage());
    }

    @ExceptionHandler(PermitNotFoundException.class)
    ResponseEntity<ProblemDetail> permitNotFound(PermitNotFoundException e) {
        return problem(HttpStatus.NOT_FOUND, "PERMIT_NOT_HELD", e.getMessage());
    }

    @ExceptionHandler(SemaphoreAlreadyExistsException.class)
    ResponseEntity<ProblemDetail> alreadyExists(SemaphoreAlreadyExistsException e) {
        return problem(HttpStatus.CONFLICT, "SEMAPHORE_EXISTS", e.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<ProblemDetail> badRequest(IllegalArgumentException e) {
        return problem(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", e.getMessage());
    }

    @ExceptionHandler(StoreUnavailableException.class)
    ResponseEntity<ProblemDetail> storeUnavailable(StoreUnavailableException e) {
        log.warn("Store unavailable: {}", e.getMessage(), e.getCause());
        ResponseEntity<ProblemDetail> response = problem(HttpStatus.SERVICE_UNAVAILABLE, "STORE_UNAVAILABLE",
                "The semaphore store is unavailable. Retrying with the same requestId is safe.");
        return ResponseEntity.status(response.getStatusCode()).header(HttpHeaders.RETRY_AFTER, "1").body(response.getBody());
    }

    @ExceptionHandler(InterruptedException.class)
    ResponseEntity<ProblemDetail> interrupted(InterruptedException e) {
        Thread.currentThread().interrupt();
        return problem(HttpStatus.SERVICE_UNAVAILABLE, "SHUTTING_DOWN", "The server is shutting down; retry.");
    }

    @ExceptionHandler(SemaphoreException.class)
    ResponseEntity<ProblemDetail> internal(SemaphoreException e) {
        log.error("Unexpected semaphore error", e);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL", e.getMessage());
    }

    private static ResponseEntity<ProblemDetail> problem(HttpStatus status, String code, String detail) {
        ProblemDetail body = ProblemDetail.forStatusAndDetail(status, detail);
        body.setProperty("code", code);
        return ResponseEntity.status(status).body(body);
    }
}
