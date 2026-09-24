package io.distsem.core;

import java.time.Duration;

/**
 * Registry entry for a named semaphore.
 *
 * @param name       unique semaphore name
 * @param capacity   maximum number of permits that may be held concurrently
 * @param defaultTtl lease duration used when an acquire request does not specify one
 */
public record SemaphoreConfig(String name, int capacity, Duration defaultTtl) {

    public SemaphoreConfig {
        Validation.name(name);
        Validation.capacity(capacity);
        Validation.positive(defaultTtl, "defaultTtl");
    }
}
