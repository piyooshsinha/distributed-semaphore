package io.distsem.core;

import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

/** Argument checks shared by the domain records. */
public final class Validation {

    public static final int MAX_NAME_LENGTH = 128;
    public static final int MAX_HOLDER_ID_LENGTH = 256;
    public static final int MAX_REQUEST_ID_LENGTH = 128;
    public static final int MAX_CAPACITY = 1_000_000;

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}");

    private Validation() {
    }

    public static String name(String name) {
        Objects.requireNonNull(name, "semaphore name");
        if (!NAME.matcher(name).matches()) {
            throw new IllegalArgumentException(
                    "semaphore name must match " + NAME.pattern() + " but was '" + name + "'");
        }
        return name;
    }

    public static String identifier(String value, String what, int maxLength) {
        Objects.requireNonNull(value, what);
        if (value.isBlank() || value.length() > maxLength) {
            throw new IllegalArgumentException(what + " must be 1.." + maxLength + " non-blank characters");
        }
        return value;
    }

    public static int capacity(int capacity) {
        if (capacity < 1 || capacity > MAX_CAPACITY) {
            throw new IllegalArgumentException("capacity must be in [1, " + MAX_CAPACITY + "] but was " + capacity);
        }
        return capacity;
    }

    public static Duration positive(Duration value, String what) {
        Objects.requireNonNull(value, what);
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException(what + " must be positive but was " + value);
        }
        return value;
    }

    public static Duration nonNegative(Duration value, String what) {
        Objects.requireNonNull(value, what);
        if (value.isNegative()) {
            throw new IllegalArgumentException(what + " must not be negative but was " + value);
        }
        return value;
    }
}
