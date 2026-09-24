package io.distsem.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DomainValidationTest {

    @ParameterizedTest
    @ValueSource(strings = {"db-flush", "log.processor:v2", "A", "x_1"})
    void acceptsValidNames(String name) {
        assertThat(Validation.name(name)).isEqualTo(name);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "-leading-dash", "has space", "slash/name", "semi;colon"})
    void rejectsInvalidNames(String name) {
        assertThatIllegalArgumentException().isThrownBy(() -> Validation.name(name));
    }

    @Test
    void rejectsNameLongerThan128() {
        assertThatIllegalArgumentException().isThrownBy(() -> Validation.name("a".repeat(129)));
        assertThat(Validation.name("a".repeat(128))).hasSize(128);
    }

    @Test
    void configRequiresPositiveCapacityAndTtl() {
        assertThatIllegalArgumentException().isThrownBy(() -> new SemaphoreConfig("s", 0, Duration.ofSeconds(1)));
        assertThatIllegalArgumentException().isThrownBy(() -> new SemaphoreConfig("s", 1, Duration.ZERO));
        assertThatIllegalArgumentException().isThrownBy(() -> new SemaphoreConfig("s", 1, Duration.ofSeconds(-1)));
    }

    @Test
    void acquireRequestDefaultsAndChecks() {
        AcquireRequest request = AcquireRequest.tryOnce("s", "worker-1", "req-1");
        assertThat(request.ttl()).isEmpty();
        assertThat(request.waitTimeout()).isZero();

        assertThatIllegalArgumentException().isThrownBy(() -> request.withTtl(Duration.ZERO));
        assertThatIllegalArgumentException().isThrownBy(() -> request.withWaitTimeout(Duration.ofSeconds(-1)));
        assertThatIllegalArgumentException().isThrownBy(() -> AcquireRequest.tryOnce("s", " ", "req-1"));
        assertThatIllegalArgumentException().isThrownBy(() -> AcquireRequest.tryOnce("s", "w", "r".repeat(129)));
        assertThat(new AcquireRequest("s", "w", "r", null, Duration.ZERO).ttl()).isEqualTo(Optional.empty());
    }

    @Test
    void availableNeverNegativeWhenCapacityLoweredBelowHolders() {
        var permit = new Permit(java.util.UUID.randomUUID(), "s", "w", "r", 1,
                java.time.Instant.EPOCH, java.time.Instant.EPOCH.plusSeconds(1));
        var state = new SemaphoreState(new SemaphoreConfig("s", 1, Duration.ofSeconds(1)),
                List.of(permit, permit), List.of(), 2, java.time.Instant.EPOCH);
        assertThat(state.available()).isZero();
    }
}
