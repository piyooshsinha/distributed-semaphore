package io.distsem.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "distsem.security.api-key=s3cret")
class ApiKeyIT extends ServiceITSupport {

    @Test
    void v1RequiresKeyButHealthDoesNot() {
        assertThat(get("/v1/semaphores").getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(http().get().uri("/v1/semaphores").header("X-API-Key", "wrong").retrieve()
                .toBodilessEntity().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(http().get().uri("/v1/semaphores").header("X-API-Key", "s3cret").retrieve()
                .toBodilessEntity().getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(get("/actuator/health").getStatusCode()).isEqualTo(HttpStatus.OK);
    }
}
