package io.distsem.service;

import java.util.List;
import java.util.Map;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Shared Postgres container and a non-throwing HTTP client for service integration tests. */
public abstract class ServiceITSupport {

    protected static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    protected static final ParameterizedTypeReference<Map<String, Object>> JSON = new ParameterizedTypeReference<>() {
    };
    protected static final ParameterizedTypeReference<List<Map<String, Object>>> JSON_LIST =
            new ParameterizedTypeReference<>() {
            };

    @LocalServerPort
    protected int port;

    protected RestClient http() {
        return RestClient.builder()
                .baseUrl("http://localhost:" + port)
                .defaultStatusHandler(status -> true, (request, response) -> {
                })
                .build();
    }

    protected ResponseEntity<Map<String, Object>> put(String path, Object body) {
        return http().put().uri(path).contentType(MediaType.APPLICATION_JSON).body(body).retrieve().toEntity(JSON);
    }

    protected ResponseEntity<Map<String, Object>> post(String path, Object body) {
        return http().post().uri(path).contentType(MediaType.APPLICATION_JSON).body(body).retrieve().toEntity(JSON);
    }

    protected ResponseEntity<Map<String, Object>> get(String path) {
        return http().get().uri(path).retrieve().toEntity(JSON);
    }

    protected ResponseEntity<Map<String, Object>> delete(String path) {
        return http().delete().uri(path).retrieve().toEntity(JSON);
    }

    protected static Map<String, Object> acquireBody(String holder, String requestId, Long waitTimeoutMs) {
        return waitTimeoutMs == null
                ? Map.of("holderId", holder, "requestId", requestId)
                : Map.of("holderId", holder, "requestId", requestId, "waitTimeoutMs", waitTimeoutMs);
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> map(Object value) {
        return (Map<String, Object>) value;
    }
}
