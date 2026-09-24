package io.distsem.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.zaxxer.hikari.HikariDataSource;
import io.distsem.postgres.PostgresSemaphoreStore;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                // A long fallback poll proves that fast wake-ups come from LISTEN/NOTIFY.
                "distsem.long-poll.fallback-poll=5s",
                "distsem.store.waiter-lease=20s",
                "distsem.long-poll.max-hold=2s"
        })
class SemaphoreApiIT extends ServiceITSupport {

    private String createSemaphore(int capacity) {
        String name = "sem-" + UUID.randomUUID().toString().substring(0, 8);
        ResponseEntity<Map<String, Object>> created = put("/v1/semaphores/" + name,
                Map.of("capacity", capacity, "defaultTtlMs", 30_000));
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return name;
    }

    @Test
    void createUpdateListAndDelete() {
        String name = createSemaphore(2);
        ResponseEntity<Map<String, Object>> updated = put("/v1/semaphores/" + name,
                Map.of("capacity", 4, "defaultTtlMs", 10_000));
        assertThat(updated.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(updated.getBody()).containsEntry("capacity", 4);

        List<Map<String, Object>> all = http().get().uri("/v1/semaphores").retrieve().body(JSON_LIST);
        assertThat(all).anySatisfy(s -> assertThat(s).containsEntry("name", name).containsEntry("capacity", 4));

        assertThat(delete("/v1/semaphores/" + name).getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
        assertThat(delete("/v1/semaphores/" + name).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        ResponseEntity<Map<String, Object>> missing = get("/v1/semaphores/" + name);
        assertThat(missing.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(missing.getBody()).containsEntry("code", "SEMAPHORE_NOT_FOUND");
    }

    @Test
    void acquireRenewReleaseLifecycle() {
        String name = createSemaphore(1);
        ResponseEntity<Map<String, Object>> granted = post("/v1/semaphores/" + name + "/acquire",
                acquireBody("worker-1", "req-1", null));
        assertThat(granted.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(granted.getBody()).containsEntry("status", "GRANTED");
        Map<String, Object> permit = map(granted.getBody().get("permit"));
        assertThat(permit).containsEntry("fencingToken", 1).containsEntry("holderId", "worker-1");
        String permitId = (String) permit.get("permitId");

        ResponseEntity<Map<String, Object>> rejected = post("/v1/semaphores/" + name + "/acquire",
                acquireBody("worker-2", "req-2", null));
        assertThat(rejected.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(rejected.getBody()).containsEntry("status", "REJECTED").containsEntry("reason", "NO_CAPACITY");

        ResponseEntity<Map<String, Object>> state = get("/v1/semaphores/" + name);
        assertThat(state.getBody()).containsEntry("held", 1).containsEntry("available", 0);

        ResponseEntity<Map<String, Object>> renewed = post(
                "/v1/semaphores/" + name + "/permits/" + permitId + "/renew", Map.of("ttlMs", 60_000));
        assertThat(renewed.getStatusCode()).isEqualTo(HttpStatus.OK);

        assertThat(delete("/v1/semaphores/" + name + "/permits/" + permitId).getBody()).containsEntry("released", true);
        assertThat(delete("/v1/semaphores/" + name + "/permits/" + permitId).getBody()).containsEntry("released", false);

        ResponseEntity<Map<String, Object>> renewAfterRelease = post(
                "/v1/semaphores/" + name + "/permits/" + permitId + "/renew", Map.of());
        assertThat(renewAfterRelease.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(renewAfterRelease.getBody()).containsEntry("code", "PERMIT_NOT_HELD");

        List<Map<String, Object>> events = http().get().uri("/v1/semaphores/" + name + "/events")
                .retrieve().body(JSON_LIST);
        assertThat(events).extracting(e -> e.get("type"))
                .containsExactly("CREATED", "ACQUIRED", "RENEWED", "RELEASED");
    }

    @Test
    void waiterIsWokenByReleaseOnAnotherReplica() throws Exception {
        String name = createSemaphore(1);
        post("/v1/semaphores/" + name + "/acquire", acquireBody("holder", "held", null));
        String permitId = (String) map(get("/v1/semaphores/" + name).getBody().get("holders") instanceof List<?> l
                ? l.getFirst() : null).get("permitId");

        CompletableFuture<ResponseEntity<Map<String, Object>>> waiting = CompletableFuture.supplyAsync(() ->
                post("/v1/semaphores/" + name + "/acquire", acquireBody("waiter", "wait-1", 10_000L)));
        await().atMost(Duration.ofSeconds(5)).until(() -> ((Number) get("/v1/semaphores/" + name).getBody()
                .get("waiting")).intValue() == 1);

        try (HikariDataSource otherReplica = new HikariDataSource()) {
            otherReplica.setJdbcUrl(POSTGRES.getJdbcUrl());
            otherReplica.setUsername(POSTGRES.getUsername());
            otherReplica.setPassword(POSTGRES.getPassword());
            long releasedAt = System.nanoTime();
            assertThat(new PostgresSemaphoreStore(otherReplica).release(name, UUID.fromString(permitId))).isTrue();

            ResponseEntity<Map<String, Object>> result = waiting.get(5, TimeUnit.SECONDS);
            long wokeAfterMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - releasedAt);
            assertThat(result.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(wokeAfterMillis).as("woken by NOTIFY, not by the 5s fallback poll").isLessThan(1500);
        }
    }

    @Test
    void longPollAnswersQueuedAfterMaxHoldAndCanBeCancelled() {
        String name = createSemaphore(1);
        post("/v1/semaphores/" + name + "/acquire", acquireBody("holder", "held", null));

        long start = System.nanoTime();
        ResponseEntity<Map<String, Object>> queued = post("/v1/semaphores/" + name + "/acquire",
                acquireBody("waiter", "wait-1", 60_000L));
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertThat(queued.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(queued.getBody()).containsEntry("status", "QUEUED").containsEntry("position", 0)
                .containsKeys("waitDeadline", "pollBefore");
        assertThat(elapsed).isBetween(1_800L, 4_000L);

        assertThat(delete("/v1/semaphores/" + name + "/waiters/wait-1").getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
        assertThat(get("/v1/semaphores/" + name).getBody()).containsEntry("waiting", 0);
    }

    @Test
    void validationErrorsAreProblemDetails() {
        String name = createSemaphore(1);
        ResponseEntity<Map<String, Object>> missingFields = post("/v1/semaphores/" + name + "/acquire", Map.of());
        assertThat(missingFields.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        ResponseEntity<Map<String, Object>> badName = get("/v1/semaphores/bad%20name");
        assertThat(badName.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(badName.getBody()).containsEntry("code", "INVALID_REQUEST");

        ResponseEntity<Map<String, Object>> tooLongTtl = post("/v1/semaphores/" + name + "/acquire",
                Map.of("holderId", "h", "requestId", "r", "ttlMs", 7_200_000));
        assertThat(tooLongTtl.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        ResponseEntity<Map<String, Object>> badCapacity = put("/v1/semaphores/x", Map.of("capacity", 0, "defaultTtlMs", 1000));
        assertThat(badCapacity.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        ResponseEntity<Map<String, Object>> badUuid = delete("/v1/semaphores/" + name + "/permits/not-a-uuid");
        assertThat(badUuid.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void eventStreamPushesEventsAndStateSnapshots() throws Exception {
        String name = createSemaphore(2);
        List<String> lines = new CopyOnWriteArrayList<>();
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(
                URI.create("http://localhost:" + port + "/v1/semaphores/" + name + "/events/stream?backfill=10"))
                .header("Accept", "text/event-stream").build();
        CompletableFuture<Void> reader = client.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream())
                .thenAccept(response -> {
                    try (BufferedReader in = new BufferedReader(
                            new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
                        String line;
                        while ((line = in.readLine()) != null) {
                            lines.add(line);
                        }
                    } catch (Exception ignored) {
                        // stream closed
                    }
                });

        await().atMost(Duration.ofSeconds(5)).until(() -> lines.stream().anyMatch(l -> l.contains("\"CREATED\"")));
        post("/v1/semaphores/" + name + "/acquire", acquireBody("streamer", "s-1", null));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(lines).anyMatch(l -> l.startsWith("data:") && l.contains("\"ACQUIRED\"")
                    && l.contains("\"holderId\":\"streamer\""));
            assertThat(lines).anyMatch(l -> l.startsWith("event:state"));
            assertThat(lines).anyMatch(l -> l.startsWith("data:") && l.contains("\"held\":1"));
        });

        delete("/v1/semaphores/" + name);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(lines).anyMatch(l -> l.contains("\"DELETED\"")));
        reader.get(5, TimeUnit.SECONDS);
    }

    @Test
    void multiplexedStreamCoversAllSemaphores() throws Exception {
        List<String> lines = new CopyOnWriteArrayList<>();
        HttpRequest request = HttpRequest.newBuilder(
                URI.create("http://localhost:" + port + "/v1/events/stream?backfill=0")).build();
        CompletableFuture<?> reader = HttpClient.newHttpClient()
                .sendAsync(request, HttpResponse.BodyHandlers.ofLines())
                .thenAccept(r -> r.body().forEach(lines::add));

        Thread.sleep(300);
        String a = createSemaphore(1);
        String b = createSemaphore(1);
        post("/v1/semaphores/" + b + "/acquire", acquireBody("x", "r", null));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(lines).anyMatch(l -> l.contains("\"semaphore\":\"" + a + "\"") && l.contains("CREATED"));
            assertThat(lines).anyMatch(l -> l.contains("\"semaphore\":\"" + b + "\"") && l.contains("ACQUIRED"));
        });
        reader.cancel(true);
    }

    @Test
    void healthAndMetricsAreExposed() {
        ResponseEntity<Map<String, Object>> health = get("/actuator/health");
        assertThat(health.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(health.getBody()).containsEntry("status", "UP");
        assertThat(map(map(health.getBody().get("components")).get("eventListener"))).containsEntry("status", "UP");

        String name = createSemaphore(1);
        post("/v1/semaphores/" + name + "/acquire", acquireBody("m", "m-1", null));
        String prometheus = http().get().uri("/actuator/prometheus").retrieve().body(String.class);
        assertThat(prometheus).contains("distsem_acquire_total{").contains("outcome=\"granted\"")
                .contains("distsem_acquire_duration_seconds_bucket");

        assertThat(get("/v3/api-docs").getStatusCode()).isEqualTo(HttpStatus.OK);
    }
}
