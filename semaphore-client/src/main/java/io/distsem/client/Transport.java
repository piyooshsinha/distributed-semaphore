package io.distsem.client;

import io.distsem.client.Model.Problem;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * HTTP plumbing with fail-over. Requests go to the endpoint that last worked; on a connection
 * failure, timeout or 502/503/504 the next endpoint is tried, with backoff between full rounds.
 * Every API call the SDK retries is idempotent on the server (acquire is keyed by requestId).
 */
final class Transport {

    private static final Logger log = LoggerFactory.getLogger(Transport.class);

    enum Retry {
        /** Safe to resend even if the first attempt may have reached the server. */
        IDEMPOTENT,
        /** Resend only if the connection was never established. */
        CONNECT_ONLY
    }

    record Response(int status, String body, URI endpoint) {
        boolean ok() {
            return status >= 200 && status < 300;
        }
    }

    final JsonMapper json = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private final List<URI> endpoints;
    private final String apiKey;
    private final Duration requestTimeout;
    private final int maxRounds;
    private final HttpClient http;
    private final AtomicInteger preferred = new AtomicInteger();

    Transport(List<URI> endpoints, String apiKey, Duration connectTimeout, Duration requestTimeout, int maxRounds) {
        this.endpoints = List.copyOf(endpoints);
        this.apiKey = apiKey;
        this.requestTimeout = requestTimeout;
        this.maxRounds = maxRounds;
        this.http = HttpClient.newBuilder()
                .connectTimeout(connectTimeout)
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    Response get(String path) throws InterruptedException {
        return send("GET", path, null, requestTimeout, Retry.IDEMPOTENT);
    }

    Response send(String method, String path, Object body, Duration timeout, Retry retry) throws InterruptedException {
        String payload = body == null ? null : json.writeValueAsString(body);
        IOException last = null;
        int n = endpoints.size();
        for (int round = 0; round < maxRounds; round++) {
            for (int i = 0; i < n; i++) {
                int index = Math.floorMod(preferred.get() + i, n);
                URI endpoint = endpoints.get(index);
                try {
                    HttpResponse<String> response = http.send(request(endpoint, method, path, payload, timeout),
                            HttpResponse.BodyHandlers.ofString());
                    int status = response.statusCode();
                    if (status == 502 || status == 503 || status == 504) {
                        log.debug("{} {} on {} answered {}; trying next endpoint", method, path, endpoint, status);
                        last = new IOException("HTTP " + status + " from " + endpoint);
                        continue;
                    }
                    preferred.set(index);
                    return new Response(status, response.body(), endpoint);
                } catch (ConnectException e) {
                    last = e;
                } catch (HttpTimeoutException e) {
                    if (retry == Retry.CONNECT_ONLY && !(e instanceof java.net.http.HttpConnectTimeoutException)) {
                        throw unreachable(method, path, e);
                    }
                    last = e;
                } catch (IOException e) {
                    if (retry == Retry.CONNECT_ONLY) {
                        throw unreachable(method, path, e);
                    }
                    last = e;
                }
                log.debug("{} {} failed on {}: {}; trying next endpoint", method, path, endpoint, last.toString());
            }
            if (round + 1 < maxRounds) {
                Thread.sleep(backoffMillis(round));
            }
        }
        throw unreachable(method, path, last);
    }

    private HttpRequest request(URI endpoint, String method, String path, String payload, Duration timeout) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(endpoint.resolve(path))
                .timeout(timeout)
                .header("Accept", "application/json");
        if (apiKey != null && !apiKey.isBlank()) {
            builder.header("X-API-Key", apiKey);
        }
        if (payload == null) {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            builder.header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofString(payload));
        }
        return builder.build();
    }

    <T> T read(Response response, Class<T> type) {
        if (!response.ok()) {
            throw error(response);
        }
        return json.readValue(response.body(), type);
    }

    <T> T read(Response response, TypeReference<T> type) {
        if (!response.ok()) {
            throw error(response);
        }
        return json.readValue(response.body(), type);
    }

    <T> T parse(Response response, Class<T> type) {
        return json.readValue(response.body(), type);
    }

    SemaphoreClientException error(Response response) {
        String detail = "HTTP " + response.status();
        String code = null;
        try {
            if (response.body() != null && !response.body().isBlank()) {
                Problem problem = json.readValue(response.body(), Problem.class);
                detail = problem.detail() != null ? problem.detail() : detail;
                code = problem.code();
            }
        } catch (JacksonException notProblemJson) {
            // keep the status-only message
        }
        return new SemaphoreClientException(detail, response.status(), code);
    }

    private static SemaphoreClientException unreachable(String method, String path, IOException cause) {
        return new SemaphoreClientException("semaphore service unreachable for " + method + " " + path, cause);
    }

    private static long backoffMillis(int round) {
        long cap = Math.min(2_000, 100L << Math.min(round, 5));
        return ThreadLocalRandom.current().nextLong(cap / 2, cap + 1);
    }
}
