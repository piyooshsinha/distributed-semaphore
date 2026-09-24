# Distributed Semaphore

A distributed **counting semaphore** service: at most *N* workers, across any number of processes and
machines, may hold a permit on a named resource at once. The rest queue in FIFO order and are woken the
moment a slot frees up.

Typical use: 30 log-processing workers all want to flush to one database. The database falls over
under that load even though every worker is healthy. A semaphore of capacity 5 means "only 5 of you
may flush at once; the rest wait."

Built with Java 25, Spring Boot 4 and PostgreSQL.

## How it works

```
 workers ──HTTP──▶  semaphore-service (N stateless replicas) ──JDBC──▶ PostgreSQL
                         ▲                                              │
                         └───────────── LISTEN/NOTIFY ◀─────────────────┘
```

- **Atomic acquire.** Each acquire is one transaction. It locks the semaphore's row, deletes expired
  permits, counts the current holders, and inserts a permit only if the count is below capacity. The
  row lock serialises acquires for one semaphore across every replica.
- **Defence in depth.** A database trigger rejects any insert that would take the holder count above
  capacity, even if application code has a bug.
- **Leases, not locks.** Permits expire after a TTL unless renewed, so a crashed worker cannot hold a
  slot forever.
- **Fencing tokens.** Every grant carries a number that strictly increases per semaphore. The protected
  resource can reject writes with a token lower than the highest it has seen. This stops a worker that
  paused past its lease from doing damage.
- **Fair queue.** Waiters are granted in arrival order, and a newcomer cannot jump ahead of the queue. A
  waiter that stops polling loses its place after a short lease, so it cannot block the queue.
- **Fast wake-ups.** Every change writes an audit event. A trigger publishes each one with
  `pg_notify`, so a waiter on any replica wakes within milliseconds. Re-polling every second covers
  any missed notification.
- **Idempotent.** Callers pass a `requestId`. Retrying with it returns the same permit or the same
  place in the queue. Releasing twice is harmless.
- **Clocks.** All timestamps come from the database clock, so replicas with skewed clocks still agree
  on expiry.

## Modules

| Module | What it contains |
|---|---|
| `semaphore-core` | Domain model and the `SemaphoreStore` interface. Plain Java, no framework. |
| `semaphore-postgres` | PostgreSQL store (plain JDBC), Flyway migrations, and the LISTEN/NOTIFY listener. |
| `semaphore-service` | Spring Boot HTTP API: acquire (waits up to 25s per call), leases, SSE event streams, metrics, node registry. |
| `semaphore-client` | Java SDK: fails over between replicas, keeps polling while queued, renews leases automatically. |
| `semaphore-simulator` | Simulated log-processor nodes: workers that contend for a semaphore and take operator commands. |
| `deploy/nuc` | Docker Compose stack: 2 replicas behind a Caddy load balancer, 3 simulator nodes (8 workers), and a deploy script. |

## HTTP API

Durations are in milliseconds. Errors use RFC 9457 problem details with a stable `code` field.
OpenAPI docs are served at `/swagger-ui.html`.

| Method & path | Purpose |
|---|---|
| `PUT /v1/semaphores/{name}` `{capacity, defaultTtlMs}` | Create the semaphore, or update its settings |
| `GET /v1/semaphores` · `GET /v1/semaphores/{name}` | List semaphores · current holders and wait queue |
| `DELETE /v1/semaphores/{name}` | Delete the semaphore |
| `POST /v1/semaphores/{name}/acquire` `{holderId, requestId, ttlMs?, waitTimeoutMs?}` | `200 GRANTED`, `202 QUEUED` (call again with the same `requestId`), `409 REJECTED` |
| `POST /v1/semaphores/{name}/permits/{id}/renew` `{ttlMs?}` | Extend a lease |
| `DELETE /v1/semaphores/{name}/permits/{id}` | Release a permit (idempotent) |
| `DELETE /v1/semaphores/{name}/waiters/{requestId}` | Leave the queue |
| `GET /v1/semaphores/{name}/events?after=&limit=` | Audit log, one page |
| `GET /v1/semaphores/{name}/events/stream` · `GET /v1/events/stream` | Live SSE: audit events plus state snapshots |

Example:

```bash
curl -X PUT localhost:8183/v1/semaphores/db-flush -H 'content-type: application/json' \
     -d '{"capacity":3,"defaultTtlMs":30000}'

curl -X POST localhost:8183/v1/semaphores/db-flush/acquire -H 'content-type: application/json' \
     -d '{"holderId":"worker-7","requestId":"8f1c…","waitTimeoutMs":60000}'
# {"status":"GRANTED","permit":{"permitId":"…","fencingToken":42,"expiresAt":"…"}}
```

## Java SDK

```java
try (SemaphoreClient client = SemaphoreClient.builder()
        .endpoints("http://sem-a:8183", "http://sem-b:8183")   // fails over between replicas
        .build()) {
    DistributedSemaphore flush = client.semaphore("db-flush");
    flush.createIfAbsent(5, Duration.ofSeconds(30));

    try (Lease lease = flush.acquire(AcquireOptions.holder("worker-7")
            .waitUpTo(Duration.ofMinutes(1))
            .onQueued(position -> log.info("queue position {}", position)))) {
        writeBatch(batch, lease.fencingToken());   // renewed in the background until closed
        if (!lease.isValid()) { /* lease lost: discard the result */ }
    }
}
```

- **Retries.** Every call the SDK retries is idempotent on the server. Acquire is keyed by a
  `requestId`, so resending after a timeout or fail-over returns the same permit or queue position.
- **Validity.** A lease counts as valid on the client's own monotonic clock, starting from when the
  grant request was *sent*. Clock skew with the server cannot make it look valid for longer than it is.
- **Lost leases.** `onLost` fires if the server no longer has the permit, or if renewal fails until
  the lease runs out.

## Worker nodes and fleet API

The simulator runs `LogProcessorWorker`s. Each one takes a batch of log lines, waits for a permit on
`log-flush`, flushes the batch while holding the permit, then releases it. Every node sends a
heartbeat once a second with its workers' states and recent tasks. The heartbeat reply carries any
operator commands queued for the node, so operators never need to reach nodes directly.

| Method & path | Purpose |
|---|---|
| `PUT /v1/nodes/{id}/heartbeat` | Node report in; unacknowledged commands out |
| `GET /v1/nodes` | All nodes, with an `online` flag, their workers and recent tasks |
| `POST /v1/nodes/{id}/commands` `{type, args}` | `SCALE_WORKERS{count}`, `PAUSE`, `RESUME`, `CRASH_WORKER{workerId}`, `SET_WORK_DURATION{minMs,maxMs}`, `KILL_NODE` |

`CRASH_WORKER` and `KILL_NODE` abandon permits without releasing them. They show lease expiry
reclaiming the slot, and fencing tokens moving past the dead holder.

## Build and run

Requirements: JDK 25+, and Docker (for the Testcontainers integration tests).

```bash
./mvnw verify                                   # unit + integration tests (starts PostgreSQL 18 in Docker)
docker run -d --name distsem-pg -e POSTGRES_USER=distsem -e POSTGRES_PASSWORD=distsem \
       -e POSTGRES_DB=distsem -p 5432:5432 postgres:18-alpine
./mvnw -pl semaphore-service -am spring-boot:run   # http://localhost:8183/swagger-ui.html
```

Configuration lives under `distsem.*` in [`application.yml`](semaphore-service/src/main/resources/application.yml):
lease limits, long-poll hold time, cleanup intervals, audit retention, and an optional API key.

## Operations

- **Health:** `/actuator/health` (liveness and readiness probes). If the LISTEN connection drops, the
  service reports `DEGRADED`; it keeps working, and waiters fall back to polling.
- **Metrics:** `/actuator/prometheus` exposes `distsem_acquire_total{semaphore,outcome}`,
  `distsem_acquire_duration_seconds` (a histogram), and release and renew counters.
- **Housekeeping:** every replica deletes expired permits and waiters each second (replicas skip
  semaphores another replica has locked), and deletes audit events older than 7 days.
- **Deploy:** `deploy/nuc/deploy.sh` builds the image on the Docker host and runs `docker compose up`
  with health-gated start-up.

## Roadmap

- [x] Core model and PostgreSQL store, including a concurrency stress test across replicas
- [x] HTTP service with long-poll, LISTEN/NOTIFY wake-ups, SSE and metrics
- [x] Java client SDK (auto-renew, fail-over between replicas) and simulated log-processor worker nodes
- [ ] React dashboard: nodes, workers, permits, queue and a live event timeline
- [ ] Chaos tests: kill replicas and workers, restart the database
