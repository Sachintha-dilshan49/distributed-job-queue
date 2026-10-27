# Distributed Job Queue

[![CI](https://github.com/Sachintha-dilshan49/distributed-job-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Sachintha-dilshan49/distributed-job-queue/actions/workflows/ci.yml)
![Java](https://img.shields.io/badge/Java-25-orange)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1-6DB33F)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791)

A background job queue built from scratch with Java, Spring Boot and PostgreSQL.

You submit a job (for example "split this document into chunks"). Several worker processes pick jobs up and run them in parallel. If a job fails, it is retried later. If a worker crashes halfway through a job, another worker takes the job over. If a job keeps failing, it is parked in a dead-letter state instead of retrying forever.

The core is packaged as a **reusable Spring Boot library** (`jobqueue-core`): add the dependency, write one class for your job type, and the queue handles everything else.

> **Why build this?** In production you would use Celery, Sidekiq, SQS or Temporal. I built this to understand what those systems do underneath: how to hand out jobs safely when many workers ask at once, how to tell a crashed worker from a slow one, and how to make sure a retried job does not repeat its side effects.

<!-- Add a screenshot or GIF of the dashboard here: ![Dashboard](docs/dashboard.png) -->

---

## Highlights

| | |
|---|---|
| **Safe concurrent claiming** | Many workers poll the same table; no job is ever claimed twice (`FOR UPDATE SKIP LOCKED`). |
| **Crash recovery** | Each claimed job has a lease. If the worker dies, the lease expires and the job goes back to the queue. |
| **Long jobs are safe** | A heartbeat keeps renewing the lease while the job runs, so a slow-but-alive worker is not robbed of its job. |
| **Retries with backoff** | Failed jobs retry after 5s, 10s, 20s, 40s… plus random jitter. |
| **Dead-letter state** | After 5 failed attempts a job moves to `DEAD`, with the error message saved. |
| **No duplicate side effects** | Every job carries an idempotency key, so a job that runs twice still applies its effect once. |
| **Observability** | Prometheus metrics, plus a Next.js dashboard showing jobs and workers live. |
| **Tested under failure** | Integration and chaos tests against real PostgreSQL, run on every push by GitHub Actions. |

## Results

Measured locally against PostgreSQL 16.

| Test | Setup | Result |
|---|---|---|
| Claim safety | 3 workers, 1,000 jobs | **~1,900 duplicate claims** with a naive read-then-update → **0** with `SKIP LOCKED` |
| Worker crash | 3 worker containers, one killed with `docker kill` mid-job | Another worker finished the job (`attempts = 2`, different `claimed_by`) |
| Long job | 40s job with a 30s lease, no crash | Finished on the first attempt; heartbeats kept the lease alive |
| Chaos | 200 jobs, 4 workers, 15% of jobs abandoned mid-run | Every job finished (`SUCCEEDED` or `DEAD`), **no job lost, no effect applied twice**, ~4s |

---

## Architecture

There is no central coordinator. Workers talk to PostgreSQL directly, and the database is the only thing coordinating them. The same jar runs in two modes (Spring profiles):

- **API** (default profile): accepts jobs over HTTP, runs the **Reaper** that recovers expired leases, runs database migrations, exposes metrics.
- **Worker** (`worker` profile): only polls for jobs and runs them. No web server.

```
  POST /jobs                                Dashboard (Next.js)
      |                                            |
      v                                            v
 +---------------------------+      +-----------------------------+
 |  API  (default profile)   |      |  Worker  (worker profile) xN |
 |  - JobController          |      |  - claims a job every 10 ms  |
 |  - AdminController        |      |  - runs the matching handler |
 |  - Reaper (every 5 s)     |      |  - heartbeat while running   |
 |  - Flyway migrations      |      |  - retry / dead-letter       |
 |  - /actuator/prometheus   |      |                              |
 +-------------+-------------+      +--------------+---------------+
               |                                   |
               +----------------+------------------+
                                |
                                v
                     +----------------------+
                     |    PostgreSQL 16     |
                     |  jobs                |
                     |  completed_effects   |
                     +----------------------+
```

### Project structure

```
distributed-job-queue/
├── jobqueue-core/     The reusable library: Worker, Reaper, JobRepository,
│                      JobHandler interface, metrics, Flyway migrations,
│                      Spring Boot auto-configuration
├── jobqueue-demo/     Example app using the library: REST API, admin API,
│                      example handlers, and all the tests
├── dashboard/         Next.js dashboard (job counts, workers, crash button)
├── docker-compose.yml PostgreSQL + 1 API + 3 workers
└── .github/workflows/ CI: builds and tests on every push
```

---

## Job lifecycle

A job is always in exactly one of four states.

```
            POST /jobs
                |
                v
           +---------+  <-------- failed, attempts left
           | PENDING |            (wait: backoff + jitter)
           +---------+  <-------- lease expired, attempts left
                |                 (Reaper)
                | claimed by a worker
                v
           +---------+ ---- failed, no attempts left ----> +------+
           | RUNNING |                                     | DEAD |
           +---------+ ---- lease expired, no attempts --> +------+
                |
                | handler finished
                v
          +-----------+
          | SUCCEEDED |
          +-----------+
```

There is no `FAILED` state: a failed attempt either goes back to `PENDING` with a later `run_after` time, or to `DEAD` when all attempts are used.

---

## Key design decisions

**1. Claiming a job in one SQL statement.**
A worker claims a job with:

```sql
UPDATE jobs SET state = 'RUNNING', attempts = attempts + 1, claimed_by = ?, lease_expires_at = ...
WHERE id = (SELECT id FROM jobs
            WHERE state = 'PENDING' AND run_after <= now()
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1)
RETURNING *;
```

`SKIP LOCKED` makes each worker skip rows another worker has already locked instead of waiting for them. So N workers polling at the same moment get N different jobs. Doing it in two steps (read a job, then update it) fails under concurrency: that version produced ~1,900 duplicate claims in the 1,000-job test.

**2. Leases and heartbeats.**
From outside, a crashed worker and a very slow worker look the same: both go quiet. So every claim comes with a lease (default 30s). While the handler runs, a background timer renews the lease every 10s (a third of the lease). If the worker dies, renewals stop, the lease expires, and the Reaper returns the job to `PENDING`.
The trade-off: renewals push the deadline forward, so a worker that dies late in a job is noticed later. In exchange, a slow worker is never robbed of a job it is still doing.

**3. Every write checks ownership.**
Marking a job succeeded, renewing its lease, retrying it or killing it all require `state = 'RUNNING' AND claimed_by = <this worker>`. A worker that stalled and lost its lease cannot come back and overwrite the job now owned by someone else; its update simply affects 0 rows and it logs `LEASE LOST`.

**4. At-least-once delivery + idempotency keys.**
Because a job can be reclaimed from a slow worker, it can run more than once. Every job therefore carries an `idempotencyKey`. The handler records its side effect and the key **in one SQL statement**, using `INSERT ... ON CONFLICT DO NOTHING` on `completed_effects`. If the key is already there, the effect is skipped. Doing both in one statement means a crash can never leave the key recorded without the effect, or the other way round.

**5. Attempts are counted when a job is claimed, not when it fails.**
A job that crashes its worker never gets to report a failure. Counting at claim time means even these "poison" jobs eventually reach `DEAD` instead of crashing workers forever.

**6. Failures are visible, not silent.**
A job whose type has no handler is treated as a normal failure: it retries and ends in `DEAD` with the message `no handler registered for type: X` saved in `last_error`.

---

## Using the library

### 1. Add the dependency

`jobqueue-core` is published to GitHub Packages:

```xml
<repositories>
  <repository>
    <id>github</id>
    <url>https://maven.pkg.github.com/Sachintha-dilshan49/distributed-job-queue</url>
  </repository>
</repositories>

<dependency>
  <groupId>lk.sachintha</groupId>
  <artifactId>jobqueue-core</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

GitHub Packages requires a personal access token with `read:packages` in your Maven `settings.xml`.

The library registers itself through Spring Boot auto-configuration and runs its own Flyway migrations. Your application needs a PostgreSQL datasource and `@EnableScheduling`.

### 2. Write a handler

```java
@Component
public class EmailJobHandler implements JobHandler {

    private final JobRepository repository;

    public EmailJobHandler(JobRepository repository) {
        this.repository = repository;
    }

    @Override
    public String type() {
        return "email";
    }

    @Override
    public void handle(Job job) throws Exception {
        // do the work...
        repository.applyEffectOnce(job.id(), job.idempotencyKey());
    }
}
```

That is all. No registration and no changes to the queue. To signal failure, throw an exception: the queue handles retries, backoff and dead-lettering.

### Example handlers in the demo app

| Handler | `type` | What it does |
|---|---|---|
| `DocumentChunkingJobHandler` | `chunk` | Splits a document into 100-word chunks and stores them (a real workload) |
| `SlowJobHandler` | `test` | Sleeps for a set time, to test leases and heartbeats |
| `FailingJobHandler` | `fail` | Always throws, to test retries and dead-lettering |

---

## Running it

**Requirements:** Java 25, Docker, Node.js 20+ (only for the dashboard).

```bash
# 1. Build the jar (tests need the test database, see below)
./mvnw clean package -DskipTests

# 2. Build the image and start PostgreSQL + API + 3 workers
docker build -t jobqueue .
docker compose up -d
```

The API is at `http://localhost:8080`. PostgreSQL is published on host port **5433**.

### Submit a job

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
        "type": "chunk",
        "payload": "{\"documentId\": \"doc-1\", \"text\": \"some long text ...\"}",
        "idempotencyKey": "doc-1-v1"
      }'
```

Returns `201 Created` with the new job's `id`. `idempotencyKey` is required.

### Watch a crash recovery

```bash
docker compose up -d                       # 3 workers running
# submit a few "test" jobs, then while they run:
docker kill distributed-job-queue-worker-2 # container name may differ: see `docker ps`
docker compose logs -f worker              # another worker picks the job up
```

### Dashboard

```bash
cd dashboard
npm install
npm run dev     # http://localhost:3000
```

Shows job counts by state and which workers are running jobs, refreshed every 3 seconds. The **Kill** button expires a worker's leases, so you can watch the Reaper hand its jobs to other workers without stopping any container.

---

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/jobs` | Submit a job (`type`, `payload`, `idempotencyKey`) |
| `GET` | `/admin/stats` | Number of jobs in each state |
| `GET` | `/admin/workers` | Workers currently running jobs |
| `POST` | `/admin/workers/{claimedBy}/kill` | Expire a worker's leases (simulated crash) |
| `GET` | `/actuator/prometheus` | Metrics for Prometheus |

## Metrics

| Metric | Type | Meaning |
|---|---|---|
| `jobqueue.jobs.pending` | gauge | Jobs waiting. If this keeps rising, workers can't keep up. |
| `jobqueue.jobs.running` | gauge | Jobs currently being worked on |
| `jobqueue.jobs.dead.current` | gauge | Jobs in the dead-letter state |
| `jobqueue.jobs.claimed` / `succeeded` / `retried` / `dead` | counter | Work done since startup |
| `jobqueue.leases.reaped` | counter | Jobs recovered from expired leases |
| `jobqueue.leases.lost` | counter | Times a worker found its job had been taken over |

Prometheus renames these with underscores and a `_total` suffix for counters, e.g. `jobqueue_jobs_succeeded_total`.

## Configuration

| Property | Default | Meaning |
|---|---|---|
| `jobqueue.lease-seconds` | `30` | How long a claim lasts without a heartbeat |
| `jobqueue.backoff-base-seconds` | `5` | First retry delay; doubles each attempt |
| `jobqueue.reaper-interval-ms` | `5000` | How often the Reaper looks for expired leases |
| `max_attempts` (per job) | `5` | Attempts before a job goes to `DEAD` |

---

## Testing

```bash
# one-time: create the test database
docker compose exec postgres psql -U jobqueue -d jobqueue -c "CREATE DATABASE jobqueue_test"

./mvnw test
```

**9 JUnit 5 tests** against a real PostgreSQL database (no in-memory substitute, because the whole design depends on PostgreSQL's locking):

- 7 integration tests: two workers never claim the same job; jobs waiting for retry are skipped; only the owning worker can complete a job; the Reaper recovers expired leases; the Reaper dead-letters jobs out of attempts; only the owner can renew a lease; an effect is applied once per key
- 1 chaos test: 200 jobs, 4 concurrent workers, 15% of claims abandoned mid-flight, checking that nothing is lost or duplicated
- 1 application startup test

GitHub Actions runs the full suite on every push and pull request, using a PostgreSQL 16 service container.

---

## Problems I ran into

| Problem | Cause | Fix |
|---|---|---|
| Jobs that crashed their worker retried forever | The Reaper always reset expired jobs to `PENDING` | Reaper now sends jobs with no attempts left to `DEAD` |
| Chaos test flaky in CI (198/200 succeeded) | With 30% abandonment some jobs correctly ran out of attempts | Assert `succeeded + dead = total` instead of all succeeded |
| CI failed with exit code 126 | `mvnw` was not executable (Windows doesn't set the bit) | `git update-index --chmod=+x mvnw` |
| Testcontainers couldn't reach Docker on Windows | Docker Desktop named-pipe issue | Use a dedicated local test database; CI uses a service container |

## Known limitations

- **At-least-once, not exactly-once.** Side effects are protected by idempotency keys, but only if the handler uses them.
- **Workers expose no metrics.** Workers run without a web server, so only the API's metrics can be scraped. A production setup would give each worker a management port.
- **No authentication or rate limiting** on the API or admin endpoints.
- **`POST /jobs` has no matching `GET`.** Job status is visible through the admin endpoints and the database, not per job.
- **Polling, not push.** Workers poll every 10 ms; at large scale `LISTEN/NOTIFY` would reduce idle database load.
- **Prometheus and Grafana are not included** in `docker-compose.yml`; the app exposes metrics but nothing collects them yet.

## Roadmap

- [x] Concurrent claiming, leases, heartbeats, Reaper
- [x] Retries with backoff and jitter, dead-letter state
- [x] Idempotency keys
- [x] Pluggable handlers and a real workload (document chunking)
- [x] Prometheus metrics
- [x] Chaos test and CI
- [x] Admin API and Next.js dashboard
- [x] Split into a reusable library with auto-configuration, published to GitHub Packages
- [ ] Deploy a live demo
- [ ] Kafka-backed variant, to compare a log-based queue with a table-based one

## Tech stack

Java 25 · Spring Boot 4.1 · Spring JDBC (`JdbcTemplate`, no JPA: the SQL is the point) · PostgreSQL 16 · Flyway · Micrometer + Prometheus · JUnit 5 · Docker & Docker Compose · GitHub Actions · Next.js

## Author

**Sachintha Dilshan**: [GitHub](https://github.com/Sachintha-dilshan49) · [LinkedIn](https://www.linkedin.com/in/sachintha-dilshan-498a3a306)