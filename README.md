# Distributed Job Queue

A job queue built from scratch on Java, Spring Boot and PostgreSQL. It tracks
work, not messages: every job has a state, an attempt count, a lease held by
whichever worker is running it, a retry schedule, and a recorded failure reason.
Several worker processes claim jobs from the same table concurrently without
ever claiming the same job twice. When a worker dies mid-job its lease expires
and another worker picks the job back up. The delivery guarantee is
at-least-once, so side effects are made idempotent with a key supplied by the
caller.

**Status:** phases 1-5, 7 and 9 of 12 complete.

## Why build this

In production you would use Celery, Sidekiq, SQS with a worker pool, or Temporal.
This is a learning and portfolio project, built to understand what those systems
do underneath: how a job is claimed safely under concurrency, how a dead worker
is told apart from a slow one, what backoff and dead-lettering actually look
like as rows in a table, and why at-least-once delivery pushes correctness onto
the handler.

## Stack

- Spring Boot 4.1.1, Java 25, Maven (`mvnw` wrapper, Maven 3.9.16)
- Spring JDBC with `JdbcTemplate` — no JPA, the SQL is the point
- Flyway for migrations
- PostgreSQL 16, published on host port **5433**
- Docker and Docker Compose
- GitHub Actions, JUnit 5

`pom.xml` still sets `<java.version>21</java.version>` while the project is built
and run on JDK 25.

## Architecture

There is no coordinator process and no claim endpoint. Workers talk to
PostgreSQL directly, and PostgreSQL is the only thing coordinating them. One jar
is built and run under two Spring profiles:

- **default profile** — the web API (`POST /jobs`) plus the Reaper
- **`worker` profile** — the polling worker only, with the web server disabled
  (`spring.main.web-application-type=none`) and Flyway off

The Reaper is annotated `@Profile("!worker")`, so it runs only in the API. That
keeps it to a single copy, and means killing a worker during a failure test does
not also kill the recovery mechanism.

```
  POST /jobs
      |
      v
 +--------------------------+     +-----------------------------+
 |  API  (default profile)  |     |  Worker (worker profile) xN |
 |  JobController           |     |  claim poll every 10ms      |
 |  Reaper every 5s         |     |  one heartbeat mid-job      |
 |  Flyway migrations       |     |  no web server, no Flyway   |
 +------------+-------------+     +--------------+--------------+
              |                                  |
              |        both go straight to       |
              +---------------+------------------+
                              |
                              v
                  +-----------------------+
                  |  PostgreSQL 16        |
                  |    jobs               |
                  |    completed_effects  |
                  |    fake_payments      |
                  +-----------------------+
```

## Job lifecycle

```
                      POST /jobs
                          |
                          v
                     +---------+
      +------------> | PENDING | <------------------+
      |              +---------+                    |
      |                   |                         |  scheduleRetry:
      |    claim: UPDATE ... WHERE id = (           |  attempts < max_attempts,
      |      SELECT ... FOR UPDATE SKIP LOCKED)     |  run_after = now() + backoff
      |    state = RUNNING, attempts += 1,          |
      |    claimed_by = <worker>,                   |
      |    lease_expires_at = now() + lease         |
      |                   |                         |
      |                   v                         |
      |              +---------+ ----- failure -----+
      |              | RUNNING |
      |              +---------+ ---- failure, attempts >= max_attempts ---+
      |               |      |                                             |
      |               |      | reaper: lease_expires_at < now()            |
      |               |      |   attempts <  max_attempts --> PENDING      |
      +---------------+      |   attempts >= max_attempts --> DEAD --------+
       (lease expired)       |                                             |
                             | effect applied once,                        v
                             | then markSucceeded                       +------+
                             |   (claimed_by must still match)          | DEAD |
                             v                                          +------+
                       +-----------+
                       | SUCCEEDED |
                       +-----------+
```

`jobs.state` is constrained to exactly these four values. There is no `FAILED`
state: a failed attempt goes back to `PENDING` with a future `run_after`, or
straight to `DEAD` when the attempts are used up.

## Key design decisions

**Claiming with `FOR UPDATE SKIP LOCKED`.** A worker claims a job in one
statement — an `UPDATE ... WHERE id = (SELECT id ... FOR UPDATE SKIP LOCKED
LIMIT 1) RETURNING *`. The subquery locks a single eligible row, and any
concurrent transaction skips over locked rows instead of blocking on them, so N
workers polling at the same moment get N different jobs. Read-then-update in two
steps does not survive concurrency: measured with 3 workers over 1000 jobs, the
two-step version produced about 1900 duplicate claims and the single-statement
version produced 0.

**Leases and heartbeats.** A claim stamps `claimed_by` (hostname + PID) and
`lease_expires_at = now() + jobqueue.lease-seconds`. The Reaper scans every 5
seconds for `RUNNING` rows whose lease has passed and returns them. From outside
the database a crashed worker and a merely slow worker look identical — both
just stop reporting, and nothing can tell them apart, so the design has to
tolerate being wrong about which one it is. The live worker keeps saying it is
alive by extending its own lease mid-job — with the current fake workload that
is a single heartbeat at the halfway point, not a repeating timer — and the
extension is conditional on `claimed_by` still matching, so it fails the moment
the job has been taken away. Being wrong in the other direction is survivable
too: a job reclaimed from a worker that was only slow simply runs again, and the
idempotency key stops the effect happening twice.

Extending the lease has a price, and it is paid on the recovery path: every
extension moves the deadline the Reaper is waiting for, so the later in a job a
worker dies, the longer that job sits `RUNNING` before anyone notices. That is
the trade being made — a slow worker is not robbed of its job, and in exchange a
dead one is found later.

**Ownership checks on every write.** `markSucceeded`, `heartbeat`,
`scheduleRetry` and `markDead` all require `state = 'RUNNING' AND claimed_by =
?`. A worker that stalled long enough to lose its lease cannot come back and
mark a job done that someone else is now running; the update affects 0 rows and
the worker logs `LEASE LOST`.

**Backoff with jitter.** `attempts` is incremented at claim time, not at failure
time, so a job that kills its worker outright is still counted. On failure the
delay is `jobqueue.backoff-base-seconds * 2^(attempts-1)` plus a random 0-3
seconds of jitter, written into `run_after`; the claim query only considers rows
with `run_after <= now()`. Backoff stops a failing dependency from being
hammered. Jitter is not optional decoration: without it, a batch of jobs that
failed together at the same instant retries in lockstep forever, so the
dependency that just fell over gets the whole batch again at exactly the same
moment, and the retries themselves become the load.

**Dead letters and poison jobs.** `max_attempts` defaults to 5. A job that fails
its last attempt goes to `DEAD` with `last_error` kept for inspection. The
Reaper applies the same rule with `CASE WHEN attempts >= max_attempts THEN
'DEAD' ELSE 'PENDING' END`, which covers the nastier case: a poison job that
crashes its worker outright never gets the chance to report its own failure, so
without that clause the Reaper would resurrect it forever and it would keep
killing workers.

**At-least-once plus idempotency.** Recovery means a job can run twice. That is
inherent, not a bug to fix: the worker and the database are separate processes,
so between "the work happened" and "the database knows the work happened" there
is a gap, and a crash in that gap is always possible. Recording the key first
just swaps the failure for one where the key is recorded and the work never
happened. Exactly-once across that boundary is not achievable; what is
achievable is at-least-once execution with effects that do not care how many
times they run.

So `idempotencyKey` is required on `POST /jobs` (a missing or blank key gets a
400), and the effect is applied through a single statement that both records the
key and does the work:

```sql
WITH recorded AS (
    INSERT INTO completed_effects (idempotency_key)
    VALUES (?)
    ON CONFLICT (idempotency_key) DO NOTHING
    RETURNING idempotency_key
)
INSERT INTO fake_payments (job_id, idempotency_key)
SELECT ?, idempotency_key FROM recorded
```

`completed_effects.idempotency_key` is the primary key, so the first writer wins
the `ON CONFLICT` race and the CTE returns no rows to anyone else — the effect
row is inserted exactly once. Because both halves are one statement, a crash
cannot record the key without also applying the effect. `fake_payments` stands
in for the real side effect (charging a card, sending an email). A worker that
finds the key already recorded sees 0 rows affected and logs `SKIPPED`.

The key is load-bearing, not a second line of defence. `applyEffectOnce` runs
before `markSucceeded` and does not check `claimed_by`, so a worker whose lease
has already been reaped can still write the effect — the uniqueness of the key
is the only thing stopping that from becoming a duplicate. Gating the effect on
`claimed_by` would not close the hole either, because the lease can expire in
the moment between the check passing and the effect landing; that check would
narrow the window without removing it. This is the deliberate shape of
at-least-once delivery rather than a defect: the worker is allowed to be wrong
about whether it still owns the job, and correctness is carried by the effect
being safe to attempt more than once.

## Configuration

All four properties live in
[application.properties](src/main/resources/application.properties) and can be
overridden on the command line or through the environment.

| Property | Default | What it affects |
|---|---|---|
| `jobqueue.lease-seconds` | `30` | How long a claim is valid. Used both when a job is claimed and when a heartbeat renews it. Shorter means faster recovery from a dead worker and more risk of reclaiming a slow one |
| `jobqueue.work-duration-ms` | `40000` | How long the fake workload sleeps. The worker sleeps half of it, sends one heartbeat, then sleeps the other half |
| `jobqueue.backoff-base-seconds` | `5` | The base of `base * 2^(attempts-1)` for the retry delay, before jitter |
| `jobqueue.reaper-interval-ms` | `5000` | The `fixedDelay` between Reaper sweeps for expired leases |

Not configurable: the worker's 10 ms claim poll interval, the 0-3 second jitter
range, and `max_attempts`, which is a column default of 5 on the `jobs` table.

Database settings come from the usual Spring properties, so `SPRING_DATASOURCE_URL`
is what Compose overrides.

## Running it

### With Docker Compose

Compose refers to the image by name (`image: jobqueue`) and does not build it,
and the Dockerfile copies an already-built jar, so the jar and the image have to
exist first:

```powershell
.\mvnw clean package -DskipTests
docker build -t jobqueue .
docker compose up -d
```

That brings up PostgreSQL 16 (with a healthcheck, published on host port 5433),
the API on port 8080, and 3 worker replicas. The API and the workers wait for
the database to report healthy. Rebuild the image after any code change —
Compose runs the copy baked into the image, not your working tree.

```powershell
docker compose logs -f worker
docker compose down
```

### Manually

Start just the database:

```powershell
docker compose up -d postgres
```

Build the jar:

```powershell
.\mvnw clean package -DskipTests
```

Run the API — default profile, web API on port 8080, plus the Reaper. Flyway
runs the migrations here:

```powershell
java -jar target\jobqueue-0.0.1-SNAPSHOT.jar
```

Run a worker in another terminal, and repeat for as many workers as you want:

```powershell
java -jar target\jobqueue-0.0.1-SNAPSHOT.jar --spring.profiles.active=worker
```

Flyway is disabled in the worker profile, so start the API at least once against
a fresh database before running workers on their own.

### Submitting a job

```powershell
$body = @{
    type           = "print"
    payload        = '{"to":"alice","amount":100}'
    idempotencyKey = "pay-001"
} | ConvertTo-Json

Invoke-RestMethod -Uri http://localhost:8080/jobs -Method Post -ContentType "application/json" -Body $body
```

`payload` is sent as a **JSON string**, not a nested JSON object — it is bound
straight into a `jsonb` column as text. `type` is `"fail"` to exercise the retry
path and anything else for the normal path. The response is `201` with
`{"id": <n>}`. A missing or blank `idempotencyKey` gets a `400` with no body.

## The recovery demo, in Docker

With `docker compose up -d` running the API and 3 workers:

1. Submit a job with the request above.
2. Find which worker claimed it:

   ```powershell
   docker compose logs worker | Select-String "Started job"
   ```

   The line carries the job id, and `docker compose logs` prefixes each line
   with the container that produced it (`jobqueue-worker-1`, `-2`, `-3`).
3. While the job is still running — the default workload lasts 40 seconds — kill
   that container outright, with no chance to clean up:

   ```powershell
   docker kill jobqueue-worker-2
   ```

   The container exits with code 137.
4. Watch the API log. Within roughly the lease length it prints
   `REAPER: reaped 1 expired leases` and the job goes back to `PENDING`.
5. One of the surviving workers claims it and finishes it. The row ends up with
   `attempts = 2` and a different `claimed_by` from the first run.

Each worker prints `Worker ID: <hostname>-<pid>` at startup. Inside a container
the PID is always 1, so it is the container hostname that keeps the id unique.

## Testing

```powershell
.\mvnw test
```

9 JUnit 5 tests: a context-load smoke test, 7 integration tests over
`JobRepository` covering claim exclusivity, `run_after`, ownership checks on
completion and heartbeat, the Reaper's two outcomes and per-key idempotency, and
one chaos test.

These are integration tests against a real PostgreSQL — there is no in-memory
substitute, because the whole design is SQL. They use a separate `jobqueue_test`
database so a test run cannot wipe development data; each test clears
`fake_payments`, `completed_effects` and `jobs` before it runs, and Flyway
creates the schema. Create the database once:

```powershell
docker compose exec postgres psql -U jobqueue -d jobqueue -c "CREATE DATABASE jobqueue_test OWNER jobqueue"
```

The connection defaults to `jdbc:postgresql://localhost:5433/jobqueue_test` and
is overridden by `TEST_DB_URL`, `TEST_DB_USER` and `TEST_DB_PASSWORD` — see
[src/test/resources/application.properties](src/test/resources/application.properties).

CI runs the same tests on every push and pull request.
[.github/workflows/ci.yml](.github/workflows/ci.yml) starts a `postgres:16`
service container holding `jobqueue_test`, points those three environment
variables at it, and runs `./mvnw --batch-mode clean verify` on Temurin 25. On
failure it prints the surefire reports.

## Test results

Timings were measured by hand against a local PostgreSQL and run from
`created_at` to the `updated_at` of the final state. The chaos figures come from
the automated run.

| Test | Setup | Result |
|---|---|---|
| Claim safety | 3 workers, 1000 jobs | ~1900 duplicate claims before `SKIP LOCKED`, 0 after |
| Kill, no heartbeats | 20s job, worker hard-killed mid-job | A second worker finished it; created to finished 54.5s (30s lease + reaper + 20s of work) |
| Heartbeats, no kill | 40s job, 30s lease, no kill | Finished in 40.2s, never reclaimed; `lease_expires_at` observed moving forward when the halfway heartbeat fired |
| Kill with heartbeats | 40s job, 30s lease, hard-killed ~23s in — after the single heartbeat at ~20s | Recovered by a different worker, `attempts = 2`; created to finished 92.2s. The heartbeat at 20s had pushed the lease from 30s out to ~50s, so the Reaper could not reclaim the job until 50s; recovery then took a further 40s for the full re-run. Killing later means slower recovery, which is the cost of tolerating slow-but-alive workers |
| Retry and dead letter | job type `fail` | Observed delays 7s, 11s, 22s, 42s, then `DEAD` on attempt 5 with `last_error` preserved |
| Poison job | `RUNNING` row, expired lease, `attempts = 5` | Reaper set it `DEAD` with `last_error` = `lease expired (worker died or stalled)`. The same row with `attempts = 2` went back to `PENDING` |
| Idempotency | Two jobs sharing one key, two workers | Both reached `SUCCEEDED`, exactly 1 row in `fake_payments`, the second worker logged `SKIPPED` |
| Docker recovery | 3 worker containers, one `docker kill`ed mid-job | The killed container exited 137, another worker finished the job, `attempts = 2`, different `claimed_by` |
| Chaos | 200 jobs, 4 concurrent workers, 15% of claims abandoned mid-flight (no completion, no heartbeat) | Every job reached a final state, payments equalled succeeded jobs, no key paid twice. Runs in about 4 seconds |

The heartbeat test is the one worth reading twice: without heartbeats, a healthy
40-second job would have been reclaimed at the 30-second mark.

## Problems hit and how they were solved

| Problem | Cause | Resolution |
|---|---|---|
| The web API was also processing jobs | `Worker` had no `@Profile`, so it ran under every profile | `@Profile("worker")` on `Worker` |
| A worker that had lost its lease could still mark the job done | Completion only checked `state = 'RUNNING'` | Also check `claimed_by = ?`, on every state-changing update |
| Healthy but slow jobs were reclaimed at the lease deadline | A fixed lease with no renewal | Heartbeat mid-job, conditional on still owning the job |
| Poison jobs that crash the worker would retry forever | The Reaper always reset an expired lease to `PENDING` | `CASE WHEN attempts >= max_attempts THEN 'DEAD' ELSE 'PENDING' END` |
| Adding `NOT NULL idempotency_key` failed on existing rows | Thousands of rows had no value | Add the column nullable, backfill `'legacy-' \|\| id`, then `SET NOT NULL` |
| All workers in Docker had PID 1 | Each container's app is its first process | The worker id is `hostname-pid`, so the container hostname keeps it unique |
| Build failed after a signature change | A stale duplicate `JobController.java` under `src/test` | Deleted it |
| Testcontainers could not reach Docker on Windows | Docker Desktop returned an empty 400 on the named pipe | Tests use a local `jobqueue_test` database instead; CI uses a Postgres service container |
| CI failed with exit code 126 | `mvnw` was not executable in Git — Windows does not set the bit | `git update-index --chmod=+x mvnw` |
| The chaos test was flaky in CI (198/200 succeeded) | At 30% abandonment some jobs reached `max_attempts` and went `DEAD`, which is correct behaviour rather than a failure | Lowered abandonment to 15% and assert `succeeded + dead = total` instead |
| Tested stale code several times | Windows locks the jar while it is running, and Docker runs the copy baked into the image | Stop every `java` process and check the jar's timestamp; rebuild the image after any code change |

## Known limitations

- The Reaper is a single instance living inside the API. If the API is down,
  nothing recovers expired leases.
- `payload` has to be sent as a JSON string rather than a nested JSON object.
- The workload is fake: two sleeps with a heartbeat between them, and a
  `fake_payments` insert standing in for a real side effect.
- No metrics and no dashboard. Queue depth, claim rate and failure rate can only
  be seen by querying the table.
- `pom.xml` still targets Java 21 as its release level while the project runs on
  25.
- The API is write-only: `POST /jobs` and nothing else. There is no endpoint for
  reading a job's state back.
- Nothing is authenticated or rate limited, and the Compose database has no
  volume, so its data goes when the container does.

## Roadmap

The phases not yet started:

- **Phase 6** — a real workload instead of sleeps
- **Phase 8** — a dashboard with a chaos panel for killing workers from the browser
- **Phase 10** — Prometheus and Grafana for queue depth, claim rate and failure rate
- **Phase 11** — deploy it somewhere
- **Phase 12** (optional) — a Kafka-backed variant, to compare a log against a table
