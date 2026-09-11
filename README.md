# Distributed Job Queue

A job queue built from scratch on Java, Spring Boot and PostgreSQL. It tracks
work rather than messages: every job has a state, an attempt count, a lease held
by whichever worker is running it, a retry schedule, and a recorded failure
reason. Multiple worker processes claim jobs from the same table concurrently
without ever claiming the same job twice. If a worker dies mid-job, its lease
expires and another worker picks the job back up. The delivery guarantee is
at-least-once, so side effects are made idempotent with a key supplied by the
caller.

**Status:** phases 1-5 of 12 complete.

## Why build this

You should use Celery, Sidekiq, SQS with a worker pool, or Temporal. This is a
learning and portfolio project, built to understand what those systems are doing
underneath — how a job is claimed safely under concurrency, how a dead worker is
told apart from a slow one, what backoff and dead-lettering actually look like
in a table, and why at-least-once delivery pushes correctness onto the handler.

## Stack

- Spring Boot 4.1.1, Maven (`mvnw` wrapper, Maven 3.9.16)
- `pom.xml` targets Java 21; built and run locally on JDK 25
- Spring JDBC with `JdbcTemplate` — no JPA, the SQL is the point
- Flyway for migrations
- PostgreSQL 16 in Docker, published on host port **5433**

## Architecture

There is no coordinator process and no claim endpoint. Workers talk to
PostgreSQL directly, and PostgreSQL is the only thing coordinating them. One jar
is built and run under two Spring profiles:

- **default profile** — the web API (`POST /jobs`) plus the Reaper
- **`worker` profile** — the polling worker only, with the web server disabled
  (`spring.main.web-application-type=none`) and Flyway off

The Reaper is annotated `@Profile("!worker")` so it runs only in the API. That
keeps it to a single copy and means killing a worker during a failure test does
not also kill the recovery mechanism.

```
  POST /jobs
      |
      v
 +----------------------+        +--------------------------+
 |  API  (default)      |        |  Worker  (--worker) xN   |
 |  JobController       |        |  poll every 10ms         |
 |  Reaper every 5s     |        |  heartbeat every 10s     |
 +----------+-----------+        +-------------+------------+
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
      |    claimed_by = <worker>, lease = now()+30s |
      |                   |                         |
      |                   v                         |
      |              +---------+ ----- failure -----+
      |              | RUNNING |
      |              +---------+ ----- failure, attempts >= max_attempts ---+
      |               |      |                                              |
      |               |      | reaper: lease_expires_at < now()             |
      |               |      |   attempts <  max_attempts --> PENDING       |
      +---------------+      |   attempts >= max_attempts --> DEAD ---------+
       (lease expired)       |                                              |
                             | effect applied once,                         v
                             | then markSucceeded                        +------+
                             |   (claimed_by must still match)           | DEAD |
                             v                                           +------+
                       +-----------+
                       | SUCCEEDED |
                       +-----------+
```

`jobs.state` is constrained to exactly these four values. There is no `FAILED`
state — a failed attempt goes back to `PENDING` with a future `run_after`, or
straight to `DEAD` when the attempts are used up.

## Key design decisions

**Claiming with `FOR UPDATE SKIP LOCKED`.** A worker claims a job in one
statement: an `UPDATE ... WHERE id = (SELECT id ... FOR UPDATE SKIP LOCKED LIMIT
1) RETURNING *`. The subquery locks a single eligible row and any concurrent
transaction skips over locked rows rather than blocking on them, so N workers
polling at the same moment get N different jobs. Read-then-update in two steps
does not survive concurrency: measured with 3 workers over 1000 jobs, the
two-step version produced about 1900 duplicate claims and the single-statement
version produced 0.

**Leases and heartbeats.** A claim stamps `claimed_by` (hostname + PID) and
`lease_expires_at = now() + 30 seconds`. The Reaper scans every 5 seconds for
`RUNNING` rows whose lease has passed and returns them. From outside the
database, a worker that has crashed and a worker that is merely slow look
identical — both just stop reporting. The only way to tell them apart is to make
the live one keep saying so, which is what the heartbeat does: the worker
extends its own lease every 10 seconds, and the extension is conditional on
`claimed_by` still matching, so it fails the moment the job has been taken away.

**Ownership checks on every write.** `markSucceeded`, `heartbeat`,
`scheduleRetry` and `markDead` all require `state = 'RUNNING' AND claimed_by =
?`. A worker that was stalled long enough to lose its lease cannot come back and
mark a job done that someone else is now running; the update simply affects 0
rows and the worker logs `LEASE LOST`.

**Backoff with jitter.** `attempts` is incremented at claim time, not at failure
time, so a job that kills its worker outright is still counted. On failure the
delay is `5s * 2^(attempts-1)` plus a random 0-3s of jitter, written into
`run_after`; the claim query only considers rows with `run_after <= now()`.
Backoff stops a failing dependency from being hammered, and jitter stops a batch
of jobs that failed together from retrying in lockstep.

**Dead letters.** `max_attempts` defaults to 5. A job that fails its last attempt
goes to `DEAD` with `last_error` kept for inspection. The Reaper applies the same
rule with `CASE WHEN attempts >= max_attempts THEN 'DEAD' ELSE 'PENDING' END`,
which covers the nastier case: a poison job that crashes its worker never gets to
report its own failure, so without that clause it would be resurrected forever.

**At-least-once plus idempotency.** Recovery means a job can run twice — that is
inherent, not a bug to be fixed. So `idempotencyKey` is required on `POST /jobs`
(a missing or blank key gets a 400), and the effect is applied through one
statement that both records the key and does the work:

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
row is inserted exactly once. `fake_payments` stands in for the real side effect
(charging a card, sending an email). A worker that finds the key already
recorded sees 0 rows affected and logs `SKIPPED`.

## Running it locally (Windows)

Start PostgreSQL — see [docker-compose.yml](docker-compose.yml) for the database
name, user and password:

```powershell
docker compose up -d
```

Build the jar:

```powershell
.\mvnw clean package -DskipTests
```

Run the API (default profile — web API on port 8080, plus the Reaper):

```powershell
java -jar target\jobqueue-0.0.1-SNAPSHOT.jar
```

Run a worker in another terminal, and repeat for as many workers as you want:

```powershell
java -jar target\jobqueue-0.0.1-SNAPSHOT.jar --spring.profiles.active=worker
```

Flyway runs the migrations when the API starts; it is disabled in the worker
profile.

Submit a job:

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
path, and anything else for the normal path. The response is `201` with
`{"id": <n>}`.

The fake workload is four 10-second sleeps with a heartbeat after each, so a
normal job takes about 40 seconds.

## The recovery demo

1. Start the API and one worker.
2. `POST /jobs` and watch the worker log `Started job: id=...`.
3. While it is still running, kill that worker process:
   `taskkill /F /PID <pid>`.
4. Start a second worker.
5. Within about 30 seconds the API logs `REAPER: reaped 1 expired leases`, the
   job returns to `PENDING`, and the second worker claims and finishes it.

On Windows the `java.exe` on the default path is a launcher shim, so the PID you
find by looking for "java" is not necessarily the JVM running the worker. Kill
the PID printed in the worker's own `Worker ID: <hostname>-<pid>` log line —
that one comes from `ProcessHandle.current().pid()` inside the JVM itself.

Start the second worker only *after* the kill. If it is already running it will
have claimed a different job, and if you kill the first worker after its job has
finished there is nothing left to recover.

## Test results

Measured by hand against a local PostgreSQL; timings run from `created_at` to
the `updated_at` of the final state.

| Test | Setup | Result |
|---|---|---|
| Claim safety | 3 workers, 1000 jobs | ~1900 duplicate claims before `SKIP LOCKED`, 0 after |
| Kill, no heartbeats | 20s job, worker hard-killed mid-job | Second worker finished it; created to finished 54.5s (30s lease + reaper + 20s of work) |
| Heartbeats, no kill | 40s job, 30s lease | Finished in 40.2s, never reclaimed; `lease_expires_at` moved forward during the run |
| Kill with heartbeats | 40s job, killed ~15s in | Recovered by a second worker; created to finished 82.5s |
| Retry and dead letter | job type `fail` | Observed delays 7s, 11s, 22s, 42s, then `DEAD` after attempt 5 with `last_error` preserved |
| Poison job | `RUNNING` row, expired lease, `attempts = 5` | Reaper set it `DEAD` with `last_error` = `lease expired (worker died or stalled)`. The same row with `attempts = 2` went back to `PENDING` |
| Idempotency | Two jobs with the same key, two workers | Both reached `SUCCEEDED`, exactly 1 row in `fake_payments`, the second worker logged `SKIPPED` |

The heartbeat test is the one worth reading twice: without heartbeats, a healthy
40-second job would have been stolen at the 30-second mark.

## Problems hit and how they were solved

| Problem | Cause | Resolution |
|---|---|---|
| Flyway found the migration file but skipped it | Windows filenames are case-insensitive, so renaming `v1__` to `V1__` left a stale lowercase copy in `target/`; Flyway's naming convention is case-sensitive | `mvnw clean` to force a fresh copy |
| The web API was also processing jobs | `Worker` had no `@Profile`, so it ran under every profile — the phase 2 numbers were probably measured with 4 workers, not 3 | `@Profile("worker")` on `Worker` |
| A zombie worker could mark a job done after it had been reclaimed | Completion only checked `state = 'RUNNING'` | Check `claimed_by = ?` as well, on every state-changing update |
| Healthy but slow jobs were stolen at 30 seconds | Fixed lease with no renewal | Heartbeat every 10 seconds, conditional on still owning the job |
| Kill test showed no recovery, twice | First time the idle worker was killed; second time the kill landed after the job had already finished | Start the second worker only after the kill, and kill while the job is still running |
| Jobs finished too fast to kill mid-job | The fake job did no work | Simulate work with sleeps |
| A poison job that crashes its worker would retry forever | The reaper always reset expired leases to `PENDING` | `CASE WHEN attempts >= max_attempts THEN 'DEAD' ELSE 'PENDING' END` |
| Adding `NOT NULL idempotency_key` failed on existing rows | 4000+ rows had no value | Add the column nullable, backfill `'legacy-' \|\| id`, then `SET NOT NULL` |
| Build failed after changing `insert()` | A stray old copy of `JobController.java` under `src/test` | Deleted it |
| `Worker.java` was not picked up | Saved under `lk/sachintha` instead of `lk/sachintha/jobqueue` | Moved it into the right package directory |
| Tested stale code several times | Windows locks the jar while it is running, so the build either fails or silently leaves the old jar in place | Stop every `java` process and check the jar's timestamp before testing |

## Known limitations

- The lease is a fixed 30 seconds, hardcoded in the claim and heartbeat SQL. It
  is not derived from how long the job type actually takes.
- The Reaper is a single instance living inside the API. If the API is down,
  nothing recovers expired leases.
- `payload` has to be sent as a JSON string rather than a nested JSON object.
- The workload is fake — four sleeps, and a `fake_payments` insert standing in
  for a real side effect.
- The API is write-only: `POST /jobs` and nothing else. There is no way to read a
  job's state back except by querying the database.
- No automated tests beyond a `contextLoads` smoke test, and no CI. Every result
  in the table above was produced by hand.
- Nothing is authenticated or rate limited, and the Compose database has no
  volume, so its data is gone once the container is removed.

## Roadmap

Phases 6-12, none of them started:

6. A real workload instead of sleeps
7. Docker Compose for the API and the workers, not just the database
8. A dashboard with a chaos panel for killing workers from the browser
9. JUnit and Testcontainers, plus CI
10. Prometheus and Grafana for queue depth, claim rate and failure rate
11. Deploy it somewhere
12. Optional: a Kafka-backed variant, to compare a log against a table
