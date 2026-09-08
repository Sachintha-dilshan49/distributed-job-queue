# Distributed Job Queue

A job queue built from scratch to understand how systems like Celery and Sidekiq
work underneath. A coordinator holds work; multiple worker processes claim jobs
and run them in parallel. If a worker dies mid-job, another picks it up.

**Status:** Phase 1 — walking skeleton.

## Stack

Java 25, Spring Boot 4, PostgreSQL 16, Flyway, Docker

## Running locally

Start the database:

```bash
docker compose up -d
```

Start the application:

```bash
./mvnw spring-boot:run
```

Flyway applies migrations on startup. The API runs on port 8080.

## Why build this when Celery exists

You should use Celery. This exists to understand what it is doing underneath —
job claiming under concurrency, lease-based failure recovery, and idempotent
handlers.

## Engineering log

Problems hit during the build and how they were solved.

| Date | Problem | Cause | Resolution |
|---|---|---|---|
| 2026-09-08 | Flyway found the migration file but skipped it | Windows filenames are case-insensitive, so renaming `v1__` to `V1__` left a stale lowercase copy in `target/`. Flyway's naming convention is case-sensitive. | `mvnw clean` to force a fresh copy. |