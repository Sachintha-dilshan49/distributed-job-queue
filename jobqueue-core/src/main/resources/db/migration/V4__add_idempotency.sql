-- 1. Add the key to jobs, filling in old rows before making it required
ALTER TABLE jobs ADD COLUMN idempotency_key TEXT;

UPDATE jobs SET idempotency_key = 'legacy-' || id;

ALTER TABLE jobs ALTER COLUMN idempotency_key SET NOT NULL;

-- 2. Keys whose real-world effect has already happened
CREATE TABLE completed_effects (
    idempotency_key TEXT PRIMARY KEY,
    completed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 3. Fake side effect: one row per time the work actually runs
CREATE TABLE fake_payments (
    id              BIGSERIAL PRIMARY KEY,
    job_id          BIGINT NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);