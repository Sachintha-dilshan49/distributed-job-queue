CREATE TABLE jobs (
    id          BIGSERIAL PRIMARY KEY,
    type        VARCHAR(100) NOT NULL,
    payload     JSONB NOT NULL,
    state       VARCHAR(20) NOT NULL CHECK (state IN ('PENDING', 'RUNNING', 'SUCCEEDED')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);