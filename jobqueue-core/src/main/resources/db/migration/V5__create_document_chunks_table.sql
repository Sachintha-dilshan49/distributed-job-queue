CREATE TABLE document_chunks (
    id          BIGSERIAL PRIMARY KEY,
    job_id      BIGINT      NOT NULL,
    document_id VARCHAR(255) NOT NULL,
    chunk_index INT         NOT NULL,
    content     TEXT        NOT NULL,
    created_at  TIMESTAMP   NOT NULL DEFAULT NOW()
);