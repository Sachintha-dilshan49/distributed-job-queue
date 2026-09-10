ALTER TABLE jobs
    ADD COLUMN claimed_by VARCHAR(255),
    ADD COLUMN lease_expires_at TIMESTAMPTZ;