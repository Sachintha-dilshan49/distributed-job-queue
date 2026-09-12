package lk.sachintha.jobqueue;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import java.time.OffsetDateTime;
import java.util.List;

@Repository
public class JobRepository {

    private final JdbcTemplate jdbc;
    private final long leaseSeconds;

    public JobRepository(JdbcTemplate jdbc,
                         @Value("${jobqueue.lease-seconds}") long leaseSeconds) {
        this.jdbc = jdbc;
        this.leaseSeconds = leaseSeconds;
    }

    public Long insert(String type, String payload, String idempotencyKey) {
        String sql = """
            INSERT INTO jobs (type, payload, state, idempotency_key)
            VALUES (?, ?::jsonb, 'PENDING', ?)
            RETURNING id
            """;

        return jdbc.queryForObject(sql, Long.class, type, payload, idempotencyKey);
    }

    public Job claim(String workerId) {
        String sql = """
            UPDATE jobs
            SET state = 'RUNNING',
                updated_at = now(),
                claimed_by = ?,
                lease_expires_at = now() + ? * interval '1 second',
                attempts = attempts + 1
            WHERE id = (
                SELECT id FROM jobs
                WHERE state = 'PENDING' AND run_after <= now()
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *
            """;

        List<Job> results = jdbc.query(sql, (rs, rowNum) -> new Job(
            rs.getLong("id"),
            rs.getString("type"),
            rs.getString("payload"),
            rs.getString("state"),
            rs.getObject("created_at", OffsetDateTime.class),
            rs.getObject("updated_at", OffsetDateTime.class),
            rs.getInt("attempts"),
            rs.getInt("max_attempts"),
            rs.getString("idempotency_key")
        ), workerId, leaseSeconds);

        return results.isEmpty() ? null : results.get(0);
    }

    public int markSucceeded(Long id, String workerId) {
        String sql = """
            UPDATE jobs
            SET state = 'SUCCEEDED', updated_at = now()
            WHERE id = ? AND state = 'RUNNING' AND claimed_by = ?
            """;

        return jdbc.update(sql, id, workerId);
    }

    public int reapExpiredLeases() {
        String sql = """
            UPDATE jobs
            SET state = CASE WHEN attempts >= max_attempts THEN 'DEAD' ELSE 'PENDING' END,
                last_error = 'lease expired (worker died or stalled)',
                claimed_by = NULL,
                lease_expires_at = NULL,
                updated_at = now()
            WHERE state = 'RUNNING'
              AND lease_expires_at < now()
            """;

        return jdbc.update(sql);
    }

    public int heartbeat(Long id, String workerId) {
        String sql = """
            UPDATE jobs
            SET lease_expires_at = now() + ? * interval '1 second',
                updated_at = now()
            WHERE id = ?
              AND state = 'RUNNING'
              AND claimed_by = ?
            """;

        return jdbc.update(sql, leaseSeconds, id, workerId);
    }

    public int scheduleRetry(Long id, String workerId, String error, long delaySeconds) {
        String sql = """
            UPDATE jobs
            SET state = 'PENDING',
                claimed_by = NULL,
                lease_expires_at = NULL,
                last_error = ?,
                run_after = now() + ? * interval '1 second',
                updated_at = now()
            WHERE id = ?
              AND state = 'RUNNING'
              AND claimed_by = ?
            """;

        return jdbc.update(sql, error, delaySeconds, id, workerId);
    }

    public int markDead(Long id, String workerId, String error) {
        String sql = """
            UPDATE jobs
            SET state = 'DEAD',
                claimed_by = NULL,
                lease_expires_at = NULL,
                last_error = ?,
                updated_at = now()
            WHERE id = ?
              AND state = 'RUNNING'
              AND claimed_by = ?
            """;

        return jdbc.update(sql, error, id, workerId);
    }

    public int applyEffectOnce(Long jobId, String idempotencyKey) {
        String sql = """
            WITH recorded AS (
                INSERT INTO completed_effects (idempotency_key)
                VALUES (?)
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING idempotency_key
            )
            INSERT INTO fake_payments (job_id, idempotency_key)
            SELECT ?, idempotency_key FROM recorded
            """;

        return jdbc.update(sql, idempotencyKey, jobId);
    }
}