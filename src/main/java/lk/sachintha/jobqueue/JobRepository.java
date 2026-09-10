package lk.sachintha.jobqueue;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import java.time.OffsetDateTime;
import java.util.List;

@Repository
public class JobRepository {

    private final JdbcTemplate jdbc;

    public JobRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Long insert(String type, String payload) {
        String sql = """
            INSERT INTO jobs (type, payload, state)
            VALUES (?, ?::jsonb, 'PENDING')
            RETURNING id
            """;

        return jdbc.queryForObject(sql, Long.class, type, payload);
    }

    public Job claim(String workerId) {
        String sql = """
            UPDATE jobs
            SET state = 'RUNNING',
                updated_at = now(),
                claimed_by = ?,
                lease_expires_at = now() + interval '30 seconds'
            WHERE id = (
                SELECT id FROM jobs
                WHERE state = 'PENDING'
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
            rs.getObject("updated_at", OffsetDateTime.class)
        ), workerId);

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
            SET state = 'PENDING',
                claimed_by = NULL,
                lease_expires_at = NULL,
                updated_at = now()
            WHERE state = 'RUNNING'
              AND lease_expires_at < now()
            """;

        return jdbc.update(sql);
    }
}