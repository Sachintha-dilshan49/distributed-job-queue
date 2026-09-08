package lk.sachintha.jobqueue;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

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
}