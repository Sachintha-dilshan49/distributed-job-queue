package lk.sachintha.jobqueue;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootTest
public abstract class AbstractIntegrationTest {

    @Autowired
    protected JdbcTemplate jdbc;

    @Autowired
    protected JobRepository repository;

    @BeforeEach
    void clearTables() {
        jdbc.update("DELETE FROM fake_payments");
        jdbc.update("DELETE FROM completed_effects");
        jdbc.update("DELETE FROM jobs");
    }
}