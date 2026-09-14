package lk.sachintha.jobqueue;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/admin")
public class AdminController {

    private final JdbcTemplate jdbcTemplate;

    public AdminController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/stats")
    public Map<String, Long> stats() {
        Map<String, Long> counts = new HashMap<>();
        counts.put("PENDING", 0L);
        counts.put("RUNNING", 0L);
        counts.put("SUCCEEDED", 0L);
        counts.put("DEAD", 0L);

        jdbcTemplate.query(
            "SELECT state, COUNT(*) FROM jobs GROUP BY state",
            rs -> {
                counts.put(rs.getString("state"), rs.getLong("count"));
            }
        );

        return counts;
    }

    @GetMapping("/workers")
    public List<Map<String, Object>> workers() {
        return jdbcTemplate.queryForList(
            "SELECT claimed_by, COUNT(*) as job_count FROM jobs WHERE state = 'RUNNING' GROUP BY claimed_by"
        );
    }

    @PostMapping("/workers/{claimed_by}/kill")
    public Map<String, String> kill(@PathVariable("claimed_by") String claimedBy) {
        jdbcTemplate.update(
            "UPDATE jobs SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE state = 'RUNNING' AND claimed_by = ?",
            claimedBy
        );
        return Map.of("status", "lease expired for " + claimedBy);
    }
}