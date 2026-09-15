package lk.sachintha.jobqueue;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class JobRepositoryTest extends AbstractIntegrationTest {

    @Test
    void twoWorkersNeverClaimTheSameJob() {
        repository.insert("test", "{}", "key-1");

        Job first = repository.claim("worker-a");
        Job second = repository.claim("worker-b");

        assertNotNull(first);
        assertNull(second);
        assertEquals("RUNNING", first.state());
        assertEquals(1, first.attempts());
    }

    @Test
    void claimSkipsJobsWaitingForRetry() {
        Long id = repository.insert("test", "{}", "key-2");
        jdbc.update("UPDATE jobs SET run_after = now() + interval '1 hour' WHERE id = ?", id);

        assertNull(repository.claim("worker-a"));
    }

    @Test
    void onlyTheOwningWorkerCanCompleteAJob() {
        repository.insert("test", "{}", "key-3");
        Job job = repository.claim("worker-a");

        assertEquals(0, repository.markSucceeded(job.id(), "worker-b"));
        assertEquals(1, repository.markSucceeded(job.id(), "worker-a"));
    }

    
        @Test
    void reaperReturnsExpiredLeaseToPending() {
        repository.insert("test", "{}", "key-4");
        Job job = repository.claim("worker-a");
        jdbc.update("UPDATE jobs SET lease_expires_at = now() - interval '1 minute' WHERE id = ?", job.id());

        assertEquals(1, repository.reapExpiredLeases());

        String state = jdbc.queryForObject("SELECT state FROM jobs WHERE id = ?", String.class, job.id());
        assertEquals("PENDING", state);
    }

    @Test
    void reaperKillsJobsThatRanOutOfAttempts() {
        repository.insert("test", "{}", "key-5");
        Job job = repository.claim("worker-a");
        jdbc.update("UPDATE jobs SET attempts = 5, lease_expires_at = now() - interval '1 minute' WHERE id = ?", job.id());

        repository.reapExpiredLeases();

        String state = jdbc.queryForObject("SELECT state FROM jobs WHERE id = ?", String.class, job.id());
        assertEquals("DEAD", state);
    }

    @Test
    void heartbeatOnlyWorksForTheOwningWorker() {
        repository.insert("test", "{}", "key-6");
        Job job = repository.claim("worker-a");

        assertEquals(0, repository.heartbeat(job.id(), "worker-b"));
        assertEquals(1, repository.heartbeat(job.id(), "worker-a"));
    }

    @Test
    void effectIsAppliedOnlyOncePerKey() {
        Long first = repository.insert("test", "{}", "same-key");
        Long second = repository.insert("test", "{}", "same-key");

        assertEquals(1, repository.applyEffectOnce(first, "same-key"));
        assertEquals(0, repository.applyEffectOnce(second, "same-key"));

        Integer payments = jdbc.queryForObject(
            "SELECT COUNT(*) FROM fake_payments WHERE idempotency_key = ?", Integer.class, "same-key");
        assertEquals(1, payments);
    }
}