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
}