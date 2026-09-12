package lk.sachintha.jobqueue;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ChaosTest extends AbstractIntegrationTest {

    private static final int JOB_COUNT = 200;
    private static final int WORKER_COUNT = 4;

    @Test
    void noJobIsLostAndNoEffectIsAppliedTwice() throws Exception {
        for (int i = 0; i < JOB_COUNT; i++) {
            repository.insert("test", "{}", "chaos-key-" + i);
        }

        AtomicInteger abandoned = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(WORKER_COUNT);

        for (int w = 0; w < WORKER_COUNT; w++) {
            String workerId = "chaos-worker-" + w;

            pool.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    Job job = repository.claim(workerId);

                    if (job == null) {
                        continue;
                    }

                    // 30% of the time, act like the worker was hard-killed:
                    // claim the job, then never finish it and never heartbeat.
                    if (ThreadLocalRandom.current().nextInt(100) < 30) {
                        abandoned.incrementAndGet();
                        continue;
                    }

                    repository.applyEffectOnce(job.id(), job.idempotencyKey());
                    repository.markSucceeded(job.id(), workerId);
                }
            });
        }

        long deadline = System.currentTimeMillis() + 60_000;

        while (System.currentTimeMillis() < deadline) {
            jdbc.update("UPDATE jobs SET lease_expires_at = now() - interval '1 second' "
                      + "WHERE state = 'RUNNING' AND lease_expires_at IS NOT NULL");
            repository.reapExpiredLeases();

            Integer unfinished = jdbc.queryForObject(
                "SELECT COUNT(*) FROM jobs WHERE state IN ('PENDING', 'RUNNING')", Integer.class);

            if (unfinished == 0) {
                break;
            }

            Thread.sleep(50);
        }

        pool.shutdownNow();
        pool.awaitTermination(10, TimeUnit.SECONDS);

        Integer succeeded = jdbc.queryForObject(
            "SELECT COUNT(*) FROM jobs WHERE state = 'SUCCEEDED'", Integer.class);
        Integer payments = jdbc.queryForObject(
            "SELECT COUNT(*) FROM fake_payments", Integer.class);
        Integer distinctKeys = jdbc.queryForObject(
            "SELECT COUNT(DISTINCT idempotency_key) FROM fake_payments", Integer.class);

        System.out.println("CHAOS: " + abandoned.get() + " jobs abandoned mid-flight");

        assertEquals(JOB_COUNT, succeeded, "every job must finish");
        assertEquals(JOB_COUNT, payments, "no duplicate effects");
        assertEquals(JOB_COUNT, distinctKeys, "one effect per key");
    }
}