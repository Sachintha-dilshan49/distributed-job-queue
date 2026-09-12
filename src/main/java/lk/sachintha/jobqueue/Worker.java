package lk.sachintha.jobqueue;

import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

import java.net.InetAddress;
import java.util.concurrent.ThreadLocalRandom;

@Component
@Profile("worker")
public class Worker {

       private static final long MAX_JITTER_SECONDS = 3;

    private final JobRepository repository;
    private final String workerId;
    private final long baseDelaySeconds;
    private final long workDurationMs;

    public Worker(JobRepository repository,
                  @Value("${jobqueue.backoff-base-seconds}") long baseDelaySeconds,
                  @Value("${jobqueue.work-duration-ms}") long workDurationMs) {
        this.repository = repository;
        this.baseDelaySeconds = baseDelaySeconds;
        this.workDurationMs = workDurationMs;

        String hostname;

        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            hostname = "unknown";
        }

        long pid = ProcessHandle.current().pid();

        this.workerId = hostname + "-" + pid;

        System.out.println("Worker ID: " + this.workerId);
    }

    @Scheduled(fixedDelay = 10)
    public void poll() {

        Job job = repository.claim(workerId);

        if (job == null) {
            return;
        }

        System.out.println(
            "Started job: id=" + job.id() +
            ", type=" + job.type() +
            ", attempt " + job.attempts() + "/" + job.maxAttempts()
        );

        try {
            if (job.type().equals("fail")) {
                throw new RuntimeException("simulated failure for testing");
            }

                       Thread.sleep(workDurationMs / 2);

            if (repository.heartbeat(job.id(), workerId) == 0) {
                System.out.println("LEASE LOST: job " + job.id());
                return;
            }

            Thread.sleep(workDurationMs / 2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (RuntimeException e) {
            handleFailure(job, e);
            return;
        }
                int applied = repository.applyEffectOnce(job.id(), job.idempotencyKey());

        if (applied == 0) {
            System.out.println(
                "SKIPPED: effect for key " + job.idempotencyKey() + " was already applied"
            );
        }
        
        int updated = repository.markSucceeded(job.id(), workerId);

        if (updated == 0) {
            System.out.println(
                "LEASE LOST: job " + job.id() + " now belongs to another worker"
            );
        } else {
            System.out.println(
                "Processing job: id=" + job.id() +
                ", type=" + job.type()
            );
        }
    }

    private void handleFailure(Job job, RuntimeException e) {
        String error = e.getMessage();
        int updated;

        if (job.attempts() >= job.maxAttempts()) {
            updated = repository.markDead(job.id(), workerId, error);

            if (updated > 0) {
                System.out.println(
                    "DEAD: job " + job.id() +
                    " after " + job.attempts() + " attempts: " + error
                );
            }
        } else {
            long backoff = baseDelaySeconds * (long) Math.pow(2, job.attempts() - 1);
            long jitter = ThreadLocalRandom.current().nextLong(0, MAX_JITTER_SECONDS + 1);
            long delay = backoff + jitter;

            updated = repository.scheduleRetry(job.id(), workerId, error, delay);

            if (updated > 0) {
                System.out.println(
                    "RETRY: job " + job.id() +
                    " attempt " + job.attempts() + "/" + job.maxAttempts() +
                    " failed, retrying in " + delay + "s"
                );
            }
        }

        if (updated == 0) {
            System.out.println(
                "LEASE LOST: job " + job.id() + " failed but now belongs to another worker"
            );
        }
    }
}