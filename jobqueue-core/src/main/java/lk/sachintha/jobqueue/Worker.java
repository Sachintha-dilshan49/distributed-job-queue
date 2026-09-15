package lk.sachintha.jobqueue;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@Profile("worker")
public class Worker {

    private static final long MAX_JITTER_SECONDS = 3;

    private final JobRepository repository;
    private final JobMetrics metrics;
    private final Map<String, JobHandler> handlers;
    private final String workerId;
    private final long baseDelaySeconds;
    private final long leaseSeconds;

    private final ScheduledExecutorService heartbeatExecutor =
        Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat");
            t.setDaemon(true);
            return t;
        });

    public Worker(JobRepository repository,
                  JobMetrics metrics,
                  List<JobHandler> handlerList,
                  @Value("${jobqueue.backoff-base-seconds}") long baseDelaySeconds,
                  @Value("${jobqueue.lease-seconds}") long leaseSeconds) {
        this.repository = repository;
        this.metrics = metrics;
        this.baseDelaySeconds = baseDelaySeconds;
        this.leaseSeconds = leaseSeconds;

        this.handlers = handlerList.stream()
            .collect(Collectors.toMap(JobHandler::type, Function.identity()));

        String hostname;

        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            hostname = "unknown";
        }

        long pid = ProcessHandle.current().pid();

        this.workerId = hostname + "-" + pid;

        System.out.println("Worker ID: " + this.workerId
            + ", handlers: " + handlers.keySet());
    }

    @Scheduled(fixedDelay = 10)
    public void poll() {

        Job job = repository.claim(workerId);

        if (job == null) {
            return;
        }

        metrics.jobClaimed();

        System.out.println(
            "Started job: id=" + job.id() +
            ", type=" + job.type() +
            ", attempt " + job.attempts() + "/" + job.maxAttempts()
        );

        ScheduledFuture<?> heartbeat = startHeartbeat(job);

        try {
            JobHandler handler = handlers.get(job.type());

            if (handler == null) {
                throw new IllegalStateException(
                    "no handler registered for type: " + job.type());
            }

            handler.handle(job);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (Exception e) {
            handleFailure(job, e);
            return;
        } finally {
            heartbeat.cancel(false);
        }

        int updated = repository.markSucceeded(job.id(), workerId);

        if (updated == 0) {
            metrics.leaseLost();
            System.out.println(
                "LEASE LOST: job " + job.id() + " now belongs to another worker"
            );
        } else {
            metrics.jobSucceeded();
            System.out.println(
                "Processing job: id=" + job.id() +
                ", type=" + job.type()
            );
        }
    }

    private ScheduledFuture<?> startHeartbeat(Job job) {
        long intervalMs = Math.max(1000, leaseSeconds * 1000 / 3);

        return heartbeatExecutor.scheduleAtFixedRate(() -> {
            if (repository.heartbeat(job.id(), workerId) == 0) {
                metrics.leaseLost();
                System.out.println("LEASE LOST: job " + job.id()
                    + " while still working on it");
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void handleFailure(Job job, Exception e) {
        String error = e.getMessage();
        int updated;

        if (job.attempts() >= job.maxAttempts()) {
            updated = repository.markDead(job.id(), workerId, error);

            if (updated > 0) {
                metrics.jobDead();
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
                metrics.jobRetried();
                System.out.println(
                    "RETRY: job " + job.id() +
                    " attempt " + job.attempts() + "/" + job.maxAttempts() +
                    " failed, retrying in " + delay + "s"
                );
            }
        }

        if (updated == 0) {
            metrics.leaseLost();
            System.out.println(
                "LEASE LOST: job " + job.id() + " failed but now belongs to another worker"
            );
        }
    }
}