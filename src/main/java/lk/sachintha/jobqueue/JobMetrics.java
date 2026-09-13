package lk.sachintha.jobqueue;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class JobMetrics {

    private final Counter claimed;
    private final Counter succeeded;
    private final Counter retried;
    private final Counter dead;
    private final Counter reaped;
    private final Counter effectsSkipped;
    private final Counter leasesLost;

    public JobMetrics(MeterRegistry registry, JobRepository repository) {
        this.claimed = Counter.builder("jobqueue.jobs.claimed")
            .description("Jobs claimed by a worker").register(registry);
        this.succeeded = Counter.builder("jobqueue.jobs.succeeded")
            .description("Jobs marked succeeded").register(registry);
        this.retried = Counter.builder("jobqueue.jobs.retried")
            .description("Jobs scheduled for retry after a failure").register(registry);
        this.dead = Counter.builder("jobqueue.jobs.dead")
            .description("Jobs moved to DEAD after max attempts").register(registry);
        this.reaped = Counter.builder("jobqueue.leases.reaped")
            .description("Expired leases reclaimed by the reaper").register(registry);
        this.effectsSkipped = Counter.builder("jobqueue.effects.skipped")
            .description("Effects skipped because the key was already applied").register(registry);
        this.leasesLost = Counter.builder("jobqueue.leases.lost")
            .description("Times a worker found its lease had been taken").register(registry);

        Gauge.builder("jobqueue.jobs.pending", () -> repository.countByState("PENDING"))
            .description("Jobs waiting to be claimed").register(registry);
        Gauge.builder("jobqueue.jobs.running", () -> repository.countByState("RUNNING"))
            .description("Jobs currently held by a worker").register(registry);
        Gauge.builder("jobqueue.jobs.dead.current", () -> repository.countByState("DEAD"))
            .description("Jobs in the dead letter state").register(registry);
    }

    public void jobClaimed() { claimed.increment(); }
    public void jobSucceeded() { succeeded.increment(); }
    public void jobRetried() { retried.increment(); }
    public void jobDead() { dead.increment(); }
    public void leasesReaped(int count) { reaped.increment(count); }
    public void effectSkipped() { effectsSkipped.increment(); }
    public void leaseLost() { leasesLost.increment(); }
}