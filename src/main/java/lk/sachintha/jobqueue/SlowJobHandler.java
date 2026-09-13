package lk.sachintha.jobqueue;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SlowJobHandler implements JobHandler {

    private final JobRepository repository;
    private final long workDurationMs;

    public SlowJobHandler(JobRepository repository,
                          @Value("${jobqueue.work-duration-ms}") long workDurationMs) {
        this.repository = repository;
        this.workDurationMs = workDurationMs;
    }

    @Override
    public String type() {
        return "test";
    }

    @Override
    public void handle(Job job) throws Exception {
        Thread.sleep(workDurationMs);
        repository.applyEffectOnce(job.id(), job.idempotencyKey());
    }
}