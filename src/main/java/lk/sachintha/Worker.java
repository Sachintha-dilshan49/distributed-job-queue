package lk.sachintha.jobqueue;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class Worker {

    private final JobRepository repository;

    public Worker(JobRepository repository) {
        this.repository = repository;
    }

    @Scheduled(fixedDelay = 1000)
    public void poll() {

      
        Job job = repository.findOnePending();

        if (job == null) {
            return;
        }

        System.out.println(
            "Processing job: id=" + job.id() + ", type=" + job.type()
        );
        repository.markSucceeded(job.id());
    }
}