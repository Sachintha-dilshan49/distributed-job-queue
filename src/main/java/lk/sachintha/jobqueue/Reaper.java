package lk.sachintha.jobqueue;

import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("!worker")
public class Reaper {

    private final JobRepository repository;

    public Reaper(JobRepository repository) {
        this.repository = repository;
    }

       @Scheduled(fixedDelayString = "${jobqueue.reaper-interval-ms}")
    public void reap() {
        int reaped = repository.reapExpiredLeases();

        if (reaped > 0) {
            System.out.println("REAPER: reaped " + reaped + " expired leases");
        }
    }
}