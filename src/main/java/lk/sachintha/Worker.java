package lk.sachintha.jobqueue;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.InetAddress;

@Component
public class Worker {

    private final JobRepository repository;
    private final String workerId;

    public Worker(JobRepository repository) {
        this.repository = repository;

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

        Job job = repository.claim();

        if (job == null) {
            return;
        }

        int updated = repository.markSucceeded(job.id());

        if (updated == 0) {
            System.out.println(
                "DUPLICATE: job " + job.id() + " was already taken"
            );
        } else {
            System.out.println(
                "Processing job: id=" + job.id() +
                ", type=" + job.type()
            );
        }
    }
}