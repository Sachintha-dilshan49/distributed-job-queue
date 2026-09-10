package lk.sachintha.jobqueue;

import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.InetAddress;

@Component
@Profile("worker")
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

        Job job = repository.claim(workerId);

        if (job == null) {
            return;
        }

        System.out.println(
            "Started job: id=" + job.id() +
            ", type=" + job.type() +
            ", working 40s"
        );

        for (int i = 0; i < 4; i++) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            int renewed = repository.heartbeat(job.id(), workerId);

            if (renewed == 0) {
                System.out.println("LEASE LOST: job " + job.id());
                return;
            }
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
}