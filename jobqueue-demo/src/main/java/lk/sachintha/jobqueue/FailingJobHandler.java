package lk.sachintha.jobqueue;

import org.springframework.stereotype.Component;

@Component
public class FailingJobHandler implements JobHandler {

    @Override
    public String type() {
        return "fail";
    }

    @Override
    public void handle(Job job) {
        throw new RuntimeException("simulated failure for testing");
    }
}