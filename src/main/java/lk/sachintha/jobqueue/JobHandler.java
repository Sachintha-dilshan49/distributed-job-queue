package lk.sachintha.jobqueue;

public interface JobHandler {

    String type();

    void handle(Job job) throws Exception;
}