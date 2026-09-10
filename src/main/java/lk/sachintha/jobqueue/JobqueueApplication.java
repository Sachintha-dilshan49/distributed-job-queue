package lk.sachintha.jobqueue;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class JobqueueApplication {

	public static void main(String[] args) {
		SpringApplication.run(JobqueueApplication.class, args);
	}

}