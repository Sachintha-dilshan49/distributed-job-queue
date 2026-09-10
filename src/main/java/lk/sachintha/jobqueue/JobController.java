package lk.sachintha.jobqueue;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

@RestController
@RequestMapping("/jobs")
public class JobController {

    private final JobRepository repository;

    public JobController(JobRepository repository) {
        this.repository = repository;
    }

    @PostMapping
    public ResponseEntity<Map<String, Long>> create(@RequestBody CreateJobRequest request) {
        Long id = repository.insert(request.type(), request.payload());
return ResponseEntity.status(201).body(Map.of("id", id));
    }

    public record CreateJobRequest(String type, String payload) {}
}