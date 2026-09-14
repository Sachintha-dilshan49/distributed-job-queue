package lk.sachintha.jobqueue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import java.util.Arrays;

@Component
public class DocumentChunkingJobHandler implements JobHandler {

    private final JobRepository repository;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DocumentChunkingJobHandler(JobRepository repository, JdbcTemplate jdbcTemplate) {
        this.repository = repository;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public String type() {
        return "chunk";
    }

    @Override
    public void handle(Job job) throws Exception {
        JsonNode payload = objectMapper.readTree(job.payload());
        String documentId = payload.get("documentId").asText();
        String text = payload.get("text").asText();

        String[] words = text.split("\\s+");
        int chunkIndex = 0;

        for (int i = 0; i < words.length; i += 100) {
            int end = Math.min(i + 100, words.length);
            String chunk = String.join(" ", Arrays.copyOfRange(words, i, end));
            jdbcTemplate.update(
                "INSERT INTO document_chunks (job_id, document_id, chunk_index, content) VALUES (?, ?, ?, ?)",
                job.id(), documentId, chunkIndex++, chunk
            );
        }

        repository.applyEffectOnce(job.id(), job.idempotencyKey());
    }
}