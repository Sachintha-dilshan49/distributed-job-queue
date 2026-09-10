package lk.sachintha.jobqueue;

import java.time.OffsetDateTime;

public record Job(
    Long id,
    String type,
    String payload,
    String state,
    OffsetDateTime createdAt,
    OffsetDateTime updatedAt,
    int attempts,
    int maxAttempts
) {}