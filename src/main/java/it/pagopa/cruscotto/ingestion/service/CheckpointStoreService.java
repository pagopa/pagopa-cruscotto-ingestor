package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.JobScheduleEntity;
import it.pagopa.cruscotto.ingestion.repository.JobScheduleRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class CheckpointStoreService {
    private final JobScheduleRepository jobScheduleRepository;

    /**
     * Retrieves the last checkpoint for the given entity.
     *
     * @param entity the entity name
     * @return Optional containing the checkpoint timestamp, or empty if not found
     */
    public Optional<Instant> getCheckpoint(EntityName entity) {
        return jobScheduleRepository.findById(entity.name())
                .map(JobScheduleEntity::getCheckpointTs);
    }

    /**
     * Updates the checkpoint for the given entity with UPSERT semantics.
     *
     * @param entity the entity name
     * @param checkpoint the new checkpoint timestamp
     * @param runId the run ID for traceability
     */
    public void updateCheckpoint(EntityName entity, Instant checkpoint, String runId) {
        Instant now = Instant.now();
        jobScheduleRepository.upsertCheckpoint(entity.name(), checkpoint, now, runId);
        log.info("Updated checkpoint runId={} entity={} checkpoint={} updated_at={}", runId, entity.name(), checkpoint, now);
    }

    /**
     * Retrieves the last run ID for the given entity.
     *
     * @param entity the entity name
     * @return Optional containing the last run ID, or empty if not found
     */
    public Optional<String> getLastRunId(EntityName entity) {
        return jobScheduleRepository.findById(entity.name())
                .map(JobScheduleEntity::getLastRunId);
    }
}

