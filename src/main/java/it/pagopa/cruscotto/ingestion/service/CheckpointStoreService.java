package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.JobScheduleEntity;
import it.pagopa.cruscotto.ingestion.repository.JobScheduleRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class CheckpointStoreService {
    private final JobScheduleRepository jobScheduleRepository;
    private final JdbcTemplate jdbcTemplate;
    private final DbSchemaConfig dbSchemaConfig;

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
     * Retrieves the initial (first run) timestamp for the given entity.
     * Used to synchronize bootstrap cursors: entities without a checkpoint
     * start from the minimum initial_ts across all entities.
     *
     * @param entity the entity name
     * @return Optional containing the initial timestamp, or empty if not found
     */
    public Optional<Instant> getInitialTs(EntityName entity) {
        return jobScheduleRepository.findById(entity.name())
                .map(JobScheduleEntity::getInitialTs);
    }

    /**
     * Retrieves the most recent checkpoint across all entities.
     * Used when a specific entity needs to catch up to others.
     *
     * @return Optional containing the maximum checkpoint timestamp across all entities, or empty if none found
     */
    public Optional<Instant> getMaxCheckpointAcrossAllEntities() {
        Instant maxCheckpoint = null;
        for (EntityName entity : EntityName.values()) {
            Optional<Instant> checkpoint = getCheckpoint(entity);
            if (checkpoint.isPresent()) {
                if (maxCheckpoint == null || checkpoint.get().isAfter(maxCheckpoint)) {
                    maxCheckpoint = checkpoint.get();
                }
            }
        }
        return Optional.ofNullable(maxCheckpoint);
    }

    /**
     * Retrieves the minimum initial timestamp across all entities.
     * All entities without a specific checkpoint start from this common date
     * to ensure no data is lost and all entities stay synchronized at startup.
     *
     * @return Optional containing the minimum initial timestamp across all entities, or empty if none found
     */
    public Optional<Instant> getMinInitialTsAcrossAllEntities() {
        Instant minInitialTs = null;
        for (EntityName entity : EntityName.values()) {
            Optional<Instant> initialTs = getInitialTs(entity);
            if (initialTs.isPresent()) {
                if (minInitialTs == null || initialTs.get().isBefore(minInitialTs)) {
                    minInitialTs = initialTs.get();
                }
            }
        }
        return Optional.ofNullable(minInitialTs);
    }

    /**
     * Initializes initial_ts to the given cursor for the entity, only if not already set.
     * Must be called before the first window is processed so that initial_ts reflects
     * the true starting point, not the first checkpoint written afterwards.
     * Idempotent — safe to call on every run.
     *
     * @param entity    the entity name
     * @param cursor    the cursor determined before processing starts
     * @param runId     the run ID for traceability
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void initializeInitialTs(EntityName entity, Instant cursor, String runId) {
        applyLocalWriteTimeouts();
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        OffsetDateTime cursorTs = OffsetDateTime.ofInstant(cursor, ZoneOffset.UTC);
        String entityName = entity.name();
        String schema = dbSchemaConfig.getSchemaName();
        int updated = jdbcTemplate.update(
                "UPDATE " + schema + ".job_schedules " +
                        "SET initial_ts = COALESCE(initial_ts, ?), updated_at = ?, last_run_id = ? " +
                        "WHERE entity_name = ?",
                cursorTs, now, runId, entityName
        );
        if (updated == 0) {
            insertScheduleRowIfMissing(entityName, runId);
            updated = jdbcTemplate.update(
                    "UPDATE " + schema + ".job_schedules " +
                            "SET initial_ts = COALESCE(initial_ts, ?), updated_at = ?, last_run_id = ? " +
                            "WHERE entity_name = ?",
                    cursorTs, now, runId, entityName
            );
            if (updated == 0) {
                throw new IllegalStateException("Unable to initialize initial_ts for entity=" + entityName);
            }
        }
        log.debug("Initialized initial_ts runId={} entity={} cursor={}", runId, entity.name(), cursor);
    }

    /**
     * Updates the checkpoint for the given entity with UPSERT semantics.
     * initial_ts is NOT modified here — it is set once via initializeInitialTs().
     *
     * @param entity the entity name
     * @param checkpoint the new checkpoint timestamp
     * @param runId the run ID for traceability
     */
    public void updateCheckpoint(EntityName entity, Instant checkpoint, String runId) {
        applyLocalWriteTimeouts();
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        OffsetDateTime checkpointTs = OffsetDateTime.ofInstant(checkpoint, ZoneOffset.UTC);
        String entityName = entity.name();
        String schema = dbSchemaConfig.getSchemaName();

        // Atomic UPSERT avoids UPDATE-then-INSERT races under concurrent runs/restarts.
        int affected = jdbcTemplate.update(
                "INSERT INTO " + schema + ".job_schedules " +
                        "(entity_name, checkpoint_ts, initial_ts, updated_at, last_run_id) " +
                        "VALUES (?, ?, ?, ?, ?) " +
                        "ON CONFLICT (entity_name) DO UPDATE SET " +
                        "checkpoint_ts = EXCLUDED.checkpoint_ts, " +
                        "initial_ts = COALESCE(" + schema + ".job_schedules.initial_ts, EXCLUDED.initial_ts), " +
                        "updated_at = EXCLUDED.updated_at, " +
                        "last_run_id = EXCLUDED.last_run_id",
                entityName, checkpointTs, checkpointTs, now, runId
        );
        if (affected == 0) {
            throw new IllegalStateException("Unable to update checkpoint for entity=" + entityName);
        }
        log.info("Updated checkpoint runId={} entity={} checkpoint={} updated_at={}", runId, entity.name(), checkpoint, now);
    }

    private void insertScheduleRowIfMissing(String entityName, String runId) {
        applyLocalWriteTimeouts();
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        String schema = dbSchemaConfig.getSchemaName();
        int inserted = jdbcTemplate.update(
                "INSERT INTO " + schema + ".job_schedules " +
                        "(entity_name, checkpoint_ts, initial_ts, updated_at, last_run_id) " +
                        "VALUES (?, NULL, NULL, ?, ?) " +
                        "ON CONFLICT (entity_name) DO NOTHING",
                entityName, now, runId
        );
        if (inserted > 0) {
            log.warn("Recovered missing job_schedules row at runtime runId={} entity={}", runId, entityName);
        }
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

    private void applyLocalWriteTimeouts() {
        // Prevent Quartz worker starvation due to long DB lock waits.
        try {
            jdbcTemplate.execute("SET LOCAL lock_timeout = '10s'");
            jdbcTemplate.execute("SET LOCAL statement_timeout = '30s'");
        } catch (Exception ex) {
            log.debug("Unable to set local DB timeouts for checkpoint writes: {}", ex.getMessage());
        }
    }
}

