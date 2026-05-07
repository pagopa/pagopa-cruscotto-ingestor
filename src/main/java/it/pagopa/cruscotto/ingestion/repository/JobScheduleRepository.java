package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.JobScheduleEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Repository
public interface JobScheduleRepository extends JpaRepository<JobScheduleEntity, String> {

    @Modifying
    @Transactional
    @Query(value = "INSERT INTO ingestor.job_schedules (entity_name, checkpoint_ts, updated_at, last_run_id) " +
            "VALUES (:entityName, :checkpointTs, :updatedAt, :lastRunId) " +
            "ON CONFLICT (entity_name) DO UPDATE SET " +
            "checkpoint_ts = EXCLUDED.checkpoint_ts, " +
            "updated_at = EXCLUDED.updated_at, " +
            "last_run_id = EXCLUDED.last_run_id", nativeQuery = true)
    void upsertCheckpoint(
            @Param("entityName") String entityName,
            @Param("checkpointTs") Instant checkpointTs,
            @Param("updatedAt") Instant updatedAt,
            @Param("lastRunId") String lastRunId
    );
}
