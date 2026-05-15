package it.pagopa.cruscotto.ingestion.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "job_schedules")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JobScheduleEntity {
    @Id
    @Column(name = "entity_name", nullable = false, length = 50)
    private String entityName;

    @Column(name = "checkpoint_ts")
    private Instant checkpointTs;

    @Column(name = "initial_ts")
    private Instant initialTs;

    @Column(name = "updated_at")
    private Instant updatedAt;

    @Column(name = "last_run_id", length = 36)
    private String lastRunId;
}
