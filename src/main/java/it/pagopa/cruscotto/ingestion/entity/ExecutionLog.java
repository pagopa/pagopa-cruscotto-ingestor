package it.pagopa.cruscotto.ingestion.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "INGEST_EXECUTION_LOG")
public class ExecutionLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ID")
    private Long id;

    @Column(name = "RUN_ID", nullable = false)
    private String runId;

    @Column(name = "ENTITY_NAME", nullable = false)
    private String entityName;

    @Column(name = "JOB_NAME")
    private String jobName;

    @Column(name = "STATUS", nullable = false)
    private String status;

    @Column(name = "STARTED_AT", nullable = false)
    private OffsetDateTime startedAt;

    @Column(name = "ENDED_AT")
    private OffsetDateTime endedAt;

    @Column(name = "LATEST_CHECKPOINT_TS")
    private OffsetDateTime latestCheckpointTs;

    @Column(name = "LATEST_OPERATION_ID")
    private String latestOperationId;

    @Column(name = "END_REASON")
    private String endReason;

    @Column(name = "RUN_WINDOW_FROM_TS")
    private OffsetDateTime runWindowFromTs;

    @Column(name = "RUN_WINDOW_TO_TS")
    private OffsetDateTime runWindowToTs;

    @Column(name = "DURATION_MS")
    private Long durationMs;

    @Column(name = "ADX_QUERY_DURATION_MS", nullable = false)
    @Builder.Default
    private Long adxQueryDurationMs = 0L;

    @Column(name = "INGESTOR_LOGIC_DURATION_MS", nullable = false)
    @Builder.Default
    private Long ingestorLogicDurationMs = 0L;

    @Column(name = "POSTGRES_INSERT_DURATION_MS", nullable = false)
    @Builder.Default
    private Long postgresInsertDurationMs = 0L;

    @Column(name = "ANAGRAFICA_DURATION_MS", nullable = false)
    @Builder.Default
    private Long anagraficaDurationMs = 0L;

    @Column(name = "FK_POSITION_DURATION_MS", nullable = false)
    @Builder.Default
    private Long fkPositionDurationMs = 0L;

    @Column(name = "FK_TOKEN_DURATION_MS", nullable = false)
    @Builder.Default
    private Long fkTokenDurationMs = 0L;

    @Column(name = "PROCESS_CPU_LOAD_PCT", nullable = false)
    @Builder.Default
    private Double processCpuLoadPct = 0.0;

    @Column(name = "JVM_USED_MEMORY_MB", nullable = false)
    @Builder.Default
    private Long jvmUsedMemoryMb = 0L;

    @Column(name = "JVM_TOTAL_MEMORY_MB", nullable = false)
    @Builder.Default
    private Long jvmTotalMemoryMb = 0L;

    @Column(name = "ANAGRAFICA_LOOKUP_COUNT", nullable = false)
    @Builder.Default
    private Long anagraficaLookupCount = 0L;

    @Column(name = "POSITION_LOOKUP_COUNT", nullable = false)
    @Builder.Default
    private Long positionLookupCount = 0L;

    @Column(name = "TOKEN_LOOKUP_COUNT", nullable = false)
    @Builder.Default
    private Long tokenLookupCount = 0L;

    @Column(name = "CACHE_HIT_COUNT", nullable = false)
    @Builder.Default
    private Long cacheHitCount = 0L;

    @Column(name = "CACHE_MISS_COUNT", nullable = false)
    @Builder.Default
    private Long cacheMissCount = 0L;

    @Column(name = "ADX_WINDOW_COUNT", nullable = false)
    @Builder.Default
    private Long adxWindowCount = 0L;

    @Column(name = "ADX_ATTEMPT_COUNT", nullable = false)
    @Builder.Default
    private Long adxAttemptCount = 0L;

    @Column(name = "EMPTY_WINDOW_COUNT", nullable = false)
    @Builder.Default
    private Long emptyWindowCount = 0L;

    @Column(name = "RECORDS_READ", nullable = false)
    @Builder.Default
    private Long recordsRead = 0L;

    @Column(name = "RECORDS_TRANSFORMED", nullable = false)
    @Builder.Default
    private Long recordsTransformed = 0L;

    @Column(name = "RECORDS_INSERTED", nullable = false)
    @Builder.Default
    private Long recordsInserted = 0L;

    @Column(name = "RECORDS_DISCARDED", nullable = false)
    @Builder.Default
    private Long recordsDiscarded = 0L;

    @Column(name = "RECORDS_STAGED", nullable = false)
    @Builder.Default
    private Long recordsStaged = 0L;

    @Column(name = "QUERY_COUNT", nullable = false)
    @Builder.Default
    private Long queryCount = 0L;

    @Column(name = "OPERATION_COUNT", nullable = false)
    @Builder.Default
    private Long operationCount = 0L;

    @Column(name = "ERROR_CODE")
    private String errorCode;

    @Column(name = "ERROR_MESSAGE", columnDefinition = "text")
    private String errorMessage;

    @Column(name = "CREATED_AT", nullable = false)
    private OffsetDateTime createdAt;
}
