package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.ingestor.LogHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@Slf4j
@Service
public class ExecutionLogService {

    private final JdbcTemplate jdbcTemplate;
    private final String schema;

    @Value("${ingestion.executionLog.enabled:true}")
    private boolean enabled;

    public ExecutionLogService(
            JdbcTemplate jdbcTemplate,
            DbSchemaConfig dbSchemaConfig) {
        this.jdbcTemplate = jdbcTemplate;
        this.schema = dbSchemaConfig.getSchemaName();
    }

    /** INSERT nativo — nessun SELECT preliminare, nessun flush EntityManager. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void logStarted(RunContext ctx, String jobName) {
        if (!enabled) return;
        try {
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
            jdbcTemplate.update(
                    "INSERT INTO " + schema + ".INGEST_EXECUTION_LOG " +
                    "(RUN_ID, ENTITY_NAME, JOB_NAME, STATUS, STARTED_AT, " +
                    "RECORDS_READ, RECORDS_TRANSFORMED, RECORDS_INSERTED, RECORDS_DISCARDED, RECORDS_STAGED, QUERY_COUNT, OPERATION_COUNT, " +
                    "ADX_QUERY_DURATION_MS, INGESTOR_LOGIC_DURATION_MS, POSTGRES_INSERT_DURATION_MS, RUN_WINDOW_FROM_TS, RUN_WINDOW_TO_TS, CREATED_AT) " +
                    "VALUES (?, ?, ?, 'STARTED', ?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NULL, NULL, ?)",
                    ctx.getRunId(), ctx.getEntityName(), jobName, now, now);
            LogHelper.info(ctx, "EXEC_LOG_START", "Execution log created");
        } catch (Exception ex) {
            LogHelper.error(ctx, "EXEC_LOG_START", "Failed to log execution start: {}", ex.getMessage());
            log.error("Failed to log execution start", ex);
        }
    }

    /**
     * UPDATE nativo — DURATION_MS calcolato in SQL via EXTRACT(EPOCH FROM (now - STARTED_AT)).
     * Nessuna SELECT + nessun roundtrip aggiuntivo.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void logCompleted(RunContext ctx, long recordsRead, long recordsTransformed,
                             long recordsInserted, long recordsDiscarded, long recordsStaged,
                             long queryCount, long operationCount, String endReason) {
        if (!enabled) return;
        try {
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
            jdbcTemplate.update(
                    "UPDATE " + schema + ".INGEST_EXECUTION_LOG " +
                    "SET STATUS = 'COMPLETED', ENDED_AT = ?, " +
                    "RECORDS_READ = ?, RECORDS_TRANSFORMED = ?, RECORDS_INSERTED = ?, " +
                    "RECORDS_DISCARDED = ?, RECORDS_STAGED = ?, QUERY_COUNT = ?, OPERATION_COUNT = ?, END_REASON = ?, " +
                    "ADX_QUERY_DURATION_MS = ?, INGESTOR_LOGIC_DURATION_MS = ?, POSTGRES_INSERT_DURATION_MS = ?, " +
                    "DURATION_MS = COALESCE((EXTRACT(EPOCH FROM (?::TIMESTAMPTZ - STARTED_AT)) * 1000)::BIGINT, 0) " +
                    "WHERE RUN_ID = ? AND ENTITY_NAME = ?",
                    now, recordsRead, recordsTransformed, recordsInserted,
                    recordsDiscarded, recordsStaged, queryCount, operationCount, endReason,
                    ctx.getAdxQueryDurationMs(), ctx.getIngestorLogicDurationMs(), ctx.getPostgresInsertDurationMs(),
                    now, ctx.getRunId(), ctx.getEntityName());
            LogHelper.info(ctx, "EXEC_LOG_END",
                    "Execution log completed: queryCount={}, operationCount={}, read={}, transformed={}, inserted={}, discarded={}, staged={}, " +
                            "adxQueryDurationMs={}, ingestorLogicDurationMs={}, postgresInsertDurationMs={}, endReason={}",
                    queryCount, operationCount, recordsRead, recordsTransformed, recordsInserted, recordsDiscarded, recordsStaged,
                    ctx.getAdxQueryDurationMs(), ctx.getIngestorLogicDurationMs(), ctx.getPostgresInsertDurationMs(), endReason);
        } catch (Exception ex) {
            LogHelper.error(ctx, "EXEC_LOG_END", "Failed to log execution completion: {}", ex.getMessage());
            log.error("Failed to log execution completion", ex);
        }
    }

    /**
     * UPDATE nativo — se il record non esiste (rows=0) esegue un INSERT FAILED come fallback.
     * DURATION_MS calcolato in SQL.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void logFailed(RunContext ctx, String errorCode, String errorMessage,
                          long recordsRead, long recordsTransformed, long recordsInserted,
                          long recordsDiscarded, long recordsStaged, long queryCount, long operationCount) {
        if (!enabled) return;
        try {
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
            int rows = jdbcTemplate.update(
                    "UPDATE " + schema + ".INGEST_EXECUTION_LOG " +
                    "SET STATUS = 'FAILED', ENDED_AT = ?, ERROR_CODE = ?, ERROR_MESSAGE = ?, " +
                    "RECORDS_READ = ?, RECORDS_TRANSFORMED = ?, RECORDS_INSERTED = ?, " +
                    "RECORDS_DISCARDED = ?, RECORDS_STAGED = ?, QUERY_COUNT = ?, OPERATION_COUNT = ?, " +
                    "ADX_QUERY_DURATION_MS = ?, INGESTOR_LOGIC_DURATION_MS = ?, POSTGRES_INSERT_DURATION_MS = ?, " +
                    "DURATION_MS = COALESCE((EXTRACT(EPOCH FROM (?::TIMESTAMPTZ - STARTED_AT)) * 1000)::BIGINT, 0) " +
                    "WHERE RUN_ID = ? AND ENTITY_NAME = ?",
                    now, errorCode, errorMessage,
                    recordsRead, recordsTransformed, recordsInserted,
                    recordsDiscarded, recordsStaged, queryCount, operationCount,
                    ctx.getAdxQueryDurationMs(), ctx.getIngestorLogicDurationMs(), ctx.getPostgresInsertDurationMs(),
                    now, ctx.getRunId(), ctx.getEntityName());
            if (rows == 0) {
                // Nessun record trovato: crea l'entry FAILED direttamente
                jdbcTemplate.update(
                        "INSERT INTO " + schema + ".INGEST_EXECUTION_LOG " +
                        "(RUN_ID, ENTITY_NAME, STATUS, STARTED_AT, ENDED_AT, ERROR_CODE, ERROR_MESSAGE, " +
                        "RECORDS_READ, RECORDS_TRANSFORMED, RECORDS_INSERTED, RECORDS_DISCARDED, RECORDS_STAGED, QUERY_COUNT, OPERATION_COUNT, " +
                        "ADX_QUERY_DURATION_MS, INGESTOR_LOGIC_DURATION_MS, POSTGRES_INSERT_DURATION_MS, " +
                        "DURATION_MS, CREATED_AT) " +
                        "VALUES (?, ?, 'FAILED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)",
                        ctx.getRunId(), ctx.getEntityName(), now, now, errorCode, errorMessage,
                        recordsRead, recordsTransformed, recordsInserted, recordsDiscarded, recordsStaged, queryCount, operationCount,
                        ctx.getAdxQueryDurationMs(), ctx.getIngestorLogicDurationMs(), ctx.getPostgresInsertDurationMs(),
                        now);
                LogHelper.error(ctx, "EXEC_LOG_END",
                        "Execution log created as FAILED: errorCode={}, errorMessage={}, queryCount={}, operationCount={}, read={}, transformed={}, inserted={}, discarded={}, staged={}, " +
                                "adxQueryDurationMs={}, ingestorLogicDurationMs={}, postgresInsertDurationMs={}",
                        errorCode, errorMessage, queryCount, operationCount,
                        recordsRead, recordsTransformed, recordsInserted, recordsDiscarded, recordsStaged,
                        ctx.getAdxQueryDurationMs(), ctx.getIngestorLogicDurationMs(), ctx.getPostgresInsertDurationMs());
            } else {
                LogHelper.error(ctx, "EXEC_LOG_END",
                        "Execution log failed: errorCode={}, errorMessage={}, queryCount={}, operationCount={}, read={}, transformed={}, inserted={}, discarded={}, staged={}, " +
                                "adxQueryDurationMs={}, ingestorLogicDurationMs={}, postgresInsertDurationMs={}",
                        errorCode, errorMessage, queryCount, operationCount,
                        recordsRead, recordsTransformed, recordsInserted, recordsDiscarded, recordsStaged,
                        ctx.getAdxQueryDurationMs(), ctx.getIngestorLogicDurationMs(), ctx.getPostgresInsertDurationMs());
            }
        } catch (Exception ex) {
            LogHelper.error(ctx, "EXEC_LOG_END", "Failed to log execution failure: {}", ex.getMessage());
            log.error("Failed to log execution failure", ex);
        }
    }

    /** UPDATE nativo — aggiorna tutti i contatori in un'unica query. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateMetrics(RunContext ctx, long recordsRead, long recordsTransformed,
                              long recordsInserted, long recordsDiscarded, long recordsStaged,
                              long queryCount, long operationCount) {
        if (!enabled) return;
        try {
            jdbcTemplate.update(
                    "UPDATE " + schema + ".INGEST_EXECUTION_LOG " +
                    "SET RECORDS_READ = ?, RECORDS_TRANSFORMED = ?, RECORDS_INSERTED = ?, " +
                    "RECORDS_DISCARDED = ?, RECORDS_STAGED = ?, QUERY_COUNT = ?, OPERATION_COUNT = ? " +
                    "WHERE RUN_ID = ? AND ENTITY_NAME = ?",
                    recordsRead, recordsTransformed, recordsInserted,
                    recordsDiscarded, recordsStaged, queryCount, operationCount,
                    ctx.getRunId(), ctx.getEntityName());
            LogHelper.info(ctx, "EXEC_LOG_UPDATE",
                    "Metrics updated: queryCount={}, operationCount={}, read={}, transformed={}, inserted={}, discarded={}, staged={}",
                    queryCount, operationCount, recordsRead, recordsTransformed, recordsInserted, recordsDiscarded, recordsStaged);
        } catch (Exception ex) {
            LogHelper.error(ctx, "EXEC_LOG_UPDATE", "Failed to update execution metrics: {}", ex.getMessage());
            log.error("Failed to update execution metrics", ex);
        }
    }

    /** UPDATE nativo — RECORDS_STAGED incrementato atomicamente in SQL (nessuna lettura). */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void incrementStagedCount(RunContext ctx) {
        if (!enabled) return;
        try {
            jdbcTemplate.update(
                    "UPDATE " + schema + ".INGEST_EXECUTION_LOG " +
                    "SET RECORDS_STAGED = COALESCE(RECORDS_STAGED, 0) + 1 " +
                    "WHERE RUN_ID = ? AND ENTITY_NAME = ?",
                    ctx.getRunId(), ctx.getEntityName());
        } catch (Exception ex) {
            LogHelper.error(ctx, "EXEC_LOG_UPDATE", "Failed to increment staged count: {}", ex.getMessage());
            log.error("Failed to increment staged count", ex);
        }
    }

    /** UPDATE nativo — aggiorna checkpoint e operationId in una sola query. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateLatestCheckpoint(RunContext ctx, Instant latestCheckpointTs) {
        if (!enabled || latestCheckpointTs == null) return;
        try {
            jdbcTemplate.update(
                    "UPDATE " + schema + ".INGEST_EXECUTION_LOG " +
                    "SET LATEST_CHECKPOINT_TS = ?, LATEST_OPERATION_ID = ? " +
                    "WHERE RUN_ID = ? AND ENTITY_NAME = ?",
                    OffsetDateTime.ofInstant(latestCheckpointTs, ZoneOffset.UTC), ctx.getOperationId(),
                    ctx.getRunId(), ctx.getEntityName());
            LogHelper.info(ctx, "EXEC_LOG_CHECKPOINT",
                    "Latest checkpoint updated: {}, latestOperationId={}", latestCheckpointTs, ctx.getOperationId());
        } catch (Exception ex) {
            LogHelper.error(ctx, "EXEC_LOG_CHECKPOINT", "Failed to update latest checkpoint: {}", ex.getMessage());
            log.error("Failed to update latest checkpoint", ex);
        }
    }

    /** UPDATE nativo — salva la finestra temporale complessiva su cui ha lavorato il run. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateRunWindow(RunContext ctx, Instant runWindowFromTs, Instant runWindowToTs) {
        if (!enabled || runWindowFromTs == null || runWindowToTs == null) return;
        try {
            jdbcTemplate.update(
                    "UPDATE " + schema + ".INGEST_EXECUTION_LOG " +
                    "SET RUN_WINDOW_FROM_TS = ?, RUN_WINDOW_TO_TS = ? " +
                    "WHERE RUN_ID = ? AND ENTITY_NAME = ?",
                    OffsetDateTime.ofInstant(runWindowFromTs, ZoneOffset.UTC),
                    OffsetDateTime.ofInstant(runWindowToTs, ZoneOffset.UTC),
                    ctx.getRunId(), ctx.getEntityName());
            LogHelper.info(ctx, "EXEC_LOG_WINDOW",
                    "Run window updated: from={}, to={}", runWindowFromTs, runWindowToTs);
        } catch (Exception ex) {
            LogHelper.error(ctx, "EXEC_LOG_WINDOW", "Failed to update run window: {}", ex.getMessage());
            log.error("Failed to update run window", ex);
        }
    }
}
