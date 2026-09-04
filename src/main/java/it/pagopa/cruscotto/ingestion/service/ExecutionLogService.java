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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@Slf4j
@Service
public class ExecutionLogService {

    /** Pod / instance identifier, resolved once. In Kubernetes HOSTNAME is the pod name. */
    private static final String INSTANCE_ID = resolveInstanceId();

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

    /** Pod / instance identifier (HOSTNAME in k8s), exposed for startup/shutdown correlation. */
    public static String getInstanceId() {
        return INSTANCE_ID;
    }

    private static String resolveInstanceId() {
        String host = System.getenv("HOSTNAME"); // pod name in Kubernetes
        if (host == null || host.isBlank()) {
            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch (Exception ignored) {
                host = "unknown";
            }
        }
        return host;
    }

    /** INSERT nativo — nessun SELECT preliminare, nessun flush EntityManager. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void logStarted(RunContext ctx, String jobName) {
        if (!enabled) return;
        try {
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
            RuntimeMetrics metrics = captureRuntimeMetrics();
            jdbcTemplate.update(
                    "INSERT INTO " + schema + ".INGEST_EXECUTION_LOG " +
                    "(RUN_ID, ENTITY_NAME, JOB_NAME, INSTANCE_ID, STATUS, STARTED_AT, LAST_HEARTBEAT_AT, " +
                    "RECORDS_READ, RECORDS_TRANSFORMED, RECORDS_INSERTED, RECORDS_DISCARDED, RECORDS_STAGED, QUERY_COUNT, OPERATION_COUNT, " +
                    "ADX_QUERY_DURATION_MS, INGESTOR_LOGIC_DURATION_MS, POSTGRES_INSERT_DURATION_MS, ANAGRAFICA_DURATION_MS, FK_POSITION_DURATION_MS, FK_TOKEN_DURATION_MS, " +
                    "PROCESS_CPU_LOAD_PCT, JVM_USED_MEMORY_MB, JVM_TOTAL_MEMORY_MB, ANAGRAFICA_LOOKUP_COUNT, POSITION_LOOKUP_COUNT, TOKEN_LOOKUP_COUNT, " +
                    "CACHE_HIT_COUNT, CACHE_MISS_COUNT, ADX_WINDOW_COUNT, ADX_ATTEMPT_COUNT, EMPTY_WINDOW_COUNT, RUN_WINDOW_FROM_TS, RUN_WINDOW_TO_TS, CREATED_AT) " +
                    "VALUES (?, ?, ?, ?, 'STARTED', ?, ?, " +
                    "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0, NULL, NULL, ?)",
                    ctx.getRunId(), ctx.getEntityName(), jobName, INSTANCE_ID, now, now,
                    metrics.processCpuLoadPct(), metrics.jvmUsedMemoryMb(), metrics.jvmTotalMemoryMb(), now);
            LogHelper.info(ctx, "EXEC_LOG_START",
                    "Execution log created: instanceId={} jvmUsedMb={} jvmTotalMb={}",
                    INSTANCE_ID, metrics.jvmUsedMemoryMb(), metrics.jvmTotalMemoryMb());
        } catch (Exception ex) {
            LogHelper.error(ctx, "EXEC_LOG_START", "Failed to log execution start: {}", ex.getMessage());
            log.error("Failed to log execution start", ex);
        }
    }

    /**
     * Best-effort liveness/progress heartbeat written periodically during a run (typically once per
     * ADX window). Refreshes LAST_HEARTBEAT_AT and a snapshot of the running counters/metrics so that
     * (a) the reaper can tell a live run from a dead one and (b) an interrupted run's row still shows
     * how far it got and its memory trajectory before the process died. Never throws — observability
     * must not affect the run.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void heartbeat(RunContext ctx, long recordsRead, long recordsTransformed, long recordsInserted,
                          long recordsDiscarded, long recordsStaged, long queryCount, long operationCount) {
        if (!enabled) return;
        try {
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
            RuntimeMetrics metrics = captureRuntimeMetrics();
            jdbcTemplate.update(
                    "UPDATE " + schema + ".INGEST_EXECUTION_LOG " +
                    "SET LAST_HEARTBEAT_AT = ?, " +
                    "RECORDS_READ = ?, RECORDS_TRANSFORMED = ?, RECORDS_INSERTED = ?, RECORDS_DISCARDED = ?, RECORDS_STAGED = ?, " +
                    "QUERY_COUNT = ?, OPERATION_COUNT = ?, " +
                    "ADX_QUERY_DURATION_MS = ?, INGESTOR_LOGIC_DURATION_MS = ?, POSTGRES_INSERT_DURATION_MS = ?, " +
                    "ANAGRAFICA_DURATION_MS = ?, FK_POSITION_DURATION_MS = ?, FK_TOKEN_DURATION_MS = ?, " +
                    "PROCESS_CPU_LOAD_PCT = ?, JVM_USED_MEMORY_MB = ?, JVM_TOTAL_MEMORY_MB = ?, " +
                    "ANAGRAFICA_LOOKUP_COUNT = ?, POSITION_LOOKUP_COUNT = ?, TOKEN_LOOKUP_COUNT = ?, " +
                    "CACHE_HIT_COUNT = ?, CACHE_MISS_COUNT = ?, ADX_WINDOW_COUNT = ?, ADX_ATTEMPT_COUNT = ?, EMPTY_WINDOW_COUNT = ? " +
                    "WHERE RUN_ID = ? AND ENTITY_NAME = ? AND STATUS = 'STARTED'",
                    now, recordsRead, recordsTransformed, recordsInserted, recordsDiscarded, recordsStaged,
                    queryCount, operationCount,
                    ctx.getAdxQueryDurationMs(), ctx.getIngestorLogicDurationMs(), ctx.getPostgresInsertDurationMs(),
                    ctx.getAnagraficaDurationMs(), ctx.getFkPositionDurationMs(), ctx.getFkTokenDurationMs(),
                    metrics.processCpuLoadPct(), metrics.jvmUsedMemoryMb(), metrics.jvmTotalMemoryMb(),
                    ctx.getAnagraficaLookupCount(), ctx.getPositionLookupCount(), ctx.getTokenLookupCount(),
                    ctx.getCacheHitCount(), ctx.getCacheMissCount(), ctx.getAdxWindowCount(), ctx.getAdxAttemptCount(), ctx.getEmptyWindowCount(),
                    ctx.getRunId(), ctx.getEntityName());
            if (log.isDebugEnabled()) {
                LogHelper.debug(ctx, "EXEC_LOG_HEARTBEAT",
                        "heartbeat: read={} inserted={} staged={} adxWindows={} jvmUsedMb={} jvmTotalMb={}",
                        recordsRead, recordsInserted, recordsStaged, ctx.getAdxWindowCount(),
                        metrics.jvmUsedMemoryMb(), metrics.jvmTotalMemoryMb());
            }
        } catch (Exception ex) {
            // Heartbeat is best-effort; swallow to never break the run.
            LogHelper.debug(ctx, "EXEC_LOG_HEARTBEAT", "Failed to write heartbeat: {}", ex.getMessage());
        }
    }

    /**
     * Reaper: transitions runs stuck in STARTED whose heartbeat (or STARTED_AT, if the run never
     * heartbeat) is older than {@code staleThreshold} to a terminal ABORTED state with a diagnostic
     * message. A STARTED row this stale cannot be a live run — every run is bounded by the
     * max-duration guardrail — so it was interrupted (OOM / pod restart / SIGKILL) before it could
     * log its own END. Idempotent (only matches STATUS='STARTED') and cluster-safe.
     *
     * @return number of runs reaped
     */
    @Transactional
    public int markOrphanedRunsAborted(Duration staleThreshold) {
        if (!enabled || staleThreshold == null || staleThreshold.isZero() || staleThreshold.isNegative()) {
            return 0;
        }
        // The reaper runs on the shared @Scheduled thread; bound its lock wait and statement time so
        // it can never hang the scheduler (best-effort SET LOCAL, proceeds if it fails).
        try {
            jdbcTemplate.execute("SET LOCAL lock_timeout = '10s'");
            jdbcTemplate.execute("SET LOCAL statement_timeout = '30s'");
        } catch (Exception ignored) {
            // proceed without local timeouts
        }
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        OffsetDateTime threshold = now.minus(staleThreshold);
        String message = "Marked ABORTED by reaper: no heartbeat/END for more than " + staleThreshold
                + "; the run cannot be alive (bounded by the max-duration guardrail) so its process was "
                + "terminated (OOM / pod restart / SIGKILL). Check Kubernetes events for the pod in INSTANCE_ID.";
        int aborted = jdbcTemplate.update(
                "UPDATE " + schema + ".INGEST_EXECUTION_LOG " +
                "SET STATUS = 'ABORTED', ENDED_AT = ?, ERROR_CODE = 'RUN_INTERRUPTED', ERROR_MESSAGE = ?, " +
                "DURATION_MS = COALESCE((EXTRACT(EPOCH FROM (?::TIMESTAMPTZ - STARTED_AT)) * 1000)::BIGINT, 0) " +
                "WHERE STATUS = 'STARTED' AND COALESCE(LAST_HEARTBEAT_AT, STARTED_AT) < ?",
                now, message, now, threshold);
        if (aborted > 0) {
            log.warn("[phase=REAP_ORPHANED_RUNS] marked {} interrupted run(s) as ABORTED "
                            + "(staleThreshold={}, cutoff={}) — likely OOM/pod-restart; see INSTANCE_ID on the rows",
                    aborted, staleThreshold, threshold);
        }
        return aborted;
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
            RuntimeMetrics metrics = captureRuntimeMetrics();
            jdbcTemplate.update(
                    "UPDATE " + schema + ".INGEST_EXECUTION_LOG " +
                    "SET STATUS = 'COMPLETED', ENDED_AT = ?, " +
                    "RECORDS_READ = ?, RECORDS_TRANSFORMED = ?, RECORDS_INSERTED = ?, " +
                    "RECORDS_DISCARDED = ?, RECORDS_STAGED = ?, RECORDS_CONSOLIDATED = ?, QUERY_COUNT = ?, OPERATION_COUNT = ?, END_REASON = ?, " +
                    "ADX_QUERY_DURATION_MS = ?, INGESTOR_LOGIC_DURATION_MS = ?, POSTGRES_INSERT_DURATION_MS = ?, " +
                    "ANAGRAFICA_DURATION_MS = ?, FK_POSITION_DURATION_MS = ?, FK_TOKEN_DURATION_MS = ?, " +
                    "PROCESS_CPU_LOAD_PCT = ?, JVM_USED_MEMORY_MB = ?, JVM_TOTAL_MEMORY_MB = ?, " +
                    "ANAGRAFICA_LOOKUP_COUNT = ?, POSITION_LOOKUP_COUNT = ?, TOKEN_LOOKUP_COUNT = ?, " +
                    "CACHE_HIT_COUNT = ?, CACHE_MISS_COUNT = ?, ADX_WINDOW_COUNT = ?, ADX_ATTEMPT_COUNT = ?, EMPTY_WINDOW_COUNT = ?, " +
                    "WINDOW_PROFILE = ?, RESOLVED_MAX_DURATION_MS = ?, " +
                    "DURATION_MS = COALESCE((EXTRACT(EPOCH FROM (?::TIMESTAMPTZ - STARTED_AT)) * 1000)::BIGINT, 0) " +
                    "WHERE RUN_ID = ? AND ENTITY_NAME = ?",
                    now, recordsRead, recordsTransformed, recordsInserted,
                    recordsDiscarded, recordsStaged, ctx.getRecordsConsolidated(), queryCount, operationCount, endReason,
                    ctx.getAdxQueryDurationMs(), ctx.getIngestorLogicDurationMs(), ctx.getPostgresInsertDurationMs(),
                    ctx.getAnagraficaDurationMs(), ctx.getFkPositionDurationMs(), ctx.getFkTokenDurationMs(),
                    metrics.processCpuLoadPct(), metrics.jvmUsedMemoryMb(), metrics.jvmTotalMemoryMb(),
                    ctx.getAnagraficaLookupCount(), ctx.getPositionLookupCount(), ctx.getTokenLookupCount(),
                    ctx.getCacheHitCount(), ctx.getCacheMissCount(), ctx.getAdxWindowCount(), ctx.getAdxAttemptCount(), ctx.getEmptyWindowCount(),
                    ctx.getWindowProfile(), ctx.getResolvedMaxDurationMs(),
                    now, ctx.getRunId(), ctx.getEntityName());
            LogHelper.info(ctx, "EXEC_LOG_END",
                    "Execution log completed: queryCount={}, operationCount={}, read={}, transformed={}, inserted={}, discarded={}, staged={}, consolidated={}, " +
                            "adxQueryDurationMs={}, ingestorLogicDurationMs={}, postgresInsertDurationMs={}, anagraficaDurationMs={}, fkPositionDurationMs={}, fkTokenDurationMs={}, " +
                            "processCpuLoadPct={}, jvmUsedMemoryMb={}, jvmTotalMemoryMb={}, endReason={}",
                    queryCount, operationCount, recordsRead, recordsTransformed, recordsInserted, recordsDiscarded, recordsStaged, ctx.getRecordsConsolidated(),
                    ctx.getAdxQueryDurationMs(), ctx.getIngestorLogicDurationMs(), ctx.getPostgresInsertDurationMs(),
                    ctx.getAnagraficaDurationMs(), ctx.getFkPositionDurationMs(), ctx.getFkTokenDurationMs(),
                    metrics.processCpuLoadPct(), metrics.jvmUsedMemoryMb(), metrics.jvmTotalMemoryMb(), endReason);
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
        logFailed(ctx, null, errorCode, errorMessage, recordsRead, recordsTransformed, recordsInserted,
                recordsDiscarded, recordsStaged, queryCount, operationCount);
    }

    /**
     * Same as {@link #logFailed(RunContext, String, String, long, long, long, long, long, long, long)}
     * but also records the job name. Used when the failure is caught by the Quartz job wrapper: there
     * the row may not exist yet (the job failed before the runner could create it), so the fallback
     * INSERT is the only trace of the error and must be self-describing.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void logFailed(RunContext ctx, String jobName, String errorCode, String errorMessage,
                          long recordsRead, long recordsTransformed, long recordsInserted,
                          long recordsDiscarded, long recordsStaged, long queryCount, long operationCount) {
        if (!enabled) return;
        try {
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
            RuntimeMetrics metrics = captureRuntimeMetrics();
            int rows = jdbcTemplate.update(
                    "UPDATE " + schema + ".INGEST_EXECUTION_LOG " +
                    "SET STATUS = 'FAILED', ENDED_AT = ?, ERROR_CODE = ?, ERROR_MESSAGE = ?, " +
                    "RECORDS_READ = ?, RECORDS_TRANSFORMED = ?, RECORDS_INSERTED = ?, " +
                    "RECORDS_DISCARDED = ?, RECORDS_STAGED = ?, QUERY_COUNT = ?, OPERATION_COUNT = ?, " +
                    "ADX_QUERY_DURATION_MS = ?, INGESTOR_LOGIC_DURATION_MS = ?, POSTGRES_INSERT_DURATION_MS = ?, " +
                    "ANAGRAFICA_DURATION_MS = ?, FK_POSITION_DURATION_MS = ?, FK_TOKEN_DURATION_MS = ?, " +
                    "PROCESS_CPU_LOAD_PCT = ?, JVM_USED_MEMORY_MB = ?, JVM_TOTAL_MEMORY_MB = ?, " +
                    "ANAGRAFICA_LOOKUP_COUNT = ?, POSITION_LOOKUP_COUNT = ?, TOKEN_LOOKUP_COUNT = ?, " +
                    "CACHE_HIT_COUNT = ?, CACHE_MISS_COUNT = ?, ADX_WINDOW_COUNT = ?, ADX_ATTEMPT_COUNT = ?, EMPTY_WINDOW_COUNT = ?, " +
                    "WINDOW_PROFILE = ?, RESOLVED_MAX_DURATION_MS = ?, " +
                    "DURATION_MS = COALESCE((EXTRACT(EPOCH FROM (?::TIMESTAMPTZ - STARTED_AT)) * 1000)::BIGINT, 0) " +
                    "WHERE RUN_ID = ? AND ENTITY_NAME = ?",
                    now, errorCode, errorMessage,
                    recordsRead, recordsTransformed, recordsInserted,
                    recordsDiscarded, recordsStaged, queryCount, operationCount,
                    ctx.getAdxQueryDurationMs(), ctx.getIngestorLogicDurationMs(), ctx.getPostgresInsertDurationMs(),
                    ctx.getAnagraficaDurationMs(), ctx.getFkPositionDurationMs(), ctx.getFkTokenDurationMs(),
                    metrics.processCpuLoadPct(), metrics.jvmUsedMemoryMb(), metrics.jvmTotalMemoryMb(),
                    ctx.getAnagraficaLookupCount(), ctx.getPositionLookupCount(), ctx.getTokenLookupCount(),
                    ctx.getCacheHitCount(), ctx.getCacheMissCount(), ctx.getAdxWindowCount(), ctx.getAdxAttemptCount(), ctx.getEmptyWindowCount(),
                    ctx.getWindowProfile(), ctx.getResolvedMaxDurationMs(),
                    now, ctx.getRunId(), ctx.getEntityName());
            if (rows == 0) {
                // Nessun record trovato: crea l'entry FAILED direttamente
                jdbcTemplate.update(
                        "INSERT INTO " + schema + ".INGEST_EXECUTION_LOG " +
                        "(RUN_ID, ENTITY_NAME, JOB_NAME, INSTANCE_ID, STATUS, STARTED_AT, ENDED_AT, ERROR_CODE, ERROR_MESSAGE, " +
                        "RECORDS_READ, RECORDS_TRANSFORMED, RECORDS_INSERTED, RECORDS_DISCARDED, RECORDS_STAGED, QUERY_COUNT, OPERATION_COUNT, " +
                        "ADX_QUERY_DURATION_MS, INGESTOR_LOGIC_DURATION_MS, POSTGRES_INSERT_DURATION_MS, " +
                        "ANAGRAFICA_DURATION_MS, FK_POSITION_DURATION_MS, FK_TOKEN_DURATION_MS, PROCESS_CPU_LOAD_PCT, JVM_USED_MEMORY_MB, JVM_TOTAL_MEMORY_MB, " +
                        "ANAGRAFICA_LOOKUP_COUNT, POSITION_LOOKUP_COUNT, TOKEN_LOOKUP_COUNT, CACHE_HIT_COUNT, CACHE_MISS_COUNT, ADX_WINDOW_COUNT, ADX_ATTEMPT_COUNT, EMPTY_WINDOW_COUNT, " +
                        "WINDOW_PROFILE, RESOLVED_MAX_DURATION_MS, " +
                        "DURATION_MS, CREATED_AT) " +
                        "VALUES (?, ?, ?, ?, 'FAILED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)",
                        ctx.getRunId(), ctx.getEntityName(), jobName, INSTANCE_ID, now, now, errorCode, errorMessage,
                        recordsRead, recordsTransformed, recordsInserted, recordsDiscarded, recordsStaged, queryCount, operationCount,
                        ctx.getAdxQueryDurationMs(), ctx.getIngestorLogicDurationMs(), ctx.getPostgresInsertDurationMs(),
                        ctx.getAnagraficaDurationMs(), ctx.getFkPositionDurationMs(), ctx.getFkTokenDurationMs(),
                        metrics.processCpuLoadPct(), metrics.jvmUsedMemoryMb(), metrics.jvmTotalMemoryMb(),
                        ctx.getAnagraficaLookupCount(), ctx.getPositionLookupCount(), ctx.getTokenLookupCount(),
                        ctx.getCacheHitCount(), ctx.getCacheMissCount(), ctx.getAdxWindowCount(), ctx.getAdxAttemptCount(), ctx.getEmptyWindowCount(),
                        ctx.getWindowProfile(), ctx.getResolvedMaxDurationMs(),
                        now);
                LogHelper.error(ctx, "EXEC_LOG_END",
                        "Execution log created as FAILED: errorCode={}, errorMessage={}, queryCount={}, operationCount={}, read={}, transformed={}, inserted={}, discarded={}, staged={}, " +
                                "adxQueryDurationMs={}, ingestorLogicDurationMs={}, postgresInsertDurationMs={}, anagraficaDurationMs={}, fkPositionDurationMs={}, fkTokenDurationMs={}, " +
                                "processCpuLoadPct={}, jvmUsedMemoryMb={}, jvmTotalMemoryMb={}",
                        errorCode, errorMessage, queryCount, operationCount,
                        recordsRead, recordsTransformed, recordsInserted, recordsDiscarded, recordsStaged,
                        ctx.getAdxQueryDurationMs(), ctx.getIngestorLogicDurationMs(), ctx.getPostgresInsertDurationMs(),
                        ctx.getAnagraficaDurationMs(), ctx.getFkPositionDurationMs(), ctx.getFkTokenDurationMs(),
                        metrics.processCpuLoadPct(), metrics.jvmUsedMemoryMb(), metrics.jvmTotalMemoryMb());
            } else {
                LogHelper.error(ctx, "EXEC_LOG_END",
                        "Execution log failed: errorCode={}, errorMessage={}, queryCount={}, operationCount={}, read={}, transformed={}, inserted={}, discarded={}, staged={}, " +
                                "adxQueryDurationMs={}, ingestorLogicDurationMs={}, postgresInsertDurationMs={}, anagraficaDurationMs={}, fkPositionDurationMs={}, fkTokenDurationMs={}, " +
                                "processCpuLoadPct={}, jvmUsedMemoryMb={}, jvmTotalMemoryMb={}",
                        errorCode, errorMessage, queryCount, operationCount,
                        recordsRead, recordsTransformed, recordsInserted, recordsDiscarded, recordsStaged,
                        ctx.getAdxQueryDurationMs(), ctx.getIngestorLogicDurationMs(), ctx.getPostgresInsertDurationMs(),
                        ctx.getAnagraficaDurationMs(), ctx.getFkPositionDurationMs(), ctx.getFkTokenDurationMs(),
                        metrics.processCpuLoadPct(), metrics.jvmUsedMemoryMb(), metrics.jvmTotalMemoryMb());
            }
        } catch (Exception ex) {
            LogHelper.error(ctx, "EXEC_LOG_END", "Failed to log execution failure: {}", ex.getMessage());
            log.error("Failed to log execution failure", ex);
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

    /**
     * Best-effort snapshot of heap and process-CPU usage. Runtime-metric capture must NEVER throw:
     * the platform MXBeans are not guaranteed on every JVM/runtime (e.g. {@code getProcessCpuLoad()}
     * can throw on some JDK distributions), and a metrics failure must not prevent the primary
     * execution-log write. Any failure degrades gracefully to zero for that metric.
     */
    private RuntimeMetrics captureRuntimeMetrics() {
        long usedMemoryMb = 0L;
        long totalMemoryMb = 0L;
        try {
            MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
            MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
            usedMemoryMb = toMegabytes(heapUsage != null ? heapUsage.getUsed() : 0L);
            totalMemoryMb = toMegabytes(heapUsage != null ? heapUsage.getCommitted() : 0L);
        } catch (Exception ex) {
            log.debug("Heap memory metrics capture failed, defaulting to 0: {}", ex.toString());
        }

        double processCpuLoadPct = 0.0;
        try {
            java.lang.management.OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
            if (osBean instanceof com.sun.management.OperatingSystemMXBean sunOsBean) {
                double cpuLoad = sunOsBean.getProcessCpuLoad();
                if (cpuLoad >= 0.0d) {
                    processCpuLoadPct = roundToTwoDecimals(cpuLoad * 100.0d);
                }
            }
        } catch (Exception ex) {
            log.debug("Process CPU metrics capture failed, defaulting to 0: {}", ex.toString());
        }

        return new RuntimeMetrics(processCpuLoadPct, usedMemoryMb, totalMemoryMb);
    }

    private long toMegabytes(long bytes) {
        if (bytes <= 0) {
            return 0L;
        }
        return bytes / (1024L * 1024L);
    }

    private double roundToTwoDecimals(double value) {
        return Math.round(value * 100.0d) / 100.0d;
    }

    private record RuntimeMetrics(double processCpuLoadPct, long jvmUsedMemoryMb, long jvmTotalMemoryMb) {
    }
}
