package it.pagopa.cruscotto.ingestion.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.StagingIngestError;
import it.pagopa.cruscotto.ingestion.entity.StagingStatus;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionErrorCode;
import it.pagopa.cruscotto.ingestion.repository.StagingIngestErrorRepository;
import it.pagopa.cruscotto.ingestion.service.ingestion.BulkWriter;
import it.pagopa.cruscotto.ingestion.service.ingestion.EntityTransformer;
import it.pagopa.cruscotto.ingestion.service.ingestion.MissingForeignKeyException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class StagingErrorService {

    private final StagingIngestErrorRepository stagingIngestErrorRepository;
    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbcTemplate;
    private final String schema;

    public StagingErrorService(
            StagingIngestErrorRepository stagingIngestErrorRepository,
            ObjectMapper objectMapper,
            JdbcTemplate jdbcTemplate,
            DbSchemaConfig dbSchemaConfig) {
        this.stagingIngestErrorRepository = stagingIngestErrorRepository;
        this.objectMapper = objectMapper;
        this.jdbcTemplate = jdbcTemplate;
        this.schema = dbSchemaConfig.getSchemaName();
    }

    /** INSERT singolo via native SQL — nessuna SELECT preliminare di JPA. */
    @Transactional
    public void insertError(RunContext ctx, String sourceKey, Map<String, Object> payload, Exception ex) {
        String sql = "INSERT INTO " + schema + ".STG_INGEST_ERROR " +
                "(RUN_ID, ENTITY_NAME, SOURCE_KEY, OPERATION_ID, PAYLOAD_JSON, ERROR_CODE, ERROR_MESSAGE, " +
                "CREATED_AT, STATUS, RETRY_COUNT, LAST_RETRY_AT, REF_NAV, REF_PA_EMITTENTE, REF_TOKEN) " +
                "VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, NULL, ?, ?, ?)";
        jdbcTemplate.update(sql,
                ctx.getRunId(), ctx.getEntityName(), sourceKey, ctx.getOperationId(),
                serializePayload(payload),
                resolveErrorCode(ex).name(),
                ex != null ? ex.getMessage() : null,
                OffsetDateTime.now(ZoneOffset.UTC),
                StagingStatus.PENDING.name(),
                0,
                extractRef(payload, "NAV", "nav"),
                extractRef(payload, "PA_EMITTENTE", "pa_emittente", "paEmittente"),
                extractRef(payload, "TOKEN", "token"));
        log.error("[runId={}][operationId={}][entity={}] ERROR - staged sourceKey={} error={}",
                ctx.getRunId(), ctx.getOperationId(), ctx.getEntityName(), sourceKey,
                ex != null ? ex.getMessage() : null);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public long insertErrorsBulk(RunContext ctx, List<StagingInputRecord> records) {
        if (records == null || records.isEmpty()) {
            return 0;
        }

        OffsetDateTime createdAt = OffsetDateTime.now(ZoneOffset.UTC);
        List<PreparedStagingRow> batch = new ArrayList<>(records.size());
        for (StagingInputRecord record : records) {
            Map<String, Object> payload = record.payload();
            batch.add(new PreparedStagingRow(
                    ctx.getRunId(),
                    ctx.getEntityName(),
                    record.sourceKey(),
                    ctx.getOperationId(),
                    serializePayload(payload),
                    resolveErrorCode(record.exception()).name(),
                    record.exception() != null ? record.exception().getMessage() : null,
                    createdAt,
                    StagingStatus.PENDING.name(),
                    0,
                    extractRef(payload, "NAV", "nav"),
                    extractRef(payload, "PA_EMITTENTE", "pa_emittente", "paEmittente"),
                    extractRef(payload, "TOKEN", "token")
            ));
        }

        long count = insertPreparedRowsBulk(batch);
        log.error("[runId={}][operationId={}][entity={}] ERROR - staged bulk records={} mode=bulk",
                ctx.getRunId(), ctx.getOperationId(), ctx.getEntityName(), count);
        return count;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public long insertDiscardedBulk(RunContext ctx, List<DiscardedInputRecord> records) {
        if (records == null || records.isEmpty()) {
            return 0;
        }

        OffsetDateTime createdAt = OffsetDateTime.now(ZoneOffset.UTC);
        List<PreparedStagingRow> batch = new ArrayList<>(records.size());
        for (DiscardedInputRecord record : records) {
            Map<String, Object> payload = record.payload();
            batch.add(new PreparedStagingRow(
                    ctx.getRunId(),
                    ctx.getEntityName(),
                    record.sourceKey(),
                    ctx.getOperationId(),
                    serializePayload(payload),
                    IngestionErrorCode.BUSINESS_RULE_DISCARDED.name(),
                    record.reason(),
                    createdAt,
                    StagingStatus.DONE.name(),
                    0,
                    extractRef(payload, "NAV", "nav"),
                    extractRef(payload, "PA_EMITTENTE", "pa_emittente", "paEmittente"),
                    extractRef(payload, "TOKEN", "token")
            ));
        }

        long count = insertPreparedRowsBulk(batch);
        log.info("[runId={}][operationId={}][entity={}] NOOP - traced discarded records={} mode=bulk",
                ctx.getRunId(), ctx.getOperationId(), ctx.getEntityName(), count);
        return count;
    }

    private long insertPreparedRowsBulk(List<PreparedStagingRow> batch) {
        if (batch == null || batch.isEmpty()) {
            return 0;
        }
        String sql = "INSERT INTO " + schema + ".STG_INGEST_ERROR " +
                "(RUN_ID, ENTITY_NAME, SOURCE_KEY, OPERATION_ID, PAYLOAD_JSON, ERROR_CODE, ERROR_MESSAGE, " +
                "CREATED_AT, STATUS, RETRY_COUNT, LAST_RETRY_AT, REF_NAV, REF_PA_EMITTENTE, REF_TOKEN) " +
                "VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                PreparedStagingRow row = batch.get(i);
                ps.setString(1, row.runId());
                ps.setString(2, row.entityName());
                ps.setString(3, row.sourceKey());
                ps.setString(4, row.operationId());
                ps.setString(5, row.payloadJson());
                ps.setString(6, row.errorCode());
                ps.setString(7, row.errorMessage());
                ps.setObject(8, row.createdAt());
                ps.setString(9, row.status());
                ps.setInt(10, row.retryCount());
                ps.setNull(11, Types.TIMESTAMP_WITH_TIMEZONE);
                ps.setString(12, row.refNav());
                ps.setString(13, row.refPaEmittente());
                ps.setString(14, row.refToken());
            }

            @Override
            public int getBatchSize() {
                return batch.size();
            }
        });
        return batch.size();
    }

    @Transactional(readOnly = true)
    public List<StagingIngestError> fetchPending(EntityName entity, int limit) {
        int pageSize = Math.max(1, limit);
        return stagingIngestErrorRepository.findByEntityNameAndStatusOrderByCreatedAtAsc(
                entity.name(),
                StagingStatus.PENDING,
                PageRequest.of(0, pageSize)
        );
    }

    /** UPDATE nativo per ID — elimina la SELECT di findById. */
    @Transactional
    public void markDone(Long id, String runId) {
        jdbcTemplate.update(
                "UPDATE " + schema + ".STG_INGEST_ERROR " +
                "SET STATUS = ?, RUN_ID = ?, LAST_RETRY_AT = ? " +
                "WHERE ID = ?",
                StagingStatus.DONE.name(), runId, OffsetDateTime.now(ZoneOffset.UTC), id);
    }

    /** UPDATE nativo per ID — RETRY_COUNT incrementato atomicamente in SQL. */
    @Transactional
    public void markRetryFailed(Long id, String runId, Exception ex) {
        jdbcTemplate.update(
                "UPDATE " + schema + ".STG_INGEST_ERROR " +
                "SET STATUS = ?, RUN_ID = ?, RETRY_COUNT = COALESCE(RETRY_COUNT, 0) + 1, " +
                "LAST_RETRY_AT = ?, ERROR_CODE = ?, ERROR_MESSAGE = ? " +
                "WHERE ID = ?",
                StagingStatus.PENDING.name(), runId, OffsetDateTime.now(ZoneOffset.UTC),
                resolveErrorCode(ex).name(), ex != null ? ex.getMessage() : null, id);
    }

    /** UPDATE nativo per ID — GREATEST gestisce il max del retry count in SQL. */
    @Transactional
    public void markParked(Long id, String runId, Exception ex, int retryCount) {
        jdbcTemplate.update(
                "UPDATE " + schema + ".STG_INGEST_ERROR " +
                "SET STATUS = ?, RUN_ID = ?, RETRY_COUNT = GREATEST(COALESCE(RETRY_COUNT, 0), ?), " +
                "LAST_RETRY_AT = ?, ERROR_CODE = ?, ERROR_MESSAGE = ? " +
                "WHERE ID = ?",
                StagingStatus.PARKED.name(), runId, retryCount, OffsetDateTime.now(ZoneOffset.UTC),
                resolveErrorCode(ex).name(), ex != null ? ex.getMessage() : null, id);
    }

    /**
     * Riporta in PENDING tutti i record PARKED la cui LAST_RETRY_AT (o CREATED_AT se null)
     * è più vecchia di {@code olderThan}, resettando retry_count = 0.
     *
     * Questo garantisce che nessuna riga ADX vada persa definitivamente: quando l'entità padre
     * ha ingested il timestamp mancante, il prossimo ciclo di reconciliation troverà il record
     * di nuovo PENDING e lo elaborerà con successo.
     *
     * @param olderThan  intervallo minimo di "parcheggio" prima di riprovare
     * @param batchSize  limite massimo di record da sbloccare per chiamata
     * @return numero di record riportati in PENDING
     */
    @Transactional
    public int unparkOldRecords(Duration olderThan, int batchSize) {
        OffsetDateTime threshold = OffsetDateTime.now(ZoneOffset.UTC).minus(olderThan);
        int updated = jdbcTemplate.update(
                "UPDATE " + schema + ".STG_INGEST_ERROR " +
                "SET STATUS = ?, RETRY_COUNT = 0, LAST_RETRY_AT = NULL " +
                "WHERE STATUS = ? " +
                "  AND COALESCE(LAST_RETRY_AT, CREATED_AT) <= ? " +
                "  AND ID IN (" +
                "    SELECT ID FROM " + schema + ".STG_INGEST_ERROR " +
                "    WHERE STATUS = ? AND COALESCE(LAST_RETRY_AT, CREATED_AT) <= ? " +
                "    ORDER BY COALESCE(LAST_RETRY_AT, CREATED_AT) ASC " +
                "    LIMIT ?" +
                "  )",
                StagingStatus.PENDING.name(),
                StagingStatus.PARKED.name(), threshold,
                StagingStatus.PARKED.name(), threshold, batchSize);
        if (updated > 0) {
            log.info("UNPARK stagingRecords={} olderThan={} threshold={}", updated, olderThan, threshold);
        }
        return updated;
    }

    /**
     * Backward-compatible API used by existing ingestion code paths.
     */
    public void logError(String runId, String entityName, String recordData, String errorMessage) {
        RunContext ctx = new RunContext(entityName, runId, Instant.now());
        insertError(ctx, null, Collections.singletonMap("raw", recordData), new RuntimeException(errorMessage));
    }

    private String extractRef(Map<String, Object> payload, String... keys) {
        if (payload == null) return null;
        for (String key : keys) {
            Object value = payload.get(key);
            if (value == null) continue;
            String rendered = String.valueOf(value).trim();
            if (!rendered.isEmpty() && !rendered.equals("null")) return rendered;
        }
        return null;
    }

    private String serializePayload(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload == null ? Collections.emptyMap() : payload);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }

    private IngestionErrorCode resolveErrorCode(Exception ex) {
        if (ex == null) {
            return IngestionErrorCode.UNEXPECTED_ERROR;
        }
        if (ex instanceof MissingForeignKeyException) {
            return IngestionErrorCode.MISSING_FOREIGN_KEY;
        }
        if (ex instanceof EntityTransformer.TransformationException) {
            return IngestionErrorCode.TRANSFORMATION_ERROR;
        }
        if (ex instanceof BulkWriter.BulkWriteException) {
            return IngestionErrorCode.BULK_WRITE_ERROR;
        }
        if (ex instanceof IllegalArgumentException) {
            return IngestionErrorCode.VALIDATION_ERROR;
        }
        return IngestionErrorCode.UNEXPECTED_ERROR;
    }

    public record StagingInputRecord(String sourceKey, Map<String, Object> payload, Exception exception) {
    }

    public record DiscardedInputRecord(String sourceKey, Map<String, Object> payload, String reason) {
    }

    private record PreparedStagingRow(
            String runId,
            String entityName,
            String sourceKey,
            String operationId,
            String payloadJson,
            String errorCode,
            String errorMessage,
            OffsetDateTime createdAt,
            String status,
            int retryCount,
            String refNav,
            String refPaEmittente,
            String refToken) {
    }
}
