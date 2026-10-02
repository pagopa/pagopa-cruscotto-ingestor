package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * JDBC access to {@code search_execution_step}: the per-phase / per-window / per-retry log of a
 * single {@code search_execution}. Each phase opens a {@code RUNNING} row on begin and closes it as
 * {@code COMPLETED} or {@code FAILED}; {@code duration_ms} is computed server-side from
 * {@code started_at} so it is consistent regardless of the caller clock.
 */
@Slf4j
@Repository
public class SearchExecutionStepRepository {

    private static final int ERROR_CODE_MAX_LENGTH = 128;

    private final NamedParameterJdbcTemplate jdbc;
    private final String schema;
    private final ObjectMapper objectMapper;

    public SearchExecutionStepRepository(NamedParameterJdbcTemplate jdbc, DbSchemaConfig dbSchemaConfig,
                                         ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.schema = dbSchemaConfig.getSchemaName();
        this.objectMapper = objectMapper;
    }

    /** Opens a new {@code RUNNING} step and returns its id. {@code window} may be {@code null}. */
    public UUID begin(UUID executionId, UUID instanceId, StepPhase phase, int attemptNo, AnalysisWindow window) {
        UUID id = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "INSERT INTO " + schema + ".search_execution_step"
            + " (id, execution_id, instance_id, phase, attempt_no, status, window_from, window_to, started_at, created_at)"
            + " VALUES (:id, :executionId, :instanceId, :phase, :attemptNo, :status, :windowFrom, :windowTo, :now, :now)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", id)
            .addValue("executionId", executionId)
            .addValue("instanceId", instanceId)
            .addValue("phase", phase.name())
            .addValue("attemptNo", attemptNo)
            .addValue("status", "RUNNING")
            .addValue("windowFrom", window == null ? null : window.fromInclusive())
            .addValue("windowTo", window == null ? null : window.toExclusive())
            .addValue("now", now);
        jdbc.update(sql, params);
        return id;
    }

    /** Closes a step as {@code COMPLETED}, recording processed rows and elapsed time. */
    public void complete(UUID stepId, long rowsProcessed) {
        complete(stepId, rowsProcessed, null);
    }

    /**
     * Closes a step as {@code COMPLETED} also persisting its diagnostics.
     *
     * <p>Le metriche sono scritte nello stesso UPDATE che chiude lo step: un secondo statement
     * potrebbe non essere mai eseguito se l'esecuzione cade subito dopo, lasciando una fase chiusa ma
     * senza diagnostica proprio nei casi che interessa analizzare.</p>
     */
    public void complete(UUID stepId, long rowsProcessed, StepMetrics metrics) {
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "UPDATE " + schema + ".search_execution_step"
            + " SET status = :status, ended_at = :now, rows_processed = :rows,"
            + " duration_ms = CAST(EXTRACT(EPOCH FROM (:now - started_at)) * 1000 AS BIGINT),"
            + " metrics = CAST(:metrics AS jsonb)"
            + " WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", stepId)
            .addValue("status", "COMPLETED")
            .addValue("rows", rowsProcessed)
            .addValue("metrics", serialize(metrics))
            .addValue("now", now);
        jdbc.update(sql, params);
    }

    /** Closes a step as {@code FAILED}, recording the error and elapsed time. */
    public void fail(UUID stepId, String errorCode, String errorMessage) {
        fail(stepId, errorCode, errorMessage, null);
    }

    /**
     * Closes a step as {@code FAILED} also persisting the diagnostics raccolte fino al fallimento.
     *
     * <p>E' il caso piu' utile: su un report andato in timeout le metriche parziali (batch completati,
     * chiavi processate, batch piu' lento) dicono dove si e' fermato e con quale throughput.</p>
     */
    public void fail(UUID stepId, String errorCode, String errorMessage, StepMetrics metrics) {
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "UPDATE " + schema + ".search_execution_step"
            + " SET status = :status, ended_at = :now,"
            + " duration_ms = CAST(EXTRACT(EPOCH FROM (:now - started_at)) * 1000 AS BIGINT),"
            + " error_code = :errorCode, error_message = :errorMessage,"
            + " metrics = CAST(:metrics AS jsonb)"
            + " WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", stepId)
            .addValue("status", "FAILED")
            .addValue("now", now)
            .addValue("errorCode", truncate(errorCode))
            .addValue("errorMessage", errorMessage)
            .addValue("metrics", serialize(metrics));
        jdbc.update(sql, params);
    }

    /**
     * La diagnostica non deve mai far cadere un'esecuzione: se la serializzazione fallisce la colonna
     * resta nulla e l'esecuzione prosegue. Vale anche per le metriche vuote, per non scrivere
     * {@code {}} e far sembrare "misurato" cio' che non lo e'.
     */
    private String serialize(StepMetrics metrics) {
        if (metrics == null || metrics.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(metrics.asMap());
        } catch (RuntimeException | JsonProcessingException e) {
            log.warn("phase=STEP_METRICS_NOT_PERSISTED reason={}", e.getMessage());
            return null;
        }
    }

    private String truncate(String value) {
        if (value == null) {
            return null;
        }
        return value.length() <= ERROR_CODE_MAX_LENGTH ? value : value.substring(0, ERROR_CODE_MAX_LENGTH);
    }
}
