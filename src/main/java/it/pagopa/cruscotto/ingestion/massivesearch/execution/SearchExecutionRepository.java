package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * JDBC access to {@code search_execution} lifecycle rows.
 */
@Repository
public class SearchExecutionRepository {

    private static final int ERROR_CODE_MAX_LENGTH = 128;
    private static final String STUCK_ERROR_CODE = "STUCK_EXECUTION";
    private static final String STUCK_ERROR_MESSAGE = "Execution exceeded running timeout";

    private final NamedParameterJdbcTemplate jdbc;
    private final String schema;

    public SearchExecutionRepository(NamedParameterJdbcTemplate jdbc, DbSchemaConfig dbSchemaConfig) {
        this.jdbc = jdbc;
        this.schema = dbSchemaConfig.getSchemaName();
    }

    /** Inserts a new execution in {@code PENDING} state and returns its id. */
    public UUID insertPending(UUID instanceId) {
        UUID id = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "INSERT INTO " + schema + ".search_execution"
            + " (id, instance_id, status, created_at, updated_at)"
            + " VALUES (:id, :instanceId, :status, :now, :now)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", id)
            .addValue("instanceId", instanceId)
            .addValue("status", ExecutionStatus.PENDING.name())
            .addValue("now", now);
        jdbc.update(sql, params);
        return id;
    }

    public void markRunning(UUID executionId) {
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "UPDATE " + schema + ".search_execution"
            + " SET status = :status, started_at = :now, updated_at = :now"
            + " WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", executionId)
            .addValue("status", ExecutionStatus.RUNNING.name())
            .addValue("now", now);
        jdbc.update(sql, params);
    }

    public void markCompleted(UUID executionId, long totalInputRows, long processedRows, int generatedFiles) {
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "UPDATE " + schema + ".search_execution"
            + " SET status = :status, completed_at = :now, updated_at = :now,"
            + " total_input_rows = :totalInputRows, processed_rows = :processedRows, generated_files = :generatedFiles"
            + " WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", executionId)
            .addValue("status", ExecutionStatus.COMPLETED.name())
            .addValue("now", now)
            .addValue("totalInputRows", totalInputRows)
            .addValue("processedRows", processedRows)
            .addValue("generatedFiles", generatedFiles);
        jdbc.update(sql, params);
    }

    public void markFailed(UUID executionId, String errorCode, String errorMessage) {
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "UPDATE " + schema + ".search_execution"
            + " SET status = :status, completed_at = :now, updated_at = :now,"
            + " error_code = :errorCode, error_message = :errorMessage"
            + " WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", executionId)
            .addValue("status", ExecutionStatus.FAILED.name())
            .addValue("now", now)
            .addValue("errorCode", truncate(errorCode))
            .addValue("errorMessage", errorMessage);
        jdbc.update(sql, params);
    }

    private String truncate(String value) {
        if (value == null) {
            return null;
        }
        return value.length() <= ERROR_CODE_MAX_LENGTH ? value : value.substring(0, ERROR_CODE_MAX_LENGTH);
    }

    /**
     * Finds executions still {@code RUNNING} whose {@code started_at} is older than {@code threshold}
     * (i.e. stuck: the JVM/pod likely died mid-processing).
     */
    public List<StuckExecutionRef> findStuckRunningExecutions(OffsetDateTime threshold) {
        String sql = "SELECT id, instance_id FROM " + schema + ".search_execution"
            + " WHERE status = :status AND started_at IS NOT NULL AND started_at < :threshold";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("status", ExecutionStatus.RUNNING.name())
            .addValue("threshold", threshold);
        return jdbc.query(sql, params, (rs, n) ->
            new StuckExecutionRef(rs.getObject("id", UUID.class), rs.getObject("instance_id", UUID.class)));
    }

    /**
     * Recovery-only transition: marks a stuck execution {@code FAILED} with the
     * {@code STUCK_EXECUTION} error code, but only if it is still {@code RUNNING} (so a normal
     * completion happening concurrently is never overwritten).
     *
     * @return {@code true} when the execution was RUNNING and got marked FAILED
     */
    public boolean recoverStuck(UUID executionId) {
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "UPDATE " + schema + ".search_execution"
            + " SET status = :failed, completed_at = :now, updated_at = :now,"
            + " error_code = :errorCode, error_message = :errorMessage"
            + " WHERE id = :id AND status = :running";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", executionId)
            .addValue("failed", ExecutionStatus.FAILED.name())
            .addValue("running", ExecutionStatus.RUNNING.name())
            .addValue("now", now)
            .addValue("errorCode", STUCK_ERROR_CODE)
            .addValue("errorMessage", STUCK_ERROR_MESSAGE);
        return jdbc.update(sql, params) == 1;
    }
}
