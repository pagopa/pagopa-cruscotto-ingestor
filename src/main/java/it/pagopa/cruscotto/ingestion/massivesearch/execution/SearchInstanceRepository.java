package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * JDBC access to {@code search_instance}, including the per-instance concurrency guard.
 */
@Repository
public class SearchInstanceRepository {

    private final NamedParameterJdbcTemplate jdbc;
    private final String schema;

    public SearchInstanceRepository(NamedParameterJdbcTemplate jdbc, DbSchemaConfig dbSchemaConfig) {
        this.jdbc = jdbc;
        this.schema = dbSchemaConfig.getSchemaName();
    }

    public Optional<SearchInstanceInfo> findById(UUID instanceId) {
        String sql = "SELECT id, name, input_type, status, selected_reports FROM " + schema
            + ".search_instance WHERE id = :id";
        List<SearchInstanceInfo> rows = jdbc.query(sql, new MapSqlParameterSource("id", instanceId), (rs, n) ->
            new SearchInstanceInfo(
                rs.getObject("id", UUID.class),
                rs.getString("name"),
                rs.getString("input_type"),
                rs.getString("status"),
                ReportSelection.parse(rs.getString("selected_reports"))));
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    /**
     * Returns the ids of the instances ready to be executed by the scanner ({@code status = 'READY'}),
     * deterministically ordered by {@code updated_at ASC} (oldest first) so no instance starves.
     *
     * @param limit maximum number of instances to pick up in a single scan
     * @return the executable instance ids, at most {@code limit}
     */
    public List<UUID> findExecutableInstances(int limit) {
        String sql = "SELECT id FROM " + schema + ".search_instance"
            + " WHERE status = 'READY'"
            + " ORDER BY updated_at ASC"
            + " LIMIT :limit";
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("limit", limit);
        return jdbc.query(sql, params, (rs, n) -> rs.getObject("id", UUID.class));
    }

    /**
     * Atomically acquires the per-instance execution lock by transitioning {@code READY -> RUNNING}.
     * This is the primary (DB-level) concurrency guard: only one caller can win the conditional
     * update, so two executions can never run for the same instance. Instances in any other state
     * ({@code DRAFT}, {@code RUNNING}, {@code EXECUTED}, {@code ARCHIVED}, {@code FAILED}) are never
     * picked up; a {@code FAILED} instance must be explicitly set back to {@code READY} by the API
     * Layer to be re-executed.
     *
     * @return {@code true} when the lock was acquired, {@code false} when the instance was not READY
     */
    public boolean tryAcquireRunning(UUID instanceId) {
        String sql = "UPDATE " + schema + ".search_instance"
            + " SET status = 'RUNNING', updated_at = :now"
            + " WHERE id = :id AND status = 'READY'";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", instanceId)
            .addValue("now", OffsetDateTime.now());
        return jdbc.update(sql, params) == 1;
    }

    public void markExecuted(UUID instanceId, UUID executionId) {
        String sql = "UPDATE " + schema + ".search_instance"
            + " SET status = 'EXECUTED', last_execution_id = :executionId, updated_at = :now"
            + " WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", instanceId)
            .addValue("executionId", executionId)
            .addValue("now", OffsetDateTime.now());
        jdbc.update(sql, params);
    }

    public void markFailed(UUID instanceId) {
        String sql = "UPDATE " + schema + ".search_instance"
            + " SET status = 'FAILED', updated_at = :now"
            + " WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", instanceId)
            .addValue("now", OffsetDateTime.now());
        jdbc.update(sql, params);
    }

    /**
     * Recovery-only transition: marks the instance {@code FAILED} only if it is still {@code RUNNING},
     * so a legitimately completed/re-armed instance is never overwritten by the stuck-execution
     * recovery.
     *
     * @return {@code true} when the instance was RUNNING and got marked FAILED
     */
    public boolean markFailedFromRunning(UUID instanceId) {
        String sql = "UPDATE " + schema + ".search_instance"
            + " SET status = 'FAILED', updated_at = :now"
            + " WHERE id = :id AND status = 'RUNNING'";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", instanceId)
            .addValue("now", OffsetDateTime.now());
        return jdbc.update(sql, params) == 1;
    }
}
