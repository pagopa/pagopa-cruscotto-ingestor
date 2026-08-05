package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * JDBC access to {@code search_result}. Only the latest result per instance is kept, so writes are
 * an upsert keyed on {@code instance_id}.
 */
@Repository
public class SearchResultRepository {

    private final NamedParameterJdbcTemplate jdbc;
    private final String schema;

    public SearchResultRepository(NamedParameterJdbcTemplate jdbc, DbSchemaConfig dbSchemaConfig) {
        this.jdbc = jdbc;
        this.schema = dbSchemaConfig.getSchemaName();
    }

    /** Upserts the latest result for the instance (keeps only the most recent ZIP). */
    public void upsertLatest(
        UUID instanceId,
        UUID executionId,
        String zipFileName,
        String zipFilePath,
        long zipSizeBytes,
        long positionRows,
        long attemptRows,
        long transferRows
    ) {
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "INSERT INTO " + schema + ".search_result"
            + " (instance_id, execution_id, zip_file_name, zip_file_path, zip_size_bytes,"
            + " position_rows, attempt_rows, transfer_rows, generated_at, updated_at)"
            + " VALUES (:instanceId, :executionId, :zipFileName, :zipFilePath, :zipSizeBytes,"
            + " :positionRows, :attemptRows, :transferRows, :now, :now)"
            + " ON CONFLICT (instance_id) DO UPDATE SET"
            + " execution_id = EXCLUDED.execution_id,"
            + " zip_file_name = EXCLUDED.zip_file_name,"
            + " zip_file_path = EXCLUDED.zip_file_path,"
            + " zip_size_bytes = EXCLUDED.zip_size_bytes,"
            + " position_rows = EXCLUDED.position_rows,"
            + " attempt_rows = EXCLUDED.attempt_rows,"
            + " transfer_rows = EXCLUDED.transfer_rows,"
            + " generated_at = EXCLUDED.generated_at,"
            + " updated_at = EXCLUDED.updated_at";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("instanceId", instanceId)
            .addValue("executionId", executionId)
            .addValue("zipFileName", zipFileName)
            .addValue("zipFilePath", zipFilePath)
            .addValue("zipSizeBytes", zipSizeBytes)
            .addValue("positionRows", positionRows)
            .addValue("attemptRows", attemptRows)
            .addValue("transferRows", transferRows)
            .addValue("now", now);
        jdbc.update(sql, params);
    }
}
