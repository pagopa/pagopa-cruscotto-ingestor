package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * JDBC persistence for perimeter generation: reads the instance filter definition and reads/writes
 * {@code search_perimeter_file} metadata. Kept on plain JDBC (consistent with the rest of the
 * ingestor) so the SERT reads and the metadata writes share the same {@code NamedParameterJdbcTemplate}.
 */
@Slf4j
@Repository
public class PerimeterFileRepository {

    static final String SOURCE_GENERATED = "GENERATED_FROM_FILTERS";
    static final String SOURCE_UPLOADED = "USER_UPLOADED";
    private static final String VALIDATION_VALID = "VALID";

    private final NamedParameterJdbcTemplate jdbc;
    private final String schema;

    private final RowMapper<PerimeterFileMetadata> metadataMapper = this::mapMetadata;

    public PerimeterFileRepository(NamedParameterJdbcTemplate jdbc, DbSchemaConfig dbSchemaConfig) {
        this.jdbc = jdbc;
        this.schema = dbSchemaConfig.getSchemaName();
    }

    /** Returns the raw {@code filter_json} associated with the instance, or empty when absent. */
    public Optional<String> readFilterJson(UUID instanceId) {
        String sql = "SELECT filter_json::text FROM " + schema + ".search_filter WHERE instance_id = :instanceId";
        try {
            String json = jdbc.queryForObject(sql, new MapSqlParameterSource("instanceId", instanceId), String.class);
            return Optional.ofNullable(json);
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    /** Returns the latest perimeter file generated from filters for the instance, if any. */
    public Optional<PerimeterFileMetadata> findLatestGenerated(UUID instanceId) {
        return findLatestBySource(instanceId, SOURCE_GENERATED);
    }

    /** Returns the latest user-uploaded perimeter file for the instance, if any. */
    public Optional<PerimeterFileMetadata> findLatestUploaded(UUID instanceId) {
        return findLatestBySource(instanceId, SOURCE_UPLOADED);
    }

    private Optional<PerimeterFileMetadata> findLatestBySource(UUID instanceId, String source) {
        String sql = "SELECT id, instance_id, execution_id, source, template, file_name, file_path, rows_count, validation_status, created_at"
            + " FROM " + schema + ".search_perimeter_file"
            + " WHERE instance_id = :instanceId AND source = :source"
            + " ORDER BY created_at DESC"
            + " LIMIT 1";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("instanceId", instanceId)
            .addValue("source", source);
        List<PerimeterFileMetadata> rows = jdbc.query(sql, params, metadataMapper);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    /** Inserts the metadata of a newly generated perimeter CSV and returns it. */
    public PerimeterFileMetadata insertGenerated(UUID instanceId, UUID executionId, String template, String fileName, String filePath, long rows) {
        UUID id = UUID.randomUUID();
        OffsetDateTime createdAt = OffsetDateTime.now();
        String sql = "INSERT INTO " + schema + ".search_perimeter_file"
            + " (id, instance_id, execution_id, source, template, file_name, file_path, rows_count, validation_status, created_at)"
            + " VALUES (:id, :instanceId, :executionId, :source, :template, :fileName, :filePath, :rows, :validationStatus, :createdAt)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", id)
            .addValue("instanceId", instanceId)
            .addValue("executionId", executionId)
            .addValue("source", SOURCE_GENERATED)
            .addValue("template", template)
            .addValue("fileName", fileName)
            .addValue("filePath", filePath)
            .addValue("rows", rows)
            .addValue("validationStatus", VALIDATION_VALID)
            .addValue("createdAt", createdAt);
        jdbc.update(sql, params);
        return new PerimeterFileMetadata(id, instanceId, executionId, SOURCE_GENERATED, template, fileName, filePath, rows, VALIDATION_VALID, createdAt);
    }

    private PerimeterFileMetadata mapMetadata(ResultSet rs, int rowNum) throws SQLException {
        OffsetDateTime createdAt = rs.getObject("created_at", OffsetDateTime.class);
        return new PerimeterFileMetadata(
            rs.getObject("id", UUID.class),
            rs.getObject("instance_id", UUID.class),
            rs.getObject("execution_id", UUID.class),
            rs.getString("source"),
            rs.getString("template"),
            rs.getString("file_name"),
            rs.getString("file_path"),
            rs.getLong("rows_count"),
            rs.getString("validation_status"),
            createdAt
        );
    }
}
