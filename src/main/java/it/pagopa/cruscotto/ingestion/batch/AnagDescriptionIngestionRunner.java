package it.pagopa.cruscotto.ingestion.batch;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.service.adx.AdxClient;
import it.pagopa.cruscotto.ingestion.service.adx.AdxQueryResult;
import it.pagopa.cruscotto.ingestion.service.adx.AnagDescriptionAdxQueryBuilder;
import it.pagopa.cruscotto.ingestion.service.ExecutionLogService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameters;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class AnagDescriptionIngestionRunner {
    private static final int SELECT_BATCH_SIZE = 500;
    private static final int ADX_LOOKUP_CHUNK_SIZE = 100;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final DbSchemaConfig dbSchemaConfig;
    private final AdxClient adxClient;
    private final IngestionConfig ingestionConfig;
    private final AnagDescriptionAdxQueryBuilder queryBuilder;
    private final ExecutionLogService executionLogService;

    public void run(JobParameters jobParameters) {
        String runId = jobParameters.getString(JobParameterKeys.RUN_ID);
        String entityName = EntityName.ANAG_DESCRIPTION_REFRESH.name();
        RunContext ctx = new RunContext(entityName, runId, Instant.now());

        log.info("jobTag=anagDescriptionJob START runId={} entityName={}", runId, entityName);
        executionLogService.logStarted(ctx, "batch-" + entityName);
        long recordsRead = 0;
        long recordsTransformed = 0;
        long recordsInserted = 0;
        long recordsDiscarded = 0;
        long recordsStaged = 0;
        long queryCount = 0;
        long operationCount = 0;
        try {
            RefreshResult result = refreshTable(ctx, new LookupSpec("ANAG_PA_EMITTENTE", queryBuilder::buildPaEmittenteQuery));
            recordsRead += result.recordsRead();
            recordsInserted += result.recordsUpdated();
            operationCount++;
            result = refreshTable(ctx, new LookupSpec("ANAG_PSP", queryBuilder::buildPspQuery));
            recordsRead += result.recordsRead();
            recordsInserted += result.recordsUpdated();
            operationCount++;
            result = refreshTable(ctx, new LookupSpec("ANAG_INTERMEDIARIO_PA", queryBuilder::buildIntermediarioPaQuery));
            recordsRead += result.recordsRead();
            recordsInserted += result.recordsUpdated();
            operationCount++;
            result = refreshTable(ctx, new LookupSpec("ANAG_INTERMEDIARIO_PSP", queryBuilder::buildIntermediarioPspQuery));
            recordsRead += result.recordsRead();
            recordsInserted += result.recordsUpdated();
            operationCount++;

            executionLogService.logCompleted(ctx, recordsRead, recordsTransformed, recordsInserted,
                    recordsDiscarded, recordsStaged, queryCount, operationCount, "COMPLETED");
        } catch (Throwable t) {
            executionLogService.logFailed(ctx, t.getClass().getSimpleName(), t.getMessage(),
                    recordsRead, recordsTransformed, recordsInserted, recordsDiscarded, recordsStaged, queryCount, operationCount);
            throw new RuntimeException(t);
        } finally {
            log.info("jobTag=anagDescriptionJob END runId={} entityName={}", runId, entityName);
        }
    }

    private RefreshResult refreshTable(RunContext ctx, LookupSpec spec) {
        long totalUpdated = 0;
        long totalRead = 0;
        while (true) {
            List<AnagRow> missingRows = fetchMissingRows(spec, SELECT_BATCH_SIZE);
            totalRead += missingRows.size();
            if (missingRows.isEmpty()) {
                if (totalUpdated == 0) {
                    log.info("jobTag=anagDescriptionJob NOOP runId={} entityName={} table={}",
                            ctx.getRunId(), ctx.getEntityName(), spec.tableName());
                }
                return new RefreshResult(totalRead, totalUpdated);
            }

            Map<String, List<AnagRow>> rowsByCode = new LinkedHashMap<>();
            for (AnagRow row : missingRows) {
                rowsByCode.computeIfAbsent(row.codice(), key -> new ArrayList<>()).add(row);
            }

            Map<String, String> descriptions = lookupDescriptions(ctx, spec, new ArrayList<>(rowsByCode.keySet()));
            if (descriptions.isEmpty()) {
                log.info("jobTag=anagDescriptionJob CHECKPOINT runId={} entityName={} table={} missingRows={} resolvedRows=0",
                        ctx.getRunId(), ctx.getEntityName(), spec.tableName(), missingRows.size());
                return new RefreshResult(totalRead, totalUpdated);
            }

            int updated = updateDescriptions(spec, rowsByCode, descriptions);
            totalUpdated += updated;

            log.info("jobTag=anagDescriptionJob CHECKPOINT runId={} entityName={} table={} missingRows={} resolvedRows={} updatedRows={}",
                    ctx.getRunId(), ctx.getEntityName(), spec.tableName(), missingRows.size(), descriptions.size(), updated);

            if (updated < missingRows.size()) {
                return new RefreshResult(totalRead, totalUpdated);
            }

            if (missingRows.size() < SELECT_BATCH_SIZE) {
                return new RefreshResult(totalRead, totalUpdated);
            }
        }
    }

    private List<AnagRow> fetchMissingRows(LookupSpec spec, int limit) {
        String sql = "SELECT ID, CODICE FROM " + table(spec.tableName()) +
                " WHERE COALESCE(BTRIM(DESCRIPTION), '') = ''" +
                " ORDER BY ID" +
                " LIMIT :limit";
        return jdbcTemplate.query(sql, new MapSqlParameterSource("limit", limit), (rs, rowNum) ->
                new AnagRow(rs.getLong("ID"), rs.getString("CODICE")));
    }

    private Map<String, String> lookupDescriptions(RunContext ctx, LookupSpec spec, List<String> codes) {
        Map<String, String> descriptions = new LinkedHashMap<>();
        for (List<String> chunk : chunk(codes, ADX_LOOKUP_CHUNK_SIZE)) {
            String query = spec.queryBuilder().apply(chunk);
            AdxQueryResult result = adxClient.executeQuery(ctx, ingestionConfig.getAdx().getDatabase(), query);
            if (!result.isSuccess()) {
                throw new IllegalStateException("ADX lookup failed for table=" + spec.tableName() + ": " + result.getError());
            }
            if (result.getData() == null || result.getData().isEmpty()) {
                continue;
            }
            result.getData().values().forEach(row -> {
                if (row instanceof Map<?, ?> mapRow) {
                    String codice = stringValue(mapRow.get("CODICE"));
                    String description = stringValue(mapRow.get("DESCRIPTION"));
                    if (codice != null && description != null && !description.isBlank()) {
                        descriptions.put(codice, description);
                    }
                }
            });
        }
        return descriptions;
    }

    private int updateDescriptions(LookupSpec spec, Map<String, List<AnagRow>> rowsByCode, Map<String, String> descriptions) {
        // Single batched UPDATE instead of one round-trip per row (former N+1).
        List<SqlParameterSource> batchParams = new ArrayList<>();
        for (Map.Entry<String, String> entry : descriptions.entrySet()) {
            List<AnagRow> rows = rowsByCode.get(entry.getKey());
            if (rows == null || rows.isEmpty()) {
                continue;
            }
            for (AnagRow row : rows) {
                batchParams.add(new MapSqlParameterSource()
                        .addValue("description", entry.getValue())
                        .addValue("id", row.id()));
            }
        }
        if (batchParams.isEmpty()) {
            return 0;
        }

        int[] counts = jdbcTemplate.batchUpdate(
                "UPDATE " + table(spec.tableName()) + " SET DESCRIPTION = :description WHERE ID = :id",
                batchParams.toArray(new SqlParameterSource[0]));

        int updated = 0;
        for (int count : counts) {
            // Defensive: a driver may report SUCCESS_NO_INFO (-2) for batched statements.
            updated += (count >= 0 ? count : 1);
        }
        return updated;
    }

    private String table(String tableName) {
        return dbSchemaConfig.getSchemaName() + "." + tableName;
    }

    private String stringValue(Object value) {
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? null : text;
    }

    private List<List<String>> chunk(List<String> values, int size) {
        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < values.size(); i += size) {
            chunks.add(values.subList(i, Math.min(i + size, values.size())));
        }
        return chunks;
    }

    private record AnagRow(long id, String codice) {
    }

    private record RefreshResult(long recordsRead, long recordsUpdated) {
    }

    private record LookupSpec(String tableName, QueryFactory queryFactory) {
        java.util.function.Function<List<String>, String> queryBuilder() {
            return queryFactory::build;
        }
    }

    @FunctionalInterface
    private interface QueryFactory {
        String build(List<String> codes);
    }
}
