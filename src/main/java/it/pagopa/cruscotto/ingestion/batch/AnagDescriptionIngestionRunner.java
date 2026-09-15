package it.pagopa.cruscotto.ingestion.batch;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.service.adx.AdxClient;
import it.pagopa.cruscotto.ingestion.service.adx.AdxQueryResult;
import it.pagopa.cruscotto.ingestion.service.adx.AnagDescriptionAdxQueryBuilder;
import it.pagopa.cruscotto.ingestion.service.ExecutionLogService;
import it.pagopa.cruscotto.ingestion.util.ThrowableDetail;
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
    /** Safety net on a full sweep: bounds ADX usage even if a table grows unexpectedly. */
    private static final int MAX_BATCHES_PER_RUN = 200;
    /** Circuit breaker: stop querying a page once ADX keeps failing without a single success. */
    private static final int MAX_CONSECUTIVE_CHUNK_FAILURES = 3;
    /** Circuit breaker: give up on a table after this many pages that failed and resolved nothing. */
    private static final int MAX_CONSECUTIVE_FAILED_PAGES = 3;

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
        long lookupFailures = 0;
        try {
            List<LookupSpec> specs = List.of(
                    new LookupSpec("ANAG_PA_EMITTENTE", queryBuilder::buildPaEmittenteQuery),
                    new LookupSpec("ANAG_PSP", queryBuilder::buildPspQuery),
                    new LookupSpec("ANAG_INTERMEDIARIO_PA", queryBuilder::buildIntermediarioPaQuery),
                    new LookupSpec("ANAG_INTERMEDIARIO_PSP", queryBuilder::buildIntermediarioPspQuery));
            for (LookupSpec spec : specs) {
                RefreshResult result = refreshTable(ctx, spec);
                recordsRead += result.recordsRead();
                recordsInserted += result.recordsUpdated();
                lookupFailures += result.lookupFailures();
                queryCount += result.adxQueries();
                operationCount++;
            }

            // An ADX outage must not be reported as a successful refresh: individual chunk
            // failures are tolerated, but resolving nothing at all while every lookup failed
            // is a failure, not a no-op.
            if (recordsInserted == 0 && lookupFailures > 0) {
                throw new IllegalStateException("ADX description lookup failed for all "
                        + lookupFailures + " chunk(s): no description could be resolved");
            }

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

    /**
     * Sweeps one anagrafica table paging on an ID cursor.
     *
     * <p>Codes that ADX cannot resolve (not present in the master table) keep an empty
     * DESCRIPTION forever, so a head-anchored query returns them on every run and blocks
     * everything behind them. The cursor therefore advances past each page unconditionally:
     * unresolvable codes are skipped rather than re-read, and a run walks the whole table.
     */
    private RefreshResult refreshTable(RunContext ctx, LookupSpec spec) {
        long totalUpdated = 0;
        long totalRead = 0;
        // IDs come from sequences starting at 1, so 0 is a safe "before the first row" cursor.
        long cursorId = 0;
        int lookupFailures = 0;
        int adxQueries = 0;
        int batches = 0;
        int failedPages = 0;
        boolean truncated = false;

        while (true) {
            if (batches >= MAX_BATCHES_PER_RUN) {
                truncated = true;
                break;
            }
            List<AnagRow> missingRows = fetchMissingRows(spec, cursorId, SELECT_BATCH_SIZE);
            if (missingRows.isEmpty()) {
                break;
            }
            batches++;
            totalRead += missingRows.size();
            cursorId = missingRows.get(missingRows.size() - 1).id();

            Map<String, List<AnagRow>> rowsByCode = new LinkedHashMap<>();
            for (AnagRow row : missingRows) {
                rowsByCode.computeIfAbsent(row.codice(), key -> new ArrayList<>()).add(row);
            }

            LookupOutcome outcome = lookupDescriptions(ctx, spec, new ArrayList<>(rowsByCode.keySet()));
            lookupFailures += outcome.failedChunks();
            adxQueries += outcome.chunksAttempted();
            int updated = updateDescriptions(spec, rowsByCode, outcome.descriptions());
            totalUpdated += updated;

            log.info("jobTag=anagDescriptionJob CHECKPOINT runId={} entityName={} table={} missingRows={} resolvedRows={} updatedRows={} failedChunks={} cursorId={}",
                    ctx.getRunId(), ctx.getEntityName(), spec.tableName(), missingRows.size(),
                    outcome.descriptions().size(), updated, outcome.failedChunks(), cursorId);

            // Circuit breaker: when ADX is down every page costs several failing round-trips
            // (each with its own client-side retries). Give up early instead of walking the
            // whole table against a dead endpoint; the sweep resumes on the next run.
            if (outcome.failedChunks() > 0 && outcome.descriptions().isEmpty()) {
                if (++failedPages >= MAX_CONSECUTIVE_FAILED_PAGES) {
                    log.warn("jobTag=anagDescriptionJob LOOKUP_CIRCUIT_OPEN runId={} entityName={} table={} failedPages={}: aborting sweep",
                            ctx.getRunId(), ctx.getEntityName(), spec.tableName(), failedPages);
                    break;
                }
            } else {
                failedPages = 0;
            }

            if (missingRows.size() < SELECT_BATCH_SIZE) {
                break;
            }
        }

        if (truncated) {
            log.warn("jobTag=anagDescriptionJob BATCH_CAP runId={} entityName={} table={} batches={} cursorId={}: sweep truncated at cap",
                    ctx.getRunId(), ctx.getEntityName(), spec.tableName(), batches, cursorId);
        }
        if (totalUpdated == 0) {
            log.info("jobTag=anagDescriptionJob NOOP runId={} entityName={} table={} rowsScanned={} failedChunks={}",
                    ctx.getRunId(), ctx.getEntityName(), spec.tableName(), totalRead, lookupFailures);
        }
        return new RefreshResult(totalRead, totalUpdated, lookupFailures, adxQueries);
    }

    private List<AnagRow> fetchMissingRows(LookupSpec spec, long afterId, int limit) {
        String sql = "SELECT ID, CODICE FROM " + table(spec.tableName()) +
                " WHERE COALESCE(BTRIM(DESCRIPTION), '') = ''" +
                "   AND ID > :afterId" +
                " ORDER BY ID" +
                " LIMIT :limit";
        return jdbcTemplate.query(sql,
                new MapSqlParameterSource().addValue("afterId", afterId).addValue("limit", limit),
                (rs, rowNum) -> new AnagRow(rs.getLong("ID"), rs.getString("CODICE")));
    }

    private LookupOutcome lookupDescriptions(RunContext ctx, LookupSpec spec, List<String> codes) {
        Map<String, String> descriptions = new LinkedHashMap<>();
        int failedChunks = 0;
        int chunksAttempted = 0;
        int consecutiveFailures = 0;
        for (List<String> chunk : chunk(codes, ADX_LOOKUP_CHUNK_SIZE)) {
            if (consecutiveFailures >= MAX_CONSECUTIVE_CHUNK_FAILURES) {
                // ADX looks unavailable: stop burning round-trips on the remaining chunks.
                log.warn("jobTag=anagDescriptionJob LOOKUP_ABORTED runId={} table={} consecutiveFailures={}",
                        ctx.getRunId(), spec.tableName(), consecutiveFailures);
                break;
            }
            chunksAttempted++;
            String query = spec.queryBuilder().apply(chunk);
            AdxQueryResult result;
            try {
                result = adxClient.executeQuery(ctx, ingestionConfig.getAdx().getDatabase(), query);
            } catch (Exception exception) {
                // One failing chunk must not abort the sweep: the job is idempotent and the
                // codes it could not resolve are picked up again on the next run.
                failedChunks++;
                consecutiveFailures++;
                log.warn("jobTag=anagDescriptionJob LOOKUP_FAILED runId={} table={} codes={}: {}",
                        ctx.getRunId(), spec.tableName(), chunk.size(), ThrowableDetail.format(exception));
                continue;
            }
            if (!result.isSuccess()) {
                failedChunks++;
                consecutiveFailures++;
                log.warn("jobTag=anagDescriptionJob LOOKUP_FAILED runId={} table={} codes={}: {}",
                        ctx.getRunId(), spec.tableName(), chunk.size(), result.getError());
                continue;
            }
            if (result.getData() == null || result.getData().isEmpty()) {
                consecutiveFailures = 0;
                continue;
            }
            consecutiveFailures = 0;
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
        return new LookupOutcome(descriptions, failedChunks, chunksAttempted);
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

    private record RefreshResult(long recordsRead, long recordsUpdated, int lookupFailures, int adxQueries) {
    }

    private record LookupOutcome(Map<String, String> descriptions, int failedChunks, int chunksAttempted) {
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
