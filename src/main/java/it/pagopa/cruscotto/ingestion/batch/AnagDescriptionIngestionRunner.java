package it.pagopa.cruscotto.ingestion.batch;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.service.adx.AdxClient;
import it.pagopa.cruscotto.ingestion.service.adx.AdxQueryResult;
import it.pagopa.cruscotto.ingestion.service.adx.AnagDescriptionAdxQueryBuilder;
import it.pagopa.cruscotto.ingestion.service.ExecutionLogService;
import it.pagopa.cruscotto.ingestion.util.ThrowableDetail;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameters;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Service
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

    /**
     * Kill switch for the destructive reconciliation on ANAG_PA_EMITTENTE: when enabled, codes with
     * an empty DESCRIPTION that a successful lookup proves absent from the ADX master are deleted, so
     * the frontend filter registry stays aligned with the master. Defaults on; set to false in
     * configuration to disable deletion instantly without a code change.
     */
    private final boolean paEmittenteReconcileDelete;

    public AnagDescriptionIngestionRunner(NamedParameterJdbcTemplate jdbcTemplate,
                                          DbSchemaConfig dbSchemaConfig,
                                          AdxClient adxClient,
                                          IngestionConfig ingestionConfig,
                                          AnagDescriptionAdxQueryBuilder queryBuilder,
                                          ExecutionLogService executionLogService,
                                          @Value("${ingestion.anag-description.pa-emittente-reconcile-delete:true}")
                                          boolean paEmittenteReconcileDelete) {
        this.jdbcTemplate = jdbcTemplate;
        this.dbSchemaConfig = dbSchemaConfig;
        this.adxClient = adxClient;
        this.ingestionConfig = ingestionConfig;
        this.queryBuilder = queryBuilder;
        this.executionLogService = executionLogService;
        this.paEmittenteReconcileDelete = paEmittenteReconcileDelete;
    }

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
            // ANAG_PA_EMITTENTE reconciles against the master: with the reconcile flag on it uses the
            // existence query (no description filter) and deletes empty-description codes proven absent.
            LookupSpec paEmittente = paEmittenteReconcileDelete
                    ? new LookupSpec("ANAG_PA_EMITTENTE", queryBuilder::buildPaEmittenteReconcileQuery, true)
                    : new LookupSpec("ANAG_PA_EMITTENTE", queryBuilder::buildPaEmittenteQuery, false);
            List<LookupSpec> specs = List.of(
                    paEmittente,
                    new LookupSpec("ANAG_PSP", queryBuilder::buildPspQuery, false),
                    new LookupSpec("ANAG_INTERMEDIARIO_PA", queryBuilder::buildIntermediarioPaQuery, false),
                    new LookupSpec("ANAG_INTERMEDIARIO_PSP", queryBuilder::buildIntermediarioPspQuery, false));
            for (LookupSpec spec : specs) {
                RefreshResult result = refreshTable(ctx, spec);
                recordsRead += result.recordsRead();
                recordsInserted += result.recordsUpdated();
                recordsDiscarded += result.recordsDeleted();
                lookupFailures += result.lookupFailures();
                queryCount += result.adxQueries();
                operationCount++;
            }

            // An ADX outage must not be reported as a successful refresh: individual chunk failures
            // are tolerated, but making no progress at all (no description resolved, nothing deleted)
            // while every lookup failed is a failure, not a no-op.
            if (recordsInserted == 0 && recordsDiscarded == 0 && lookupFailures > 0) {
                throw new IllegalStateException("ADX anagrafica lookup failed for all "
                        + lookupFailures + " chunk(s): no description resolved and nothing reconciled");
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
        long totalDeleted = 0;
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

            // Reconciliation: codes a successful lookup proved absent from the master are removed, so
            // the registry never keeps phantom codes. Absence is collected only from successful chunks
            // (see lookupDescriptions), so an ADX failure can never trigger a deletion.
            int deleted = spec.reconcileDelete()
                    ? deleteAbsentRows(spec, rowsByCode, outcome.absentCodes())
                    : 0;
            totalDeleted += deleted;

            log.info("jobTag=anagDescriptionJob CHECKPOINT runId={} entityName={} table={} missingRows={} resolvedRows={} updatedRows={} deletedRows={} failedChunks={} cursorId={}",
                    ctx.getRunId(), ctx.getEntityName(), spec.tableName(), missingRows.size(),
                    outcome.descriptions().size(), updated, deleted, outcome.failedChunks(), cursorId);

            // Circuit breaker: when ADX is down every page costs several failing round-trips
            // (each with its own client-side retries). Give up early instead of walking the
            // whole table against a dead endpoint; the sweep resumes on the next run. A page that
            // resolved a description or deleted an absent code made progress and resets the counter.
            boolean pageMadeProgress = !outcome.descriptions().isEmpty() || deleted > 0;
            if (outcome.failedChunks() > 0 && !pageMadeProgress) {
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
        // Per-table summary: the single line to watch on the first reconcile run. A healthy run shows
        // updated>0 (descriptions filled) and a modest deleted; deleted spiking toward scanned with
        // updated~0 signals the master is unreachable or wrong (kill switch: reconcile-delete=false).
        log.info("jobTag=anagDescriptionJob TABLE_DONE runId={} entityName={} table={} scanned={} updated={} deleted={} failedChunks={} adxQueries={}",
                ctx.getRunId(), ctx.getEntityName(), spec.tableName(), totalRead, totalUpdated, totalDeleted, lookupFailures, adxQueries);
        return new RefreshResult(totalRead, totalUpdated, totalDeleted, lookupFailures, adxQueries);
    }

    private List<AnagRow> fetchMissingRows(LookupSpec spec, long afterId, int limit) {
        String sql = "SELECT ID, CODICE FROM " + table(spec.tableName()) +
                " WHERE COALESCE(BTRIM(DESCRIPTION), '') = ''" +
                "   AND ID > :afterId" +
                " ORDER BY ID" +
                " LIMIT :limit";
        return jdbcTemplate.query(sql,
                new MapSqlParameterSource().addValue("afterId", afterId).addValue("limit", limit),
                (rs, rowNum) -> {
                    String codice = rs.getString("CODICE");
                    // Normalize once: historical rows may carry surrounding whitespace (inserted before
                    // the ingestion-side trim). An untrimmed code would miss the master 'in~' match and
                    // be wrongly judged absent, so both the lookup and the delete decision use the
                    // trimmed value; deletion targets the row ID regardless.
                    return new AnagRow(rs.getLong("ID"), codice == null ? null : codice.trim());
                });
    }

    private LookupOutcome lookupDescriptions(RunContext ctx, LookupSpec spec, List<String> codes) {
        Map<String, String> descriptions = new LinkedHashMap<>();
        Set<String> absentCodes = new LinkedHashSet<>();
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
            consecutiveFailures = 0;
            // Existence set for reconciliation: every code the master returned, regardless of whether
            // it carries a description. A code present with an empty description is kept, not deleted.
            Set<String> returnedCodes = spec.reconcileDelete() ? new LinkedHashSet<>() : null;
            Map<String, Object> data = result.getData();
            if (data != null) {
                for (Object row : data.values()) {
                    if (!(row instanceof Map<?, ?> mapRow)) {
                        continue;
                    }
                    String codice = stringValue(mapRow.get("CODICE"));
                    if (codice == null) {
                        continue;
                    }
                    if (returnedCodes != null) {
                        returnedCodes.add(codice);
                    }
                    String description = stringValue(mapRow.get("DESCRIPTION"));
                    if (description != null) {
                        descriptions.put(codice, description);
                    }
                }
            }
            // Only a SUCCESSFUL chunk contributes to absence: a code queried here but not returned is
            // proven absent from the master and therefore deletable. Failed chunks never reach this.
            if (spec.reconcileDelete()) {
                for (String code : chunk) {
                    if (!returnedCodes.contains(code)) {
                        absentCodes.add(code);
                    }
                }
            }
        }
        return new LookupOutcome(descriptions, failedChunks, chunksAttempted, absentCodes);
    }

    private int deleteAbsentRows(LookupSpec spec, Map<String, List<AnagRow>> rowsByCode, Set<String> absentCodes) {
        if (absentCodes.isEmpty()) {
            return 0;
        }
        List<Long> ids = new ArrayList<>();
        for (String code : absentCodes) {
            List<AnagRow> rows = rowsByCode.get(code);
            if (rows == null) {
                continue;
            }
            for (AnagRow row : rows) {
                ids.add(row.id());
            }
        }
        if (ids.isEmpty()) {
            return 0;
        }
        return jdbcTemplate.update(
                "DELETE FROM " + table(spec.tableName()) + " WHERE ID IN (:ids)",
                new MapSqlParameterSource("ids", ids));
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

    private record RefreshResult(long recordsRead, long recordsUpdated, long recordsDeleted, int lookupFailures,
                                 int adxQueries) {
    }

    private record LookupOutcome(Map<String, String> descriptions, int failedChunks, int chunksAttempted,
                                 Set<String> absentCodes) {
    }

    private record LookupSpec(String tableName, QueryFactory queryFactory, boolean reconcileDelete) {
        java.util.function.Function<List<String>, String> queryBuilder() {
            return queryFactory::build;
        }
    }

    @FunctionalInterface
    private interface QueryFactory {
        String build(List<String> codes);
    }
}
