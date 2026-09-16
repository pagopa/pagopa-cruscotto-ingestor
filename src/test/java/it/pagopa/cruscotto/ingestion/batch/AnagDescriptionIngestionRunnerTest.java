package it.pagopa.cruscotto.ingestion.batch;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.service.ExecutionLogService;
import it.pagopa.cruscotto.ingestion.service.adx.AdxClient;
import it.pagopa.cruscotto.ingestion.service.adx.AdxQueryResult;
import it.pagopa.cruscotto.ingestion.service.adx.AnagDescriptionAdxQueryBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression coverage for the description refresh sweep.
 *
 * <p>The job used to re-read the head of each table ({@code ORDER BY ID LIMIT 500}, no cursor) and
 * to return as soon as a single code could not be resolved. Codes absent from the ADX master keep
 * an empty DESCRIPTION forever, so they piled up at the head and permanently blocked every
 * legitimate code behind them.
 */
@ExtendWith(MockitoExtension.class)
class AnagDescriptionIngestionRunnerTest {

    private static final int SELECT_BATCH_SIZE = 500;
    private static final String PA_TABLE = "ANAG_PA_EMITTENTE";

    @Mock
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Mock
    private DbSchemaConfig dbSchemaConfig;

    @Mock
    private AdxClient adxClient;

    @Mock
    private IngestionConfig ingestionConfig;

    @Mock
    private AnagDescriptionAdxQueryBuilder queryBuilder;

    @Mock
    private ExecutionLogService executionLogService;

    /** Rows served by the fake SELECT, per table, in ID order. */
    private final Map<String, List<long[]>> rowIdsByTable = new LinkedHashMap<>();
    private final Map<Long, String> codiceById = new LinkedHashMap<>();

    /** Codes the fake ADX master can resolve; anything else is unresolvable. */
    private final Map<String, String> adxMaster = new LinkedHashMap<>();

    private final List<Long> observedCursors = new ArrayList<>();

    @BeforeEach
    void setUp() {
        lenient().when(dbSchemaConfig.getSchemaName()).thenReturn("sert_ingestor");
        IngestionConfig.AdxConfig adxConfig = new IngestionConfig.AdxConfig();
        adxConfig.setDatabase("db");
        lenient().when(ingestionConfig.getAdx()).thenReturn(adxConfig);
        lenient().when(queryBuilder.buildPaEmittenteQuery(any())).thenReturn("q");
        lenient().when(queryBuilder.buildPaEmittenteReconcileQuery(any())).thenReturn("q");
        lenient().when(queryBuilder.buildPspQuery(any())).thenReturn("q");
        lenient().when(queryBuilder.buildIntermediarioPaQuery(any())).thenReturn("q");
        lenient().when(queryBuilder.buildIntermediarioPspQuery(any())).thenReturn("q");

        stubSelect();
        stubBatchUpdate();
        stubDelete();
    }

    private AnagDescriptionIngestionRunner runner() {
        return runner(false);
    }

    private AnagDescriptionIngestionRunner runner(boolean reconcileDelete) {
        return new AnagDescriptionIngestionRunner(jdbcTemplate, dbSchemaConfig, adxClient,
                ingestionConfig, queryBuilder, executionLogService, reconcileDelete);
    }

    private JobParameters jobParameters() {
        return new JobParametersBuilder().addString(JobParameterKeys.RUN_ID, "run-anag").toJobParameters();
    }

    /** Serves paged rows honouring the ID cursor, the way the real query does. */
    @SuppressWarnings("unchecked")
    private void stubSelect() {
        lenient().when(jdbcTemplate.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
                .thenAnswer(invocation -> {
                    String sql = invocation.getArgument(0);
                    SqlParameterSource params = invocation.getArgument(1);
                    RowMapper<Object> mapper = invocation.getArgument(2);

                    String table = tableOf(sql);
                    long afterId = ((Number) params.getValue("afterId")).longValue();
                    int limit = ((Number) params.getValue("limit")).intValue();
                    if (PA_TABLE.equals(table)) {
                        observedCursors.add(afterId);
                    }

                    List<Object> page = new ArrayList<>();
                    for (long[] row : rowIdsByTable.getOrDefault(table, List.of())) {
                        long id = row[0];
                        if (id <= afterId) {
                            continue;
                        }
                        page.add(mapper.mapRow(resultSetFor(id, codiceById.get(id)), page.size()));
                        if (page.size() == limit) {
                            break;
                        }
                    }
                    return page;
                });
    }

    /** Single-call stub: avoids nested {@code when(...)} inside an answer. */
    private ResultSet resultSetFor(long id, String codice) {
        return mock(ResultSet.class, invocation -> switch (invocation.getMethod().getName()) {
            case "getLong" -> id;
            case "getString" -> codice;
            default -> null;
        });
    }

    /** UPDATE ... WHERE ID = :id — reports one affected row per parameter set. */
    private void stubBatchUpdate() {
        lenient().when(jdbcTemplate.batchUpdate(anyString(), any(SqlParameterSource[].class)))
                .thenAnswer(invocation -> {
                    SqlParameterSource[] batch = invocation.getArgument(1);
                    int[] counts = new int[batch.length];
                    Arrays.fill(counts, 1);
                    return counts;
                });
    }

    /** DELETE ... WHERE ID IN (:ids) — reports one affected row per id. */
    private void stubDelete() {
        lenient().when(jdbcTemplate.update(anyString(), any(SqlParameterSource.class)))
                .thenAnswer(invocation -> {
                    SqlParameterSource params = invocation.getArgument(1);
                    Object ids = params.getValue("ids");
                    return ids instanceof Collection<?> collection ? collection.size() : 0;
                });
    }

    private String tableOf(String sql) {
        for (String table : List.of(PA_TABLE, "ANAG_PSP", "ANAG_INTERMEDIARIO_PA", "ANAG_INTERMEDIARIO_PSP")) {
            if (sql.contains("sert_ingestor." + table + " ")) {
                return table;
            }
        }
        return "?";
    }

    /** ADX answers with a description only for codes present in {@link #adxMaster}. */
    private void stubAdxFromMaster() {
        when(adxClient.executeQuery(any(), eq("db"), anyString()))
                .thenAnswer(invocation -> new AdxQueryResult(true, masterAsAdxRows(), null));
    }

    private Map<String, Object> masterAsAdxRows() {
        Map<String, Object> data = new LinkedHashMap<>();
        adxMaster.forEach((codice, description) ->
                data.put(codice, Map.of("CODICE", codice, "DESCRIPTION", description)));
        return data;
    }

    private void givenRows(String table, long fromId, int count, String codicePrefix) {
        List<long[]> rows = rowIdsByTable.computeIfAbsent(table, key -> new ArrayList<>());
        for (int i = 0; i < count; i++) {
            long id = fromId + i;
            rows.add(new long[] {id});
            codiceById.put(id, codicePrefix + id);
        }
    }

    @Test
    void sweepsPastUnresolvableCodesInsteadOfStoppingAtTheFirstPage() {
        // Head of the table: a whole page of codes ADX cannot resolve (the real-world garbage).
        givenRows(PA_TABLE, 1, SELECT_BATCH_SIZE, "junk-");
        // Behind them: a legitimate code the old implementation could never reach.
        givenRows(PA_TABLE, 1_000, 1, "legit-");
        adxMaster.put("legit-1000", "COMUNE DI ESEMPIO");
        stubAdxFromMaster();

        runner().run(jobParameters());

        ArgumentCaptor<SqlParameterSource[]> batchCaptor = ArgumentCaptor.forClass(SqlParameterSource[].class);
        verify(jdbcTemplate, atLeastOnce()).batchUpdate(anyString(), batchCaptor.capture());
        List<Object> updatedIds = new ArrayList<>();
        for (SqlParameterSource[] batch : batchCaptor.getAllValues()) {
            for (SqlParameterSource params : batch) {
                updatedIds.add(params.getValue("id"));
            }
        }
        assertTrue(updatedIds.contains(1_000L),
                "the legitimate code behind the unresolvable head was never updated: " + updatedIds);
    }

    @Test
    void advancesTheCursorPastEveryPage() {
        givenRows(PA_TABLE, 1, SELECT_BATCH_SIZE, "junk-");
        givenRows(PA_TABLE, 1_000, 1, "legit-");
        adxMaster.put("legit-1000", "COMUNE DI ESEMPIO");
        stubAdxFromMaster();

        runner().run(jobParameters());

        // First page starts at 0; the next resumes strictly after the last ID of page one.
        assertEquals(List.of(0L, 500L), observedCursors);
    }

    @Test
    void completesTheRunWhenNothingCanBeResolved() {
        // A table of purely unresolvable codes is a no-op, not a failure.
        givenRows(PA_TABLE, 1, 10, "junk-");
        stubAdxFromMaster(); // empty master

        runner().run(jobParameters());

        verify(jdbcTemplate, never()).batchUpdate(anyString(), any(SqlParameterSource[].class));
        verify(executionLogService).logCompleted(any(), anyLong(), anyLong(), anyLong(),
                anyLong(), anyLong(), anyLong(), anyLong(), eq("COMPLETED"));
    }

    @Test
    void keepsSweepingWhenASingleAdxChunkFails() {
        // 150 codes => two ADX chunks (100 + 50); the first fails, the second must still run.
        givenRows(PA_TABLE, 1, 150, "legit-");
        for (long id = 1; id <= 150; id++) {
            adxMaster.put("legit-" + id, "PA " + id);
        }
        AtomicInteger calls = new AtomicInteger();
        when(adxClient.executeQuery(any(), eq("db"), anyString())).thenAnswer(invocation ->
                calls.getAndIncrement() == 0
                        ? new AdxQueryResult(false, null, "ADX transient failure")
                        : new AdxQueryResult(true, masterAsAdxRows(), null));

        runner().run(jobParameters());

        assertTrue(calls.get() >= 2, "the sweep stopped at the first failing chunk");
        verify(jdbcTemplate, atLeastOnce()).batchUpdate(anyString(), any(SqlParameterSource[].class));
        verify(executionLogService).logCompleted(any(), anyLong(), anyLong(), anyLong(),
                anyLong(), anyLong(), anyLong(), anyLong(), eq("COMPLETED"));
    }

    @Test
    void stopsHammeringAdxWhenItIsDown() {
        // Removing the early exits must not turn an ADX outage into a full-table scan of failing
        // calls: 20 pages x 5 chunks would be 100 round-trips (each with its own client retries).
        // The circuit breaker must cap it at MAX_CONSECUTIVE_FAILED_PAGES x MAX_CONSECUTIVE_CHUNK_FAILURES.
        givenRows(PA_TABLE, 1, 20 * SELECT_BATCH_SIZE, "legit-");
        AtomicInteger calls = new AtomicInteger();
        when(adxClient.executeQuery(any(), eq("db"), anyString())).thenAnswer(invocation -> {
            calls.incrementAndGet();
            return new AdxQueryResult(false, null, "ADX unreachable");
        });

        assertThrows(RuntimeException.class, () -> runner().run(jobParameters()));

        assertTrue(calls.get() <= 9,
                "circuit breaker did not bound the calls against a dead ADX: " + calls.get());
    }

    @Test
    void failsTheRunWhenEveryAdxLookupFails() {
        // ADX down: tolerating every chunk silently would report a green run that did nothing.
        givenRows(PA_TABLE, 1, 10, "legit-");
        when(adxClient.executeQuery(any(), eq("db"), anyString()))
                .thenReturn(new AdxQueryResult(false, null, "ADX unreachable"));

        assertThrows(RuntimeException.class, () -> runner().run(jobParameters()));

        verify(executionLogService).logFailed(any(), anyString(), anyString(),
                anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong());
        verify(executionLogService, never()).logCompleted(any(), anyLong(), anyLong(), anyLong(),
                anyLong(), anyLong(), anyLong(), anyLong(), anyString());
    }

    // ---- Reconciliation (ANAG_PA_EMITTENTE only, toggle ON) ----

    @Test
    void deletesCodesAbsentFromMaster() {
        // Empty-description codes that the master (successfully queried) does not know are deleted.
        givenRows(PA_TABLE, 1, 3, "junk-");
        stubAdxFromMaster(); // empty master => all absent

        runner(true).run(jobParameters());

        ArgumentCaptor<SqlParameterSource> captor = ArgumentCaptor.forClass(SqlParameterSource.class);
        verify(jdbcTemplate, atLeastOnce()).update(contains("DELETE"), captor.capture());
        Object ids = captor.getValue().getValue("ids");
        assertTrue(ids instanceof Collection, "delete must bind an id collection");
        assertEquals(3, ((Collection<?>) ids).size());
        verify(executionLogService).logCompleted(any(), anyLong(), anyLong(), anyLong(),
                anyLong(), anyLong(), anyLong(), anyLong(), eq("COMPLETED"));
    }

    @Test
    void keepsCodePresentInMasterWithoutDescription() {
        // Present in master but with empty RAGIONE_SOCIALE: kept (not deleted, not updated).
        givenRows(PA_TABLE, 1, 1, "legit-");
        adxMaster.put("legit-1", ""); // exists, no description
        stubAdxFromMaster();

        runner(true).run(jobParameters());

        verify(jdbcTemplate, never()).update(contains("DELETE"), any(SqlParameterSource.class));
        verify(jdbcTemplate, never()).batchUpdate(anyString(), any(SqlParameterSource[].class));
        verify(executionLogService).logCompleted(any(), anyLong(), anyLong(), anyLong(),
                anyLong(), anyLong(), anyLong(), anyLong(), eq("COMPLETED"));
    }

    @Test
    void neverDeletesWhenTheLookupFails() {
        // A failed chunk must never be read as "absent": no deletion, and the run fails.
        givenRows(PA_TABLE, 1, 3, "junk-");
        when(adxClient.executeQuery(any(), eq("db"), anyString()))
                .thenReturn(new AdxQueryResult(false, null, "ADX unreachable"));

        assertThrows(RuntimeException.class, () -> runner(true).run(jobParameters()));

        verify(jdbcTemplate, never()).update(contains("DELETE"), any(SqlParameterSource.class));
    }

    @Test
    void doesNotDeleteWhenReconcileToggleIsOff() {
        givenRows(PA_TABLE, 1, 3, "junk-");
        stubAdxFromMaster(); // empty master

        runner(false).run(jobParameters());

        verify(jdbcTemplate, never()).update(contains("DELETE"), any(SqlParameterSource.class));
    }

    @Test
    void doesNotReconcileNonPaEmittenteTables() {
        // An absent PSP code must never be deleted: reconciliation is scoped to ANAG_PA_EMITTENTE.
        givenRows("ANAG_PSP", 1, 3, "psp-");
        stubAdxFromMaster(); // empty master

        runner(true).run(jobParameters());

        verify(jdbcTemplate, never()).update(contains("DELETE"), any(SqlParameterSource.class));
    }

    @Test
    void deletesOnlyCodesFromSuccessfulChunksWhenAPageHasAPartialFailure() {
        // 150 codes => two ADX chunks (100 + 50). The first chunk fails, the second succeeds against an
        // empty master. Only the 50 codes proven absent by the successful chunk may be deleted; the 100
        // codes of the failed chunk must survive (a failed lookup is never read as "absent").
        givenRows(PA_TABLE, 1, 150, "legit-");
        AtomicInteger calls = new AtomicInteger();
        when(adxClient.executeQuery(any(), eq("db"), anyString())).thenAnswer(invocation ->
                calls.getAndIncrement() == 0
                        ? new AdxQueryResult(false, null, "ADX transient failure")
                        : new AdxQueryResult(true, masterAsAdxRows(), null)); // empty master => absent

        runner(true).run(jobParameters());

        ArgumentCaptor<SqlParameterSource> captor = ArgumentCaptor.forClass(SqlParameterSource.class);
        verify(jdbcTemplate, atLeastOnce()).update(contains("DELETE"), captor.capture());
        int deletedIds = captor.getAllValues().stream()
                .map(params -> params.getValue("ids"))
                .filter(ids -> ids instanceof Collection)
                .mapToInt(ids -> ((Collection<?>) ids).size())
                .sum();
        assertEquals(50, deletedIds, "only the successful chunk's absent codes may be deleted");
    }
}
