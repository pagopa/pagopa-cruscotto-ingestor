package it.pagopa.cruscotto.ingestion.service.adx;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.config.AdxTableNamesConfig;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link AdxQueryService#findNextInsertedTimestamp} — the empty-window "probe": it must
 * return the earliest {@code INSERTED_TIMESTAMP} in range, distinguish a genuine "no data" (min NULL)
 * from a failed/ambiguous probe (throws, so the caller falls back to the safe step advance).
 */
@ExtendWith(MockitoExtension.class)
class AdxQueryServiceTest {

    @Mock
    private AdxClient adxClient;
    @Mock
    private PositionAdxQueryBuilder positionBuilder;
    @Mock
    private PositionTokensAdxQueryBuilder positionTokensBuilder;
    @Mock
    private TransfersAdxQueryBuilder transfersBuilder;
    @Mock
    private EventsWfAdxQueryBuilder eventsWfBuilder;
    @Mock
    private ExtraInfoAdxQueryBuilder extraInfoBuilder;

    private AdxQueryService service;

    private final RunContext ctx = new RunContext("POSITION", "run-probe", Instant.parse("2026-08-13T22:00:00Z"));
    private final Instant from = Instant.parse("2026-08-13T20:00:00Z");
    private final Instant to = Instant.parse("2026-08-13T20:00:00Z").plusSeconds(3600);

    @BeforeEach
    void setUp() {
        IngestionConfig ingestionConfig = new IngestionConfig();
        AdxTableNamesConfig tableNames = new AdxTableNamesConfig();
        tableNames.setTables(Map.of("POSITION", "SERT_POSITION"));
        service = new AdxQueryService(adxClient, ingestionConfig, tableNames,
                positionBuilder, positionTokensBuilder, transfersBuilder, eventsWfBuilder, extraInfoBuilder);
    }

    private Map<String, Object> aggregateRow(Object nextTs) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("NEXT_TS", nextTs);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("row_0", row);
        return data;
    }

    @Test
    void returnsEarliestTimestampAndTargetsTheEntityTable() {
        Instant next = Instant.parse("2026-08-13T20:30:00Z");
        when(adxClient.executeQuery(eq(ctx), anyString(), anyString()))
                .thenReturn(new AdxQueryResult(true, aggregateRow(next), null));

        Optional<Instant> result = service.findNextInsertedTimestamp(ctx, EntityName.POSITION, from, to);

        assertEquals(Optional.of(next), result);
        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        verify(adxClient).executeQuery(eq(ctx), anyString(), queryCaptor.capture());
        String query = queryCaptor.getValue();
        assertTrue(query.contains("SERT_POSITION"), "probe must target the entity table");
        assertTrue(query.contains("INSERTED_TIMESTAMP"), "probe must filter on INSERTED_TIMESTAMP");
        assertTrue(query.contains("min(INSERTED_TIMESTAMP)"), "probe must aggregate the minimum");
    }

    @Test
    void parsesStringTimestampFromAdx() {
        // ADX (depending on driver/serialization) may return the aggregated datetime as a String.
        when(adxClient.executeQuery(eq(ctx), anyString(), anyString()))
                .thenReturn(new AdxQueryResult(true, aggregateRow("2026-08-03T00:00:00Z"), null));

        Optional<Instant> result = service.findNextInsertedTimestamp(ctx, EntityName.POSITION, from, to);

        assertEquals(Optional.of(Instant.parse("2026-08-03T00:00:00Z")), result);
    }

    @Test
    void throwsWhenNextTsPresentButUnparseable() {
        // Present but not a timestamp -> ambiguous: must throw (caller falls back to step), NOT skip.
        when(adxClient.executeQuery(eq(ctx), anyString(), anyString()))
                .thenReturn(new AdxQueryResult(true, aggregateRow("not-a-date"), null));

        assertThrows(IllegalStateException.class,
                () -> service.findNextInsertedTimestamp(ctx, EntityName.POSITION, from, to));
    }

    @Test
    void returnsEmptyWhenNoDataInRange() {
        when(adxClient.executeQuery(eq(ctx), anyString(), anyString()))
                .thenReturn(new AdxQueryResult(true, aggregateRow(null), null));

        Optional<Instant> result = service.findNextInsertedTimestamp(ctx, EntityName.POSITION, from, to);

        assertTrue(result.isEmpty(), "a NULL min means genuinely no data in range");
    }

    @Test
    void throwsWhenProbeQueryFails() {
        when(adxClient.executeQuery(eq(ctx), anyString(), anyString()))
                .thenReturn(new AdxQueryResult(false, null, "ADX timeout"));

        assertThrows(IllegalStateException.class,
                () -> service.findNextInsertedTimestamp(ctx, EntityName.POSITION, from, to));
    }

    // ---------------------------------------------------------------
    // fetchWindow: classificazione errore -> dimezza la finestra o fallisce subito
    // ---------------------------------------------------------------

    @Test
    void halvesWindowOnResourceLimitErrorAndSucceeds() {
        // E_RUNAWAY_QUERY = limite di servizio superato: ridurre la finestra e' la cura corretta,
        // quindi la query va ritentata dimezzata invece di fallire subito (che bloccherebbe l'entita').
        when(positionBuilder.buildQuery(eq(ctx), any(), any())).thenReturn("SERT_POSITION | take 1");
        when(adxClient.executeQuery(eq(ctx), anyString(), anyString()))
                .thenReturn(new AdxQueryResult(false, null, "Query aborted: E_RUNAWAY_QUERY, too many records"))
                .thenReturn(new AdxQueryResult(true, new LinkedHashMap<>(), null));

        Optional<AdxWindowResult> result = service.fetchWindow(ctx, from, Duration.ofMinutes(8), to);

        assertTrue(result.isPresent(), "after halving, the retry must return the window");
        assertEquals(Duration.ofMinutes(4), result.orElseThrow().getWindowUsed(), "window must be halved");
        assertEquals(2, result.orElseThrow().getAttempts());
    }

    @Test
    void halvesWindowOnNestedKustoLimitError() {
        // The real production string: generic top-level message + flattened nested cause carrying the
        // limit keywords (this is what AdxClientImpl now produces). It must be recognised so the
        // window halves instead of failing fast (the EVENTS_WF stall the client reported).
        String nestedLimitError = "RuntimeException: Error found while parsing json response as "
                + "KustoOperationResult:Query execution failed with multiple inner exceptions "
                + "| causedBy=DataServiceException: LimitsExceeded: The results of this query exceed "
                + "the set limit of 64 MB (E_QUERY_RESULT_SET_TOO_LARGE, 0x80DA0003)";
        when(positionBuilder.buildQuery(eq(ctx), any(), any())).thenReturn("SERT_POSITION | take 1");
        when(adxClient.executeQuery(eq(ctx), anyString(), anyString()))
                .thenReturn(new AdxQueryResult(false, null, nestedLimitError))
                .thenReturn(new AdxQueryResult(true, new LinkedHashMap<>(), null));

        Optional<AdxWindowResult> result = service.fetchWindow(ctx, from, Duration.ofMinutes(8), to);

        assertTrue(result.isPresent(), "nested LimitsExceeded must trigger halving, not fail-fast");
        assertEquals(Duration.ofMinutes(4), result.orElseThrow().getWindowUsed());
        assertEquals(2, result.orElseThrow().getAttempts());
    }

    @Test
    void halvesWindowOnAuthenticRenderedKustoResultSetTooLarge() throws Exception {
        // The definitive end-to-end check the client's stall demands: build the EXACT prod
        // KustoServiceQueryError, let the REAL AdxClientImpl render it (same buildErrorMessage the
        // runtime uses), then feed that AUTHENTIC string through fetchWindow. Proves the loop
        // recognises the real message and halves on EACH subsequent attempt (8m -> 4m -> 2m) until a
        // smaller window succeeds — instead of failing fast, which is what left EVENTS_WF frozen.
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode innerExceptions = mapper.createArrayNode();
        innerExceptions.add(mapper.readTree(
                "{\"error\":{\"code\":\"LimitsExceeded\",\"@message\":\"The results of this query exceed the "
                        + "set limit of 64 MB (E_QUERY_RESULT_SET_TOO_LARGE, 0x80DA0003).\"}}"));
        KustoServiceQueryError kustoError = new KustoServiceQueryError(
                innerExceptions, false, "Query execution failed with multiple inner exceptions");
        Exception top = new RuntimeException(
                "Error found while parsing json response as KustoOperationResult:"
                        + "Query execution failed with multiple inner exceptions", kustoError);

        // Authentic error string, produced by the REAL client from the REAL SDK exception. Use a
        // just-started run so the max-duration guardrail pre-check passes and the query is actually
        // executed (a run started in the past would short-circuit to the guardrail sentinel instead).
        Client throwingKusto = mock(Client.class);
        when(throwingKusto.execute(anyString(), anyString(), any())).thenThrow(top);
        RunContext freshCtx = new RunContext("POSITION", "run-authentic", Instant.now());
        String authenticError = new AdxClientImpl(throwingKusto, new IngestionConfig())
                .executeQuery(freshCtx, "db", "SERT_POSITION | take 1").getError();
        assertTrue(authenticError.contains("LimitsExceeded"), authenticError);
        assertTrue(authenticError.contains("E_QUERY_RESULT_SET_TOO_LARGE"), authenticError);

        // Now drive fetchWindow with that exact string: too-large on 8m and 4m, success on 2m.
        when(positionBuilder.buildQuery(eq(ctx), any(), any())).thenReturn("SERT_POSITION | take 1");
        when(adxClient.executeQuery(eq(ctx), anyString(), anyString()))
                .thenReturn(new AdxQueryResult(false, null, authenticError))
                .thenReturn(new AdxQueryResult(false, null, authenticError))
                .thenReturn(new AdxQueryResult(true, new LinkedHashMap<>(), null));

        Optional<AdxWindowResult> result = service.fetchWindow(ctx, from, Duration.ofMinutes(8), to);

        assertTrue(result.isPresent(), "authentic rendered LimitsExceeded must trigger halving, not fail-fast");
        assertEquals(Duration.ofMinutes(2), result.orElseThrow().getWindowUsed(), "two halvings: 8m -> 4m -> 2m");
        assertEquals(3, result.orElseThrow().getAttempts());
        verify(adxClient, times(3)).executeQuery(eq(ctx), anyString(), anyString());
    }

    @Test
    void halvesWindowOnConfiguredCustomErrorPattern() {
        // Un messaggio del vendor non previsto si gestisce da configurazione, senza rilascio.
        IngestionConfig config = new IngestionConfig();
        config.getAdx().setWindowTooLargeErrorPatterns(List.of("Custom vendor limit"));
        AdxTableNamesConfig tableNames = new AdxTableNamesConfig();
        tableNames.setTables(Map.of("POSITION", "SERT_POSITION"));
        AdxQueryService configured = new AdxQueryService(adxClient, config, tableNames,
                positionBuilder, positionTokensBuilder, transfersBuilder, eventsWfBuilder, extraInfoBuilder);

        when(positionBuilder.buildQuery(eq(ctx), any(), any())).thenReturn("SERT_POSITION | take 1");
        when(adxClient.executeQuery(eq(ctx), anyString(), anyString()))
                .thenReturn(new AdxQueryResult(false, null, "Custom vendor limit reached"))
                .thenReturn(new AdxQueryResult(true, new LinkedHashMap<>(), null));

        Optional<AdxWindowResult> result = configured.fetchWindow(ctx, from, Duration.ofMinutes(8), to);

        assertTrue(result.isPresent());
        assertEquals(Duration.ofMinutes(4), result.orElseThrow().getWindowUsed());
    }

    @Test
    void mapsGuardrailExceededToAGracefulStopNotAFailure() {
        // Budget max-duration esaurito a metà run: NON è un errore ADX. fetchWindow deve segnalarlo
        // come stop guardrail (AdxGuardrailStopException) e non come AdxQueryFailedException/FAILED,
        // e senza dimezzare la finestra.
        when(positionBuilder.buildQuery(eq(ctx), any(), any())).thenReturn("SERT_POSITION | take 1");
        when(adxClient.executeQuery(eq(ctx), anyString(), anyString()))
                .thenReturn(new AdxQueryResult(false, null, AdxClient.MAX_DURATION_GUARDRAIL_EXCEEDED_ERROR));

        assertThrows(AdxGuardrailStopException.class,
                () -> service.fetchWindow(ctx, from, Duration.ofMinutes(8), to));

        // una sola query: nessun dimezzamento/retry
        verify(adxClient).executeQuery(eq(ctx), anyString(), anyString());
    }

    @Test
    void failsFastWithoutHalvingOnUnrelatedErrorAndCarriesTheAdxMessage() {
        // Errore non legato ai limiti: ridurre la finestra non aiuta -> una sola query, e l'eccezione
        // deve portare il messaggio del vendor, che finisce in INGEST_EXECUTION_LOG.ERROR_MESSAGE.
        when(positionBuilder.buildQuery(eq(ctx), any(), any())).thenReturn("SERT_POSITION | take 1");
        when(adxClient.executeQuery(eq(ctx), anyString(), anyString()))
                .thenReturn(new AdxQueryResult(false, null, "Authentication failed"));

        AdxQueryFailedException thrown = assertThrows(AdxQueryFailedException.class,
                () -> service.fetchWindow(ctx, from, Duration.ofMinutes(8), to));

        assertEquals("Authentication failed", thrown.getAdxError());
        assertTrue(thrown.getMessage().contains("Authentication failed"),
                "the vendor message must be part of the exception message persisted in the log table");
        verify(adxClient).executeQuery(eq(ctx), anyString(), anyString());
    }

    @Test
    void throwsWindowTooLargeWhenHalvingAttemptsAreExhausted() {
        when(positionBuilder.buildQuery(eq(ctx), any(), any())).thenReturn("SERT_POSITION | take 1");
        when(adxClient.executeQuery(eq(ctx), anyString(), anyString()))
                .thenReturn(new AdxQueryResult(false, null, "LimitsExceeded"));

        assertThrows(AdxWindowTooLargeException.class,
                () -> service.fetchWindow(ctx, from, Duration.ofMinutes(8), to));
    }

    @Test
    void throwsWhenAggregateColumnMissing() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("OTHER", 1);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("row_0", row);
        when(adxClient.executeQuery(eq(ctx), anyString(), anyString()))
                .thenReturn(new AdxQueryResult(true, data, null));

        assertThrows(IllegalStateException.class,
                () -> service.findNextInsertedTimestamp(ctx, EntityName.POSITION, from, to));
    }
}
