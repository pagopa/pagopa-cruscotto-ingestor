package it.pagopa.cruscotto.ingestion.service.adx;

import com.microsoft.azure.kusto.data.Client;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.config.AdxTableNamesConfig;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Golden catalogue of REAL error strings seen in production (INGEST_EXECUTION_LOG exports) mapped to the
 * decision our code must take. Each incident becomes a permanent regression guard: if a classifier ever
 * stops bucketing one of these correctly, this test fails in CI instead of the pipeline stalling in prod.
 *
 * <p>Buckets:
 * <ul>
 *   <li><b>TOO_LARGE</b> — result-set/limit error → halve the window ({@link AdxQueryService});</li>
 *   <li><b>TRANSIENT</b> — network/throttle blip → retry the idempotent read ({@link AdxClientImpl});</li>
 *   <li><b>PERMANENT</b> — halving/retry would not help → fail fast, carrying the vendor message;</li>
 *   <li><b>GUARDRAIL</b> — max-duration budget hit → graceful stop, not a failure.</li>
 * </ul>
 * Add a new line here (with the expected bucket) every time prod shows a new error shape.</p>
 */
@ExtendWith(MockitoExtension.class)
class AdxErrorClassificationGoldenTest {

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
    @Mock
    private Client kustoClient;

    private final RunContext ctx = new RunContext("POSITION", "run-golden", Instant.now());
    private final Instant from = Instant.parse("2026-08-03T09:12:00Z");
    private final Instant to = from.plusSeconds(3600);

    private AdxQueryService windowService() {
        AdxTableNamesConfig tableNames = new AdxTableNamesConfig();
        tableNames.setTables(Map.of("POSITION", "SERT_POSITION"));
        return new AdxQueryService(adxClient, new IngestionConfig(), tableNames,
                positionBuilder, positionTokensBuilder, transfersBuilder, eventsWfBuilder, extraInfoBuilder);
    }

    private AdxClientImpl retryClient(int maxAttempts) {
        IngestionConfig cfg = new IngestionConfig();
        cfg.getAdx().setQueryTimeout(Duration.ofSeconds(30));
        cfg.getAdx().getTransientRetry().setMaxAttempts(maxAttempts);
        cfg.getAdx().getTransientRetry().setBaseBackoff(Duration.ZERO);
        return new AdxClientImpl(kustoClient, cfg);
    }

    // ----------------------------------------------------------------------------------
    // TOO_LARGE  ->  window halving  (AdxQueryService.fetchWindow)
    // ----------------------------------------------------------------------------------
    @ParameterizedTest(name = "TOO_LARGE: {0}")
    @ValueSource(strings = {
            "RuntimeException: Error found while parsing json response as KustoOperationResult:Query "
                    + "execution failed with multiple inner exceptions | inner=Exception: {\"error\":{\"code\":"
                    + "\"LimitsExceeded\",\"@message\":\"exceed the set limit of 64 MB (E_QUERY_RESULT_SET_TOO_LARGE, 0x80DA0003).\"}}}",
            "Query execution has exceeded the allowed limits",
            "Query aborted: E_RUNAWAY_QUERY, too many records",
            "PartialQueryFailure: E_LOW_MEMORY_CONDITION, memory budget exceeded",
            "Aborted due to throttling"
    })
    void tooLargeErrorsTriggerHalving(String error) {
        when(positionBuilder.buildQuery(eq(ctx), any(), any())).thenReturn("SERT_POSITION | take 1");
        when(adxClient.executeQuery(eq(ctx), anyString(), anyString()))
                .thenReturn(new AdxQueryResult(false, null, error))
                .thenReturn(new AdxQueryResult(true, new LinkedHashMap<>(), null));

        Optional<AdxWindowResult> result = windowService().fetchWindow(ctx, from, Duration.ofMinutes(8), to);

        assertTrue(result.isPresent(), "must halve and retry, not fail fast");
        assertEquals(2, result.orElseThrow().getAttempts(), "one halving = a second attempt");
        assertEquals(Duration.ofMinutes(4), result.orElseThrow().getWindowUsed());
    }

    // ----------------------------------------------------------------------------------
    // PERMANENT  ->  fail fast, no halving  (AdxQueryService.fetchWindow)
    // ----------------------------------------------------------------------------------
    @ParameterizedTest(name = "PERMANENT: {0}")
    @ValueSource(strings = {
            "DataServiceException: Query timed out during the query planning phase., "
                    + "code=RequestExecutionTimeout", // confirmed: NOT transient, do not retry
            "Semantic error: 'COLUMN_X' could not be resolved",
            "Bad request: syntax error near 'summarize'",
            "Request is invalid and cannot be executed."
    })
    void permanentErrorsFailFastWithoutHalving(String error) {
        when(positionBuilder.buildQuery(eq(ctx), any(), any())).thenReturn("SERT_POSITION | take 1");
        when(adxClient.executeQuery(eq(ctx), anyString(), anyString()))
                .thenReturn(new AdxQueryResult(false, null, error));

        AdxQueryFailedException thrown = assertThrows(AdxQueryFailedException.class,
                () -> windowService().fetchWindow(ctx, from, Duration.ofMinutes(8), to));

        assertTrue(thrown.getMessage().contains(error) || error.contains(thrown.getAdxError()), thrown.getMessage());
        verify(adxClient, times(1)).executeQuery(eq(ctx), anyString(), anyString()); // no retry/halving
    }

    // ----------------------------------------------------------------------------------
    // GUARDRAIL  ->  graceful stop  (AdxQueryService.fetchWindow)
    // ----------------------------------------------------------------------------------
    @Test
    void guardrailSentinelStopsGracefullyWithoutHalving() {
        when(positionBuilder.buildQuery(eq(ctx), any(), any())).thenReturn("SERT_POSITION | take 1");
        when(adxClient.executeQuery(eq(ctx), anyString(), anyString()))
                .thenReturn(new AdxQueryResult(false, null, AdxClient.MAX_DURATION_GUARDRAIL_EXCEEDED_ERROR));

        assertThrows(AdxGuardrailStopException.class,
                () -> windowService().fetchWindow(ctx, from, Duration.ofMinutes(8), to));
        verify(adxClient, times(1)).executeQuery(eq(ctx), anyString(), anyString());
    }

    // ----------------------------------------------------------------------------------
    // TRANSIENT  ->  retried  (AdxClientImpl.executeQuery)
    // ----------------------------------------------------------------------------------
    @ParameterizedTest(name = "TRANSIENT: {0}")
    @ValueSource(strings = {
            "DataServiceException: Timed out in post request:Read timed out", // prod rows 109 / 111
            "java.net.SocketTimeoutException: Read timed out",
            "Connection reset",
            "connect timed out",
            "ServiceUnavailable: the service is temporarily unavailable",
            "TooManyRequests: request rate is too high"
    })
    void transientErrorsAreRetried(String error) throws Exception {
        AdxClientImpl client = retryClient(2);
        when(kustoClient.execute(anyString(), anyString(), any())).thenThrow(new RuntimeException(error));

        client.executeQuery(ctx, "db", "SERT_POSITION | take 1");

        verify(kustoClient, times(2)).execute(anyString(), anyString(), any()); // retried up to maxAttempts
    }

    // ----------------------------------------------------------------------------------
    // NON-TRANSIENT  ->  NOT retried at the client level  (AdxClientImpl.executeQuery)
    // ----------------------------------------------------------------------------------
    @ParameterizedTest(name = "NON_TRANSIENT: {0}")
    @ValueSource(strings = {
            "Query timed out during the query planning phase.", // confirmed: not transient
            "LimitsExceeded: exceed the set limit of 64 MB (E_QUERY_RESULT_SET_TOO_LARGE)", // halving, not retry
            "Semantic error: 'COLUMN_X' could not be resolved",
            "AADSTS7000215: Invalid client secret provided"
    })
    void nonTransientErrorsAreNotRetried(String error) throws Exception {
        AdxClientImpl client = retryClient(3);
        when(kustoClient.execute(anyString(), anyString(), any())).thenThrow(new RuntimeException(error));

        client.executeQuery(ctx, "db", "SERT_POSITION | take 1");

        verify(kustoClient, times(1)).execute(anyString(), anyString(), any()); // executed once, no retry
    }
}
