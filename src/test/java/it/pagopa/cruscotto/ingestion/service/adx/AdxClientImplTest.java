package it.pagopa.cruscotto.ingestion.service.adx;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultColumn;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AdxClientImplTest {

    private static final String DATABASE = "re";
    private static final String QUERY = "SERT_POSITION | take 2";

    @Mock
    private Client kustoClient;

    @Mock
    private KustoOperationResult operationResult;

    @Mock
    private KustoResultSetTable resultTable;

    private AdxClientImpl adxClient;

    @BeforeEach
    void setUp() {
        IngestionConfig ingestionConfig = new IngestionConfig();
        ingestionConfig.getAdx().setQueryTimeout(Duration.ofSeconds(30));
        adxClient = new AdxClientImpl(kustoClient, ingestionConfig);
    }

    @Test
    void executeQueryShouldMapRowsAndEnsureUniqueKeys() throws Exception {
        KustoResultColumn[] columns = new KustoResultColumn[] {
                new KustoResultColumn("UNIQUE_ID", "string", 0),
                new KustoResultColumn("NAV", "string", 1)
        };

        when(kustoClient.execute(eq(DATABASE), eq(QUERY), any())).thenReturn(operationResult);
        when(operationResult.getPrimaryResults()).thenReturn(resultTable);
        when(resultTable.getColumns()).thenReturn(columns);
        when(resultTable.next()).thenReturn(true, true, false);
        when(resultTable.getObject(0)).thenReturn("id-1", "id-1");
        when(resultTable.getObject(1)).thenReturn("nav-a", "nav-b");

        AdxQueryResult result = adxClient.executeQuery(newRunContext(), DATABASE, QUERY);

        assertTrue(result.isSuccess());
        assertNotNull(result.getData());
        assertEquals(2, result.getData().size());
        assertTrue(result.getData().containsKey("id-1"));
        assertTrue(result.getData().containsKey("id-1#1"));

        ArgumentCaptor<ClientRequestProperties> requestPropertiesCaptor = ArgumentCaptor.forClass(ClientRequestProperties.class);
        verify(kustoClient).execute(eq(DATABASE), eq(QUERY), requestPropertiesCaptor.capture());
        assertNotNull(requestPropertiesCaptor.getValue());
    }

    @Test
    void executeQueryShouldFailFastWhenQueryIsBlank() throws Exception {
        AdxQueryResult result = adxClient.executeQuery(newRunContext(), DATABASE, " ");

        assertFalse(result.isSuccess());
        assertTrue(result.getError().contains("Invalid ADX query"));
        verify(kustoClient, never()).execute(eq(DATABASE), eq(" "), any());
    }

    @Test
    void executeQueryShouldReturnFailureWhenClientThrowsException() throws Exception {
        when(kustoClient.execute(eq(DATABASE), eq(QUERY), any())).thenThrow(new RuntimeException("boom"));

        AdxQueryResult result = adxClient.executeQuery(newRunContext(), DATABASE, QUERY);

        assertFalse(result.isSuccess());
        assertTrue(result.getError().contains("RuntimeException"));
        assertTrue(result.getError().contains("boom"));
    }

    @Test
    void executeQueryShouldExplainInvalidClientSecret() throws Exception {
        when(kustoClient.execute(eq(DATABASE), eq(QUERY), any()))
                .thenThrow(new RuntimeException("AADSTS7000215: Invalid client secret provided"));

        AdxQueryResult result = adxClient.executeQuery(newRunContext(), DATABASE, QUERY);

        assertFalse(result.isSuccess());
        assertTrue(result.getError().contains("Invalid ADX client secret"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void executeQueryShouldNormalizeLocalDateTimeToInstant() throws Exception {
        KustoResultColumn[] columns = new KustoResultColumn[] {
                new KustoResultColumn("UNIQUE_ID", "string", 0),
                new KustoResultColumn("INSERTED_TIMESTAMP", "datetime", 1)
        };

        LocalDateTime localDateTime = LocalDateTime.parse("2026-05-07T12:30:00");
        when(kustoClient.execute(eq(DATABASE), eq(QUERY), any())).thenReturn(operationResult);
        when(operationResult.getPrimaryResults()).thenReturn(resultTable);
        when(resultTable.getColumns()).thenReturn(columns);
        when(resultTable.next()).thenReturn(true, false);
        when(resultTable.getObject(0)).thenReturn("id-ts");
        when(resultTable.getObject(1)).thenReturn(localDateTime);

        AdxQueryResult result = adxClient.executeQuery(newRunContext(), DATABASE, QUERY);

        Map<String, Object> row = (Map<String, Object>) result.getData().get("id-ts");
        assertNotNull(row);
        assertEquals(Instant.parse("2026-05-07T12:30:00Z"), row.get("INSERTED_TIMESTAMP"));
    }

    @Test
    void executeQueryAppliesDefaultTimeoutWhenNoConfigAndNoGuardrail() throws Exception {
        // queryTimeout null + guardrail disabled (POJO default): the hard fallback must still bound
        // the server-side timeout, otherwise a hung query could pin a Quartz worker forever.
        IngestionConfig config = new IngestionConfig();
        config.getAdx().setQueryTimeout(null);
        AdxClientImpl client = new AdxClientImpl(kustoClient, config);

        when(kustoClient.execute(eq(DATABASE), eq(QUERY), any())).thenReturn(operationResult);
        when(operationResult.getPrimaryResults()).thenReturn(null);

        client.executeQuery(newRunContext(), DATABASE, QUERY);

        assertEquals(Duration.ofMinutes(8).toMillis(), capturedTimeoutMs());
    }

    @Test
    void executeQueryUsesConfiguredQueryTimeoutWhenNoGuardrail() throws Exception {
        IngestionConfig config = new IngestionConfig();
        config.getAdx().setQueryTimeout(Duration.ofSeconds(30));
        AdxClientImpl client = new AdxClientImpl(kustoClient, config);

        when(kustoClient.execute(eq(DATABASE), eq(QUERY), any())).thenReturn(operationResult);
        when(operationResult.getPrimaryResults()).thenReturn(null);

        client.executeQuery(newRunContext(), DATABASE, QUERY);

        assertEquals(30_000L, capturedTimeoutMs());
    }

    @Test
    void executeQueryCapsTimeoutToRemainingGuardrailDuration() throws Exception {
        // Guardrail remaining (5s) is smaller than the configured query timeout (30s): the query
        // must be capped to the remaining budget so it can never outlive the run's max-duration.
        IngestionConfig config = new IngestionConfig();
        config.getAdx().setQueryTimeout(Duration.ofSeconds(30));
        config.getGuardrails().setEnableMaxDuration(true);
        config.getGuardrails().setMaxDuration(Duration.ofSeconds(5));
        AdxClientImpl client = new AdxClientImpl(kustoClient, config);

        when(kustoClient.execute(eq(DATABASE), eq(QUERY), any())).thenReturn(operationResult);
        when(operationResult.getPrimaryResults()).thenReturn(null);

        client.executeQuery(new RunContext("POSITION", "run-1", Instant.now()), DATABASE, QUERY);

        long timeoutMs = capturedTimeoutMs();
        assertTrue(timeoutMs >= 1L && timeoutMs <= 5_000L,
                "expected timeout capped to remaining guardrail budget (<=5000ms), was " + timeoutMs);
    }

    private long capturedTimeoutMs() throws Exception {
        ArgumentCaptor<ClientRequestProperties> captor = ArgumentCaptor.forClass(ClientRequestProperties.class);
        verify(kustoClient).execute(eq(DATABASE), eq(QUERY), captor.capture());
        Long timeout = captor.getValue().getTimeoutInMilliSec();
        assertNotNull(timeout, "server-side timeout must always be set");
        return timeout;
    }

    private RunContext newRunContext() {
        return new RunContext("POSITION", "run-1", Instant.now());
    }
}


