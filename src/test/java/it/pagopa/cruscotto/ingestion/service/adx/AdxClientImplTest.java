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
    private static final RunContext RUN_CONTEXT = new RunContext("POSITION", "run-1", Instant.parse("2026-05-07T00:00:00Z"));

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

        AdxQueryResult result = adxClient.executeQuery(RUN_CONTEXT, DATABASE, QUERY);

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
        AdxQueryResult result = adxClient.executeQuery(RUN_CONTEXT, DATABASE, " ");

        assertFalse(result.isSuccess());
        assertTrue(result.getError().contains("Invalid ADX query"));
        verify(kustoClient, never()).execute(eq(DATABASE), eq(" "), any());
    }

    @Test
    void executeQueryShouldReturnFailureWhenClientThrowsException() throws Exception {
        when(kustoClient.execute(eq(DATABASE), eq(QUERY), any())).thenThrow(new RuntimeException("boom"));

        AdxQueryResult result = adxClient.executeQuery(RUN_CONTEXT, DATABASE, QUERY);

        assertFalse(result.isSuccess());
        assertTrue(result.getError().contains("RuntimeException"));
        assertTrue(result.getError().contains("boom"));
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

        AdxQueryResult result = adxClient.executeQuery(RUN_CONTEXT, DATABASE, QUERY);

        Map<String, Object> row = (Map<String, Object>) result.getData().get("id-ts");
        assertNotNull(row);
        assertEquals(Instant.parse("2026-05-07T12:30:00Z"), row.get("INSERTED_TIMESTAMP"));
    }
}



