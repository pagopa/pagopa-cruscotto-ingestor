package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.service.adx.AdxClient;
import it.pagopa.cruscotto.ingestion.service.adx.AdxQueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OldestTimestampProviderImplTest {

    private static final RunContext RUN_CONTEXT = new RunContext(
            "POSITION",
            "run-oldest-1",
            Instant.parse("2026-05-07T00:00:00Z")
    );

    @Mock
    private AdxClient adxClient;

    private OldestTimestampProviderImpl provider;

    @BeforeEach
    void setUp() {
        provider = new OldestTimestampProviderImpl(adxClient, new IngestionConfig());
    }

    @Test
    void shouldResolveOldestTimestampFromAdx() {
        Instant oldest = Instant.parse("2026-03-30T13:25:13Z");
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("OLDEST_TIMESTAMP", oldest);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("row_0", row);

        when(adxClient.executeQuery(eq(RUN_CONTEXT), eq("re"), contains("SERT_POSITION")))
                .thenReturn(new AdxQueryResult(true, data, null));

        Optional<Instant> result = provider.getOldestTimestamp(RUN_CONTEXT, EntityName.POSITION);

        assertTrue(result.isPresent());
        assertEquals(oldest, result.get());
        verify(adxClient).executeQuery(eq(RUN_CONTEXT), eq("re"), contains("summarize OLDEST_TIMESTAMP=min(INSERTED_TIMESTAMP)"));
    }

    @Test
    void shouldResolveOldestTimestampFromLocalDateTime() {
        LocalDateTime oldest = LocalDateTime.parse("2026-03-30T13:25:13");
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("oldest_timestamp", oldest);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("row_0", row);

        when(adxClient.executeQuery(eq(RUN_CONTEXT), eq("re"), contains("SERT_POSITION")))
                .thenReturn(new AdxQueryResult(true, data, null));

        Optional<Instant> result = provider.getOldestTimestamp(RUN_CONTEXT, EntityName.POSITION);

        assertTrue(result.isPresent());
        assertEquals(Instant.parse("2026-03-30T13:25:13Z"), result.get());
    }

    @Test
    void shouldReturnEmptyWhenAdxQueryFails() {
        when(adxClient.executeQuery(eq(RUN_CONTEXT), eq("re"), contains("SERT_POSITION")))
                .thenReturn(new AdxQueryResult(false, null, "boom"));

        Optional<Instant> result = provider.getOldestTimestamp(RUN_CONTEXT, EntityName.POSITION);

        assertFalse(result.isPresent());
    }

    @Test
    void shouldReturnEmptyForUnsupportedEntity() {
        Optional<Instant> result = provider.getOldestTimestamp(RUN_CONTEXT, EntityName.RECONCILIATION);

        assertFalse(result.isPresent());
    }
}


