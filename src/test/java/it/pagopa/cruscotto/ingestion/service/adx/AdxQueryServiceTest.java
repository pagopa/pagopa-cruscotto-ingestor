package it.pagopa.cruscotto.ingestion.service.adx;

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

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
