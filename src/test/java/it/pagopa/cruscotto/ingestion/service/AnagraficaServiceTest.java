package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AnagraficaServiceTest {

    @Mock
    private NamedParameterJdbcTemplate jdbc;

    private AnagraficaService service;

    @BeforeEach
    void setUp() {
        DbSchemaConfig dbSchemaConfig = new DbSchemaConfig();
        dbSchemaConfig.setSchema("test_schema");
        IngestionConfig ingestionConfig = new IngestionConfig();
        ingestionConfig.getAnagrafica().getCache().setEnabled(false);
        service = new AnagraficaService(jdbc, dbSchemaConfig, ingestionConfig);
    }

    @Test
    void resolvePaEmittenteShouldUseDedicatedTable() {
        assertResolverUsesDedicatedTable(
                () -> service.resolvePaEmittenteId("run-1", "00147990923"),
                "ANAG_PA_EMITTENTE",
                "SQ_ANAG_PA_EMITTENTE"
        );
    }

    @Test
    void resolveIntermediarioPaShouldUseDedicatedTable() {
        assertResolverUsesDedicatedTable(
                () -> service.resolveIntermediarioPaId("run-1", "97735020584"),
                "ANAG_INTERMEDIARIO_PA",
                "SQ_ANAG_INTERMEDIARIO_PA"
        );
    }

    @Test
    void resolveIntermediarioPspShouldUseDedicatedTable() {
        assertResolverUsesDedicatedTable(
                () -> service.resolveIntermediarioPspId("run-1", "97735020584"),
                "ANAG_INTERMEDIARIO_PSP",
                "SQ_ANAG_INTERMEDIARIO_PSP"
        );
    }

    /**
     * Regression: ADX can carry a PA_EMITTENTE longer than the VARCHAR(255) of ANAG_PA_EMITTENTE.
     * Before clamping, the INSERT failed with "value too long for type character varying(255)";
     * being a SQL failure it aborted the whole ingestion run, the checkpoint was never persisted and
     * every following run re-read the same row — blocking POSITION (and all child entities) forever.
     */
    @Test
    void resolveTruncatesValueExceedingColumnWidth() {
        String oversized = "X".repeat(300);
        AtomicInteger queryCount = new AtomicInteger();
        when(jdbc.query(anyString(), any(MapSqlParameterSource.class), any(RowMapper.class))).thenAnswer(call -> {
            if (queryCount.getAndIncrement() == 0) {
                return List.of();
            }
            return List.of(7L);
        });
        when(jdbc.update(anyString(), anyMap())).thenReturn(1);

        long id = service.resolvePaEmittenteId("run-1", oversized);

        assertEquals(7L, id);

        // The SELECT must look up the truncated value, otherwise it would never match the stored row.
        ArgumentCaptor<MapSqlParameterSource> selectParams = ArgumentCaptor.forClass(MapSqlParameterSource.class);
        verify(jdbc, times(2)).query(anyString(), selectParams.capture(), any(RowMapper.class));
        assertEquals(255, String.valueOf(selectParams.getValue().getValue("value")).length());

        // The INSERT must bind the truncated value: this is what used to blow up in production.
        ArgumentCaptor<Map<String, Object>> insertParams = ArgumentCaptor.forClass(Map.class);
        verify(jdbc).update(anyString(), insertParams.capture());
        assertEquals(255, String.valueOf(insertParams.getValue().get("value")).length());
        assertEquals("X".repeat(255), insertParams.getValue().get("value"));
    }

    @Test
    void resolveKeepsValueWithinColumnWidthUnchanged() {
        String codice = "00147990923";
        AtomicInteger queryCount = new AtomicInteger();
        when(jdbc.query(anyString(), any(MapSqlParameterSource.class), any(RowMapper.class))).thenAnswer(call -> {
            if (queryCount.getAndIncrement() == 0) {
                return List.of();
            }
            return List.of(7L);
        });
        when(jdbc.update(anyString(), anyMap())).thenReturn(1);

        service.resolvePaEmittenteId("run-1", codice);

        ArgumentCaptor<Map<String, Object>> insertParams = ArgumentCaptor.forClass(Map.class);
        verify(jdbc).update(anyString(), insertParams.capture());
        assertEquals(codice, insertParams.getValue().get("value"));
    }

    @Test
    void resolveDoesNotSplitASurrogatePairWhenTruncating() {
        // The 255th char (index 254) is the HIGH surrogate of an emoji: a naive substring(0,255) would
        // leave a lone surrogate, which PostgreSQL rejects as invalid UTF-8 — reintroducing the very
        // failure the clamp prevents. Expect truncation to 254 chars with no trailing lone surrogate.
        String oversized = "A".repeat(254) + "😀" + "B".repeat(50); // 😀 at indices 254-255
        AtomicInteger queryCount = new AtomicInteger();
        when(jdbc.query(anyString(), any(MapSqlParameterSource.class), any(RowMapper.class))).thenAnswer(call -> {
            if (queryCount.getAndIncrement() == 0) {
                return List.of();
            }
            return List.of(7L);
        });
        when(jdbc.update(anyString(), anyMap())).thenReturn(1);

        service.resolvePaEmittenteId("run-1", oversized);

        ArgumentCaptor<Map<String, Object>> insertParams = ArgumentCaptor.forClass(Map.class);
        verify(jdbc).update(anyString(), insertParams.capture());
        String bound = String.valueOf(insertParams.getValue().get("value"));
        assertEquals(254, bound.length());
        assertEquals("A".repeat(254), bound);
        assertTrue(bound.isEmpty() || !Character.isHighSurrogate(bound.charAt(bound.length() - 1)),
                "must not end with a lone high surrogate");
    }

    private void assertResolverUsesDedicatedTable(LongSupplier invocation, String tableName, String sequenceName) {
        AtomicInteger queryCount = new AtomicInteger();
        when(jdbc.query(anyString(), any(MapSqlParameterSource.class), any(RowMapper.class))).thenAnswer(call -> {
            if (queryCount.getAndIncrement() == 0) {
                return List.of();
            }
            return List.of(7L);
        });
        when(jdbc.update(anyString(), anyMap())).thenReturn(1);

        long id = invocation.getAsLong();

        assertEquals(7L, id);

        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbc, times(2)).query(queryCaptor.capture(), any(MapSqlParameterSource.class), any(RowMapper.class));
        assertTrue(queryCaptor.getAllValues().stream().allMatch(sql -> sql.contains(tableName)));

        ArgumentCaptor<String> updateCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbc).update(updateCaptor.capture(), anyMap());
        assertTrue(updateCaptor.getValue().contains(tableName));
        assertTrue(updateCaptor.getValue().contains(sequenceName));
    }

    @FunctionalInterface
    private interface LongSupplier {
        long getAsLong();
    }
}
