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
