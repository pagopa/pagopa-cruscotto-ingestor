package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.Position;
import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Guards the set-based cache readback (replacing the former per-row N+1 SELECTs after a bulk
 * insert): exactly one query is issued and the batch cache is populated from its result set.
 */
@ExtendWith(MockitoExtension.class)
class BulkWriterImplTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    private BulkWriterImpl bulkWriter;

    @BeforeEach
    void setUp() {
        bulkWriter = new BulkWriterImpl(jdbcTemplate, new DbSchemaConfig(), new IngestionConfig());
    }

    @Test
    void positionInsertPopulatesCacheWithASingleReadbackQuery() throws Exception {
        LocalDateTime insertedTs = LocalDateTime.parse("2026-05-07T12:30:00");
        Position position = new Position();
        position.setNav("NAV-1");
        position.setPaEmittente("PA-1");
        position.setInsertedTimestamp(insertedTs);
        position.setDateEvent(LocalDate.parse("2026-05-07"));

        when(jdbcTemplate.batchUpdate(anyString(), any(BatchPreparedStatementSetter.class)))
                .thenReturn(new int[] {1});

        // Simulate the set-based readback returning request_key=0 -> id=555, capturing the SQL.
        final String[] capturedSql = {null};
        doAnswer(invocation -> {
            capturedSql[0] = invocation.getArgument(0);
            RowCallbackHandler handler = invocation.getArgument(1);
            ResultSet rs = mock(ResultSet.class);
            when(rs.getInt("request_key")).thenReturn(0);
            when(rs.getInt("position_id")).thenReturn(555);
            when(rs.wasNull()).thenReturn(false);
            handler.processRow(rs);
            return null;
        }).when(jdbcTemplate).query(anyString(), any(RowCallbackHandler.class), any(Object[].class));

        BatchLocalCache cache = new BatchLocalCache();
        bulkWriter.writeBulk(EntityName.POSITION, List.of(position), "run-1", cache);

        // Cache resolves the inserted id via the in-window lookup.
        assertEquals(555, cache.findPositionInWindow("NAV-1", "PA-1", insertedTs));
        // Exactly one set-based query, and NO per-row queryForObject (the old N+1).
        verify(jdbcTemplate, times(1)).query(anyString(), any(RowCallbackHandler.class), any(Object[].class));
        verify(jdbcTemplate, never()).queryForObject(anyString(), any(Class.class), any(Object[].class));
        // Partition pruning: the readback must constrain DATE_EVENT and pick the newest id.
        assertTrue(capturedSql[0].contains("DATE_EVENT = r.date_event"),
                "POSITION readback must filter on DATE_EVENT for partition pruning: " + capturedSql[0]);
        assertTrue(capturedSql[0].contains("ORDER BY position.ID DESC"), capturedSql[0]);
    }

    @Test
    void tokenInsertPopulatesCacheWithASingleReadbackQuery() throws Exception {
        byte[] token = "token-abc".getBytes(StandardCharsets.UTF_8);
        String tokenBase64 = Base64.getEncoder().encodeToString(token);

        PositionTokens positionToken = new PositionTokens();
        positionToken.setToken(token);
        positionToken.setDateEvent(LocalDate.parse("2026-05-07"));

        when(jdbcTemplate.batchUpdate(anyString(), any(BatchPreparedStatementSetter.class)))
                .thenReturn(new int[] {1});

        final String[] capturedSql = {null};
        doAnswer(invocation -> {
            capturedSql[0] = invocation.getArgument(0);
            RowCallbackHandler handler = invocation.getArgument(1);
            ResultSet rs = mock(ResultSet.class);
            when(rs.getInt("request_key")).thenReturn(0);
            when(rs.getObject("token_id")).thenReturn(Integer.valueOf(777));
            handler.processRow(rs);
            return null;
        }).when(jdbcTemplate).query(anyString(), any(RowCallbackHandler.class), any(Object[].class));

        BatchLocalCache cache = new BatchLocalCache();
        bulkWriter.writeBulk(EntityName.POSITION_TOKENS, List.of(positionToken), "run-1", cache);

        assertEquals(777, cache.findToken(tokenBase64));
        verify(jdbcTemplate, times(1)).query(anyString(), any(RowCallbackHandler.class), any(Object[].class));
        verify(jdbcTemplate, never()).queryForObject(anyString(), any(Class.class), any(Object[].class));
        // First-write-wins = lowest id: ORDER BY id ASC, and NO DATE_EVENT filter (the canonical
        // row may live in an earlier partition, so pruning by date would break FK resolution).
        assertTrue(capturedSql[0].contains("ORDER BY position_token.ID ASC"), capturedSql[0]);
        assertFalse(capturedSql[0].toLowerCase().contains("date_event"),
                "TOKEN readback must NOT filter on DATE_EVENT (first-write-wins is cross-partition): " + capturedSql[0]);
    }
}
