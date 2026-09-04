package it.pagopa.cruscotto.ingestion.service.ingestion;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.Position;
import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import it.pagopa.cruscotto.ingestion.repository.PositionRepository;
import it.pagopa.cruscotto.ingestion.repository.PositionTokenRegistryReader;
import it.pagopa.cruscotto.ingestion.repository.PositionTokensRepository;
import it.pagopa.cruscotto.ingestion.service.AnagraficaService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.sql.ResultSet;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class WindowFkPrefetchTest {

    @Mock
    private AnagraficaService anagraficaService;
    @Mock
    private PositionRepository positionRepository;
    @Mock
    private PositionTokensRepository positionTokensRepository;
    @Mock
    private PositionTokenRegistryReader positionTokenRegistryReader;
    @Mock
    private JdbcTemplate jdbcTemplate;
    @Mock
    private DbSchemaConfig dbSchemaConfig;

    private EntityTransformerImpl transformer;
    private PositionFkBatchPrefetcher positionPrefetcher;
    private TokenFkBatchPrefetcher tokenPrefetcher;

    @BeforeEach
    void setUp() {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        transformer = new EntityTransformerImpl(mapper, anagraficaService, positionRepository,
                positionTokensRepository,
                new CanonicalTokenResolver(positionTokensRepository, positionTokenRegistryReader));
        lenient().when(dbSchemaConfig.getSchemaName()).thenReturn("test_schema");
        positionPrefetcher = new PositionFkBatchPrefetcher(jdbcTemplate, dbSchemaConfig);
        tokenPrefetcher = new TokenFkBatchPrefetcher(jdbcTemplate, dbSchemaConfig);
    }

    @Test
    void positionPrefetchHitAvoidsIndividualLookup() throws Exception {
        LocalDateTime sourceTimestamp = LocalDateTime.of(2026, 5, 2, 0, 30);
        RunContext ctx = new RunContext(EntityName.POSITION_TOKENS.name(), "position-hit", Instant.now());
        ctx.getBatchLocalCache().putPositionWindowPrefetch("NAV", "PA", sourceTimestamp, 10);

        PositionTokens result = transformer.transform(positionRow("NAV", "PA", sourceTimestamp),
                PositionTokens.class, ctx, EntityName.POSITION_TOKENS);

        assertEquals(10, result.getFkPosition());
        verify(positionRepository, never())
                .findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
                        any(), any(), any(), any(), any(), any());
    }

    @Test
    void positionPrefetchMissFallsBackWithInclusive24HourBoundary() throws Exception {
        LocalDateTime sourceTimestamp = LocalDateTime.of(2026, 5, 2, 0, 30);
        Position position = new Position();
        position.setId(11);
        when(positionRepository.findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
                eq("NAV"), eq("PA"), eq(LocalDate.of(2026, 5, 1)), eq(LocalDate.of(2026, 5, 2)),
                eq(sourceTimestamp.minusHours(24)), eq(sourceTimestamp)))
                .thenReturn(Optional.of(position));

        PositionTokens result = transformer.transform(positionRow("NAV", "PA", sourceTimestamp),
                PositionTokens.class,
                new RunContext(EntityName.POSITION_TOKENS.name(), "position-miss", Instant.now()),
                EntityName.POSITION_TOKENS);

        assertEquals(11, result.getFkPosition());
    }

    @Test
    void positionPrefetchUsesOneBatchQueryForMultipleKeys() throws Exception {
        LocalDateTime firstTimestamp = LocalDateTime.of(2026, 5, 2, 9, 0);
        LocalDateTime secondTimestamp = firstTimestamp.plusMinutes(1);
        Map<String, Object> rows = new LinkedHashMap<>();
        rows.put("first", rawPositionRow("NAV-1", "PA-1", firstTimestamp));
        rows.put("second", rawPositionRow("NAV-2", "PA-2", secondTimestamp));
        ResultSet firstResult = mock(ResultSet.class);
        when(firstResult.getInt("request_key")).thenReturn(0);
        when(firstResult.getInt("position_id")).thenReturn(21);
        when(firstResult.wasNull()).thenReturn(false);
        ResultSet secondResult = mock(ResultSet.class);
        when(secondResult.getInt("request_key")).thenReturn(1);
        when(secondResult.getInt("position_id")).thenReturn(22);
        when(secondResult.wasNull()).thenReturn(false);
        doAnswer(invocation -> {
            RowCallbackHandler rowHandler = invocation.getArgument(1);
            rowHandler.processRow(firstResult);
            rowHandler.processRow(secondResult);
            return null;
        }).when(jdbcTemplate).query(anyString(), (RowCallbackHandler) any(),
                any(), any(), any(), any(), any(), any(), any(), any());

        BatchLocalCache cache = new BatchLocalCache();
        positionPrefetcher.prefetchForPositionTokens(rows, cache,
                new RunContext(EntityName.POSITION_TOKENS.name(), "position-batch", Instant.now()));

        assertEquals(21, cache.getPositionWindowPrefetch("NAV-1", "PA-1", firstTimestamp));
        assertEquals(22, cache.getPositionWindowPrefetch("NAV-2", "PA-2", secondTimestamp));
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).query(sqlCaptor.capture(), (RowCallbackHandler) any(),
                any(), any(), any(), any(), any(), any(), any(), any());
        assertTrue(sqlCaptor.getValue().contains("?::timestamp"));
    }

    @Test
    void tokenPrefetchHitAvoidsIndividualLookup() throws Exception {
        String token = "token-hit";
        RunContext ctx = new RunContext(EntityName.POSITION_TRANSFERS.name(), "token-hit", Instant.now());
        ctx.getBatchLocalCache().putTokenWindowPrefetch(
                java.util.Base64.getEncoder().encodeToString(token.getBytes()), 30);

        PositionTransfers result = transformer.transform(Map.of("TOKEN", token, "DATE_EVENT", "2026-05-02"),
                PositionTransfers.class, ctx, EntityName.POSITION_TRANSFERS);

        assertEquals(30, result.getFkToken());
        verify(positionTokenRegistryReader, never()).findFirstDateEventByToken(any());
    }

    @Test
    void tokenPrefetchMissFallsBackToIndividualResolver() throws Exception {
        String token = "token-miss";
        PositionTokens positionToken = new PositionTokens();
        positionToken.setId(31);
        when(positionTokenRegistryReader.findFirstDateEventByToken(token.getBytes()))
                .thenReturn(Optional.of(LocalDate.of(2026, 5, 1)));
        when(positionTokensRepository.findCanonicalByTokenAndDate(token.getBytes(), LocalDate.of(2026, 5, 1)))
                .thenReturn(Optional.of(positionToken));

        PositionTransfers result = transformer.transform(Map.of("TOKEN", token, "DATE_EVENT", "2026-05-02"),
                PositionTransfers.class,
                new RunContext(EntityName.POSITION_TRANSFERS.name(), "token-miss", Instant.now()),
                EntityName.POSITION_TRANSFERS);

        assertEquals(31, result.getFkToken());
    }

    @Test
    void tokenPrefetchUsesOneBatchQueryForMultipleTokens() throws Exception {
        Map<String, Object> rows = new LinkedHashMap<>();
        rows.put("first", Map.of("TOKEN", "token-1"));
        rows.put("second", Map.of("TOKEN", "token-2"));
        ResultSet firstResult = mock(ResultSet.class);
        when(firstResult.getInt("request_key")).thenReturn(0);
        when(firstResult.getObject("token_id")).thenReturn(41);
        when(firstResult.getObject("fk_position")).thenReturn(51);
        ResultSet secondResult = mock(ResultSet.class);
        when(secondResult.getInt("request_key")).thenReturn(1);
        when(secondResult.getObject("token_id")).thenReturn(42);
        when(secondResult.getObject("fk_position")).thenReturn(52);
        doAnswer(invocation -> {
            RowCallbackHandler rowHandler = invocation.getArgument(1);
            rowHandler.processRow(firstResult);
            rowHandler.processRow(secondResult);
            return null;
        }).when(jdbcTemplate).query(anyString(), (RowCallbackHandler) any(),
                any(), any(), any(), any());
        BatchLocalCache cache = new BatchLocalCache();
        tokenPrefetcher.prefetchForPositionTransfers(rows, cache,
                new RunContext(EntityName.POSITION_TRANSFERS.name(), "token-batch", Instant.now()));

        assertEquals(41, cache.getTokenWindowPrefetch(
                java.util.Base64.getEncoder().encodeToString("token-1".getBytes())));
        assertEquals(42, cache.getTokenWindowPrefetch(
                java.util.Base64.getEncoder().encodeToString("token-2".getBytes())));
        assertEquals(41, cache.getTokenCanonicalLookupResult(
                java.util.Base64.getEncoder().encodeToString("token-1".getBytes())));
        assertEquals(51, cache.getTokenCanonicalFkPosition(
                java.util.Base64.getEncoder().encodeToString("token-1".getBytes())));
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).query(sqlCaptor.capture(), (RowCallbackHandler) any(),
                any(), any(), any(), any());
        assertTrue(sqlCaptor.getValue().contains("?::bytea"));
    }

    @Test
    void windowPrefetchClearDoesNotClearRunCache() {
        BatchLocalCache cache = new BatchLocalCache();
        LocalDateTime timestamp = LocalDateTime.of(2026, 5, 2, 9, 0);
        cache.cachePositionLookupResult("NAV", "PA", timestamp, 50);
        cache.putPositionWindowPrefetch("NAV", "PA", timestamp, 51);

        cache.clearWindowPrefetch();

        assertFalse(cache.hasPositionWindowPrefetch("NAV", "PA", timestamp));
        assertEquals(50, cache.getPositionLookupResult("NAV", "PA", timestamp));
    }

    private Map<String, Object> positionRow(String nav, String paEmittente, LocalDateTime timestamp) {
        Map<String, Object> row = rawPositionRow(nav, paEmittente, timestamp);
        row.put("DATE_EVENT", timestamp.toLocalDate().toString());
        return row;
    }

    private Map<String, Object> rawPositionRow(String nav, String paEmittente, LocalDateTime timestamp) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("NAV", nav);
        row.put("PA_EMITTENTE", paEmittente);
        row.put("INSERTED_TIMESTAMP", timestamp);
        return row;
    }
}
