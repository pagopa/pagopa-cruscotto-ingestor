package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.ExtraInfo;
import it.pagopa.cruscotto.ingestion.entity.Position;
import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
    void positionUpdateNeverRewritesBirthCoordinatesAndAppendsAdditionalDay() throws Exception {
        // Rule 7.1: l'evento dell'11/09 si aggancia alla POSITION nata il 10/09. DATE_EVENT e
        // INSERTED_TIMESTAMP (la nascita) non devono essere riscritti: sono l'ancora della finestra
        // 24h e della risoluzione FK dei figli. Il giorno nuovo va solo in DATE_EVENTS.
        Position position = new Position();
        position.setId(4242);
        position.setNav("NAV-1");
        position.setPaEmittente("PA-1");
        position.setDateEvent(LocalDate.parse("2026-09-11"));
        position.setInsertedTimestamp(LocalDateTime.parse("2026-09-11T06:23:07"));
        position.setLastEvent(LocalDateTime.parse("2026-09-11T06:23:07"));

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<BatchPreparedStatementSetter> setterCaptor =
                ArgumentCaptor.forClass(BatchPreparedStatementSetter.class);
        when(jdbcTemplate.batchUpdate(sqlCaptor.capture(), setterCaptor.capture())).thenReturn(new int[] {1});

        BatchLocalCache cache = new BatchLocalCache();
        bulkWriter.writeBulk(EntityName.POSITION, List.of(position), "run-1", cache);

        String sql = sqlCaptor.getValue();
        assertFalse(sql.contains("DATE_EVENT = ?"),
                "l'UPDATE non deve riscrivere DATE_EVENT (data di nascita): " + sql);
        assertFalse(sql.contains("INSERTED_TIMESTAMP = ?"),
                "l'UPDATE non deve riscrivere INSERTED_TIMESTAMP (data di nascita): " + sql);
        assertTrue(sql.contains("DATE_EVENTS"), sql);
        assertTrue(sql.contains("jsonb_agg(DISTINCT d ORDER BY d)"),
                "l'array deve restare deduplicato e ordinato: " + sql);
        assertTrue(sql.contains("d <> to_char(DATE_EVENT, 'YYYY-MM-DD')"),
                "il giorno di nascita non va mai in DATE_EVENTS: " + sql);
        assertFalse(sql.contains("x::date"),
                "nessun cast a date: un elemento malformato aborterebbe l'intero batch: " + sql);

        PreparedStatement ps = mock(PreparedStatement.class);
        setterCaptor.getValue().setValues(ps, 0);
        // la data dell'evento corrente e' il candidato giorno aggiuntivo, non un nuovo DATE_EVENT
        verify(ps, times(2)).setString(anyInt(), eq("2026-09-11"));
        verify(ps).setInt(4, 4242);

        // La cache "finestra 24h" indicizza per timestamp di NASCITA: l'update non deve inserirvi
        // il timestamp dell'evento, altrimenti la finestra scorre in memoria (accorpamento a catena).
        assertNull(cache.findPositionInWindow("NAV-1", "PA-1", LocalDateTime.parse("2026-09-11T06:23:07")));
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

    @Test
    void tokenWriteNeverRewritesThePartitionKey() throws Exception {
        // L'invariante DATE_EVENT = date(INSERTED_TIMESTAMP) e' cio' che consente a ReportWindowSql di
        // potare le partizioni mensili. Regge solo finche' la riga token nasce una volta sola: nessun
        // ramo di UPDATE deve comparire, tantomeno uno che riscriva la chiave di partizionamento.
        PositionTokens positionToken = new PositionTokens();
        positionToken.setToken("token-abc".getBytes(StandardCharsets.UTF_8));
        positionToken.setDateEvent(LocalDate.parse("2026-05-07"));
        positionToken.setInsertedTimestamp(LocalDateTime.parse("2026-05-07T09:15:00"));
        positionToken.setId(4242); // anche con l'id valorizzato la scrittura resta insert-only

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.batchUpdate(sqlCaptor.capture(), any(BatchPreparedStatementSetter.class)))
                .thenReturn(new int[] {1});

        bulkWriter.writeBulk(EntityName.POSITION_TOKENS, List.of(positionToken), "run-1", new BatchLocalCache());

        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("POSITION_TOKEN_REGISTRY"),
                "la scrittura deve passare dall'insert registry-gated (first-write-wins): " + sql);
        assertFalse(sql.contains("SET DATE_EVENT"),
                "DATE_EVENT e' la chiave di partizionamento e la data di nascita: mai riscriverla: " + sql);
        assertFalse(sql.contains("POSITION_TOKENS SET"),
                "nessun ramo di UPDATE su POSITION_TOKENS: la riga nasce una volta sola: " + sql);
    }

    @Test
    void transferWriteNeverRewritesThePartitionKey() throws Exception {
        // Su POSITION_TRANSFERS l'idempotenza e' data dall'ON CONFLICT sulla chiave naturale, che
        // include DATE_EVENT: il refresh avviene percio' solo a parita' di giorno e non puo' spostare
        // la riga di partizione ne' rompere l'invariante con INSERTED_TIMESTAMP.
        PositionTransfers tr = transfer(11, "PA1", (short) 1, LocalDate.parse("2026-03-23"), "IBAN-1");
        tr.setId(99); // un id valorizzato non deve piu' dirottare su un UPDATE

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.batchUpdate(sqlCaptor.capture(), any(BatchPreparedStatementSetter.class)))
                .thenReturn(new int[] {1});

        bulkWriter.writeBulk(EntityName.POSITION_TRANSFERS, List.of(tr), "run-1", null);

        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("ON CONFLICT (FK_TOKEN, PA_TRANSFER, ID_TRANSFER, DATE_EVENT)"),
                "DATE_EVENT deve restare nella chiave di conflitto: " + sql);
        assertFalse(sql.contains("SET DATE_EVENT"), sql);
        assertFalse(sql.contains("DATE_EVENT = EXCLUDED.DATE_EVENT"), sql);
    }

    @Test
    void clampsOversizedTextColumnToVarchar255AtWriteBoundary() throws Exception {
        // A single oversized ADX free-text value must NOT reach the INSERT unclamped: otherwise the
        // whole chunk fails with "value too long for type character varying(255)" and the entity stalls.
        String oversized = "x".repeat(400);
        ExtraInfo extraInfo = ExtraInfo.builder()
                .dateEvent(LocalDate.parse("2026-08-14"))
                .infoName("desc")
                .infoValue(oversized)
                .build();

        ArgumentCaptor<BatchPreparedStatementSetter> setterCaptor =
                ArgumentCaptor.forClass(BatchPreparedStatementSetter.class);
        when(jdbcTemplate.batchUpdate(anyString(), setterCaptor.capture())).thenReturn(new int[] {1});

        bulkWriter.writeBulk(EntityName.EXTRA_INFO, List.of(extraInfo), "run-1", null);

        // Drive the captured setter against a mock statement to inspect the bound values.
        PreparedStatement ps = mock(PreparedStatement.class);
        setterCaptor.getValue().setValues(ps, 0);

        ArgumentCaptor<String> infoValueCaptor = ArgumentCaptor.forClass(String.class);
        verify(ps).setString(eq(4), infoValueCaptor.capture()); // INFO_VALUE is bind position 4
        assertEquals(255, infoValueCaptor.getValue().length());
        assertEquals(oversized.substring(0, 255), infoValueCaptor.getValue());
    }

    // --- POSITION_TRANSFERS intra-batch dedup (ON CONFLICT DO UPDATE non puo' toccare 2 volte la stessa riga) ---

    @Test
    void dedupTransfersRemovesIntraBatchConflictKeepingLastOccurrence() {
        PositionTransfers first = transfer(11, "PA1", (short) 1, LocalDate.parse("2026-03-23"), "IBAN-OLD");
        PositionTransfers second = transfer(11, "PA1", (short) 1, LocalDate.parse("2026-03-23"), "IBAN-NEW");
        PositionTransfers other = transfer(12, "PA2", (short) 1, LocalDate.parse("2026-03-23"), "IBAN-X");

        List<PositionTransfers> result = BulkWriterImpl.dedupTransfersByConflictKey(List.of(first, second, other));

        assertEquals(2, result.size());
        PositionTransfers deduped = result.stream()
                .filter(t -> Integer.valueOf(11).equals(t.getFkToken()))
                .findFirst().orElseThrow();
        assertEquals("IBAN-NEW", deduped.getIbanTransfer(), "deve vincere l'ultima occorrenza (last-write-wins)");
    }

    @Test
    void dedupTransfersKeepsRowsWithNullKeyComponent() {
        // fk_token null -> NULL distinti nell'unique index a DB -> non deduplicare
        PositionTransfers a = transfer(null, "PA1", (short) 1, LocalDate.parse("2026-03-23"), "IBAN-A");
        PositionTransfers b = transfer(null, "PA1", (short) 1, LocalDate.parse("2026-03-23"), "IBAN-B");

        List<PositionTransfers> result = BulkWriterImpl.dedupTransfersByConflictKey(List.of(a, b));

        assertEquals(2, result.size());
    }

    @Test
    void dedupTransfersKeepsDistinctKeys() {
        PositionTransfers a = transfer(1, "PA1", (short) 1, LocalDate.parse("2026-03-23"), "IBAN-A");
        PositionTransfers b = transfer(1, "PA1", (short) 2, LocalDate.parse("2026-03-23"), "IBAN-B"); // id_transfer diverso
        PositionTransfers c = transfer(1, "PA1", (short) 1, LocalDate.parse("2026-03-24"), "IBAN-C"); // date_event diverso

        List<PositionTransfers> result = BulkWriterImpl.dedupTransfersByConflictKey(List.of(a, b, c));

        assertEquals(3, result.size());
    }

    @Test
    void writeBulkTransfersDedupsIntraBatchBeforeInsert() throws Exception {
        // Wiring end-to-end: writeBulk deve passare all'INSERT ON CONFLICT DO UPDATE un batch gia'
        // deduplicato, altrimenti PostgreSQL fallisce con "cannot affect row a second time".
        PositionTransfers a = transfer(11, "PA1", (short) 1, LocalDate.parse("2026-03-23"), "IBAN-OLD");
        PositionTransfers b = transfer(11, "PA1", (short) 1, LocalDate.parse("2026-03-23"), "IBAN-NEW"); // stessa chiave di a
        PositionTransfers c = transfer(12, "PA2", (short) 1, LocalDate.parse("2026-03-23"), "IBAN-X");

        ArgumentCaptor<BatchPreparedStatementSetter> setterCaptor =
                ArgumentCaptor.forClass(BatchPreparedStatementSetter.class);
        when(jdbcTemplate.batchUpdate(anyString(), setterCaptor.capture())).thenReturn(new int[] {1, 1});

        bulkWriter.writeBulk(EntityName.POSITION_TRANSFERS, List.of(a, b, c), "run-1", null);

        assertEquals(2, setterCaptor.getValue().getBatchSize(),
                "il batch verso il DB deve essere deduplicato (2 righe distinte, non 3)");
    }

    private static PositionTransfers transfer(Integer fkToken, String paTransfer, Short idTransfer,
                                              LocalDate dateEvent, String iban) {
        PositionTransfers t = new PositionTransfers();
        t.setFkToken(fkToken);
        t.setPaTransfer(paTransfer);
        t.setIdTransfer(idTransfer);
        t.setDateEvent(dateEvent);
        t.setIbanTransfer(iban);
        return t;
    }
}
