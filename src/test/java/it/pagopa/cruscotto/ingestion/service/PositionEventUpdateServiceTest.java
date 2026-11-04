package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.EventsWf;
import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import it.pagopa.cruscotto.ingestion.repository.PositionTokensRepository;
import it.pagopa.cruscotto.ingestion.repository.PositionTransfersRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PositionEventUpdateServiceTest {

    @Mock
    private PositionTokensRepository positionTokensRepository;

    @Mock
    private PositionTransfersRepository positionTransfersRepository;

    @Mock
    private AnagraficaService anagraficaService;

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private DbSchemaConfig dbSchemaConfig;

    @InjectMocks
    private PositionEventUpdateService positionEventUpdateService;

    @Test
    void shouldUpdatePositionWithAtomicStatementWithoutReadingIt() throws Exception {
        // L'aggiornamento non deve rileggere la POSITION: il job POSITION gira in parallelo e un
        // read-modify-write dell'array perderebbe i giorni aggiunti nel frattempo.
        when(dbSchemaConfig.getSchemaName()).thenReturn("ingestor");
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<BatchPreparedStatementSetter> setterCaptor =
                ArgumentCaptor.forClass(BatchPreparedStatementSetter.class);
        when(jdbcTemplate.batchUpdate(sqlCaptor.capture(), setterCaptor.capture())).thenReturn(new int[] {1});

        EventsWf event = new EventsWf();
        event.setFkPosition(10);
        event.setDateEvent(LocalDate.parse("2026-04-13"));
        event.setInsertedTimestampResp(LocalDateTime.parse("2026-04-13T10:15:00"));

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        String sql = sqlCaptor.getValue();
        assertTrue(sql.startsWith("UPDATE ingestor.POSITION SET"), sql);
        assertTrue(sql.contains("GREATEST(LAST_EVENT"), "LAST_EVENT non deve regredire: " + sql);
        assertFalse(sql.contains("DATE_EVENT = ?"),
                "la data di nascita non va mai riscritta: " + sql);
        assertFalse(sql.contains("INSERTED_TIMESTAMP = ?"),
                "la data di nascita non va mai riscritta: " + sql);
        assertTrue(sql.contains("d <> to_char(DATE_EVENT, 'YYYY-MM-DD')"),
                "il giorno di nascita resta escluso dall'array: " + sql);

        PreparedStatement ps = mock(PreparedStatement.class);
        setterCaptor.getValue().setValues(ps, 0);
        // formato ISO richiesto dal requisito, non piu' BASIC_ISO
        verify(ps).setString(2, "2026-04-13");
        verify(ps).setString(3, "2026-04-13");
        verify(ps).setInt(4, 10);
    }

    @Test
    void shouldAdvanceLastEventEvenWhenTheEventCarriesNoDate() throws Exception {
        when(dbSchemaConfig.getSchemaName()).thenReturn("ingestor");
        ArgumentCaptor<BatchPreparedStatementSetter> setterCaptor =
                ArgumentCaptor.forClass(BatchPreparedStatementSetter.class);
        when(jdbcTemplate.batchUpdate(anyString(), setterCaptor.capture())).thenReturn(new int[] {1});

        EventsWf event = new EventsWf();
        event.setFkPosition(11);
        event.setInsertedTimestampResp(LocalDateTime.parse("2026-04-13T10:15:00"));

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        PreparedStatement ps = mock(PreparedStatement.class);
        setterCaptor.getValue().setValues(ps, 0);
        // data assente: l'array resta intatto (il CASE sul bind NULL), ma LAST_EVENT avanza
        verify(ps).setString(2, null);
        verify(ps).setInt(4, 11);
    }


    @Test
    void shouldUpdateTokenFromSendPaymentOutcomeEvent() {
        PositionTokens token = new PositionTokens();
        token.setId(55);
        token.setTouchpoint("Touchpoint PSP");
        token.setPaymentDate(null);
        token.setOutcome(null);

        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "REQ/RESP")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "REQ/RESP")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "REQ/RESP")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "REQ/RESP")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "REQ/RESP")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "")).thenReturn(106L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "REQ/RESP")).thenReturn(106L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO")).thenReturn(201L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO_KO")).thenReturn(202L);
        when(positionTokensRepository.findAllById(any())).thenReturn(List.of(token));
        when(positionTransfersRepository.findByFkTokenInOrderByFkTokenAscIdDesc(any())).thenReturn(List.of());
        when(positionTokensRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        EventsWf event = new EventsWf();
        event.setFkTokens(55);
        event.setTipoEvento((short) 101);
        event.setOutcomeResp("OK");
        event.setOutcomeReq("OK");
        event.setInsertedTimestampReq(LocalDateTime.parse("2026-04-13T10:15:00"));
        event.setPaymentMethod("CP");

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        ArgumentCaptor<List<PositionTokens>> tokenCaptor = ArgumentCaptor.forClass(List.class);
        verify(positionTokensRepository).saveAll(tokenCaptor.capture());
        PositionTokens savedToken = tokenCaptor.getValue().get(0);

        assertEquals("OK", savedToken.getOutcome());
        assertEquals(LocalDateTime.parse("2026-04-13T10:15:00"), savedToken.getPaymentDate());
        assertEquals("CP", savedToken.getPaymentMethod());
    }

    @Test
    void shouldNotOverwriteExistingPaymentDateOnSendPaymentOutcome() {
        // Contratto chiave: l'enrichment valorizza payment_date SOLO se null. Questo e' il motivo per cui
        // l'insert del token deve lasciare payment_date null (fix del fallback a INSERTED_TIMESTAMP):
        // se fosse gia' valorizzato (es. con la data di creazione), la data reale di pagamento non verrebbe
        // mai scritta. Qui verifichiamo che una payment_date preesistente NON venga sovrascritta.
        PositionTokens token = new PositionTokens();
        token.setId(55);
        token.setTouchpoint("Touchpoint PSP");
        LocalDateTime preexisting = LocalDateTime.parse("2026-01-01T00:00:00");
        token.setPaymentDate(preexisting);
        token.setOutcome(null);

        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "REQ/RESP")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "REQ/RESP")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "REQ/RESP")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "REQ/RESP")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "REQ/RESP")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "")).thenReturn(106L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "REQ/RESP")).thenReturn(106L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO")).thenReturn(201L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO_KO")).thenReturn(202L);
        when(positionTokensRepository.findAllById(any())).thenReturn(List.of(token));
        when(positionTransfersRepository.findByFkTokenInOrderByFkTokenAscIdDesc(any())).thenReturn(List.of());
        when(positionTokensRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        EventsWf event = new EventsWf();
        event.setFkTokens(55);
        event.setTipoEvento((short) 101);
        event.setOutcomeResp("OK");
        event.setOutcomeReq("OK");
        event.setInsertedTimestampReq(LocalDateTime.parse("2026-04-13T10:15:00"));
        event.setPaymentMethod("CP");

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        ArgumentCaptor<List<PositionTokens>> tokenCaptor = ArgumentCaptor.forClass(List.class);
        verify(positionTokensRepository).saveAll(tokenCaptor.capture());
        PositionTokens savedToken = tokenCaptor.getValue().get(0);

        // outcome viene comunque aggiornato (prova che l'evento e' stato processato), ma payment_date resta quella preesistente
        assertEquals("OK", savedToken.getOutcome());
        assertEquals(preexisting, savedToken.getPaymentDate());
    }

    @Test
    void shouldNotOverwriteExistingPaymentMethodOnSendPaymentOutcome() {
        // Requisito SPO, entrambi i casi: "Valorizzare solo se e' null". Il primo evento che porta il
        // metodo di pagamento vince, i successivi non lo riallineano.
        PositionTokens token = new PositionTokens();
        token.setId(55);
        token.setTouchpoint("Touchpoint PSP");
        token.setPaymentMethod("ESISTENTE");

        stubSpoAnagrafiche(token);

        EventsWf event = new EventsWf();
        event.setFkTokens(55);
        event.setTipoEvento((short) 101);
        event.setOutcomeResp("OK");
        event.setOutcomeReq("OK");
        event.setInsertedTimestampReq(LocalDateTime.parse("2026-04-13T10:15:00"));
        event.setPaymentMethod("NUOVO");

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        ArgumentCaptor<List<PositionTokens>> tokenCaptor = ArgumentCaptor.forClass(List.class);
        verify(positionTokensRepository).saveAll(tokenCaptor.capture());
        assertEquals("ESISTENTE", tokenCaptor.getValue().get(0).getPaymentMethod());
    }

    @Test
    void outcomeIsNotClearedByAnEventWithoutOutcomeReq() {
        // Gli eventi SPO sono applicati in sequenza su un token gia' persistito e OUTCOME, a
        // differenza di PAYMENT_DATE, non e' first-write-wins. Un evento senza OUTCOME_REQ non deve
        // pero' cancellare un 'OK' gia' registrato: il requisito parla del "valore di OUTCOME_REQ
        // presente nell'evento". Senza il guard il token restava con PAYMENT_DATE valorizzata e
        // OUTCOME nullo, cioe' incassato e non incassato insieme, e il report Position mostrava
        // DATE_PAYED con IS_PAYED='false'.
        PositionTokens token = new PositionTokens();
        token.setId(55);
        token.setTouchpoint("Touchpoint PSP");
        token.setOutcome("OK");
        LocalDateTime paid = LocalDateTime.parse("2026-01-01T00:00:00");
        token.setPaymentDate(paid);

        stubSpoAnagrafiche(token);

        EventsWf event = new EventsWf();
        event.setFkTokens(55);
        event.setTipoEvento((short) 101);
        event.setOutcomeResp("OK");
        event.setOutcomeReq(null);
        event.setInsertedTimestampReq(LocalDateTime.parse("2026-04-13T10:15:00"));

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        // Nessuna modifica da applicare: il token non viene nemmeno riscritto.
        verify(positionTokensRepository, never()).saveAll(any());
        assertEquals("OK", token.getOutcome());
        assertEquals(paid, token.getPaymentDate());
    }

    @Test
    void expiredTokenWithOutcomeReqKoStillSetsPaymentMethod() {
        // Requisito Caso 2, punto 3 "NON IN AND con il punto 2": nel ramo OUTCOME_RESP = KO il metodo di
        // pagamento e' indipendente dalla data. Senza OUTCOME_REQ = OK non c'e' incasso (payment_date
        // resta nulla) ma payment_method va comunque valorizzato.
        PositionTokens token = new PositionTokens();
        token.setId(55);
        token.setTouchpoint("Touchpoint PSP");

        stubSpoAnagrafiche(token);

        EventsWf event = new EventsWf();
        event.setFkTokens(55);
        event.setTipoEvento((short) 101);
        event.setOutcomeResp("KO");
        event.setOutcomeReq("KO");
        event.setFaultCode((short) 201);
        event.setInsertedTimestampReq(LocalDateTime.parse("2026-04-13T10:15:00"));
        event.setPaymentMethod("CP");

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        ArgumentCaptor<List<PositionTokens>> tokenCaptor = ArgumentCaptor.forClass(List.class);
        verify(positionTokensRepository).saveAll(tokenCaptor.capture());
        PositionTokens savedToken = tokenCaptor.getValue().get(0);

        assertEquals("KO", savedToken.getOutcome());
        assertNull(savedToken.getPaymentDate());
        assertEquals("CP", savedToken.getPaymentMethod());
    }

    /** Stub comune per i test delle regole sendPaymentOutcome. */
    private void stubSpoAnagrafiche(PositionTokens token) {
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "REQ/RESP")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "REQ/RESP")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "REQ/RESP")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "REQ/RESP")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "REQ/RESP")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "")).thenReturn(106L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "REQ/RESP")).thenReturn(106L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO")).thenReturn(201L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO_KO")).thenReturn(202L);
        when(positionTokensRepository.findAllById(any())).thenReturn(List.of(token));
        when(positionTransfersRepository.findByFkTokenInOrderByFkTokenAscIdDesc(any())).thenReturn(List.of());
        // lenient: i test che verificano la non-modifica non arrivano a salvare.
        lenient().when(positionTokensRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));
    }

    @Test
    void shouldUpdateCreditorRefIdFromActivatePaymentNoticeWhenDifferentFromIuv() {
        PositionTokens token = new PositionTokens();
        token.setId(77);
        token.setIuv("IUV-0001");
        token.setCreditorRefId(null);

        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "REQ/RESP")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "REQ/RESP")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "REQ/RESP")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "REQ/RESP")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "REQ/RESP")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "")).thenReturn(106L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "REQ/RESP")).thenReturn(106L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO")).thenReturn(201L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO_KO")).thenReturn(202L);
        when(positionTokensRepository.findAllById(any())).thenReturn(List.of(token));
        when(positionTransfersRepository.findByFkTokenInOrderByFkTokenAscIdDesc(any())).thenReturn(List.of());
        when(positionTokensRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        EventsWf event = new EventsWf();
        event.setFkTokens(77);
        event.setTipoEvento((short) 103);
        event.setOutcomeResp("OK");
        event.setCreditorRefId("CR-12345");

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        ArgumentCaptor<List<PositionTokens>> tokenCaptor = ArgumentCaptor.forClass(List.class);
        verify(positionTokensRepository).saveAll(tokenCaptor.capture());
        PositionTokens savedToken = tokenCaptor.getValue().get(0);

        assertEquals("CR-12345", savedToken.getCreditorRefId());
    }

    @Test
    void shouldClearCreditorRefIdWhenActivatePaymentNoticeMatchesIuv() {
        PositionTokens token = new PositionTokens();
        token.setId(88);
        token.setIuv("IUV-0001");
        token.setCreditorRefId("OLD-CREDITOR");

        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "REQ/RESP")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "REQ/RESP")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "REQ/RESP")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "REQ/RESP")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "REQ/RESP")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "")).thenReturn(106L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "REQ/RESP")).thenReturn(106L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO")).thenReturn(201L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO_KO")).thenReturn(202L);
        when(positionTokensRepository.findAllById(any())).thenReturn(List.of(token));
        when(positionTransfersRepository.findByFkTokenInOrderByFkTokenAscIdDesc(any())).thenReturn(List.of());
        when(positionTokensRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        EventsWf event = new EventsWf();
        event.setFkTokens(88);
        event.setTipoEvento((short) 103);
        event.setOutcomeResp("OK");
        event.setCreditorRefId("IUV-0001");

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        ArgumentCaptor<List<PositionTokens>> tokenCaptor = ArgumentCaptor.forClass(List.class);
        verify(positionTokensRepository).saveAll(tokenCaptor.capture());
        PositionTokens savedToken = tokenCaptor.getValue().get(0);

        assertNull(savedToken.getCreditorRefId());
    }

    @Test
    void shouldUpdateTokenAndTransfersFromPspNotifyPaymentEvent() {
        PositionTokens token = new PositionTokens();
        token.setId(99);
        token.setOutcome("OK");

        PositionTransfers transfer = new PositionTransfers();
        transfer.setId(1001);
        transfer.setFkToken(99);

        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "REQ/RESP")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "REQ/RESP")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "REQ/RESP")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "REQ/RESP")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "REQ/RESP")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "")).thenReturn(106L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "REQ/RESP")).thenReturn(106L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO")).thenReturn(201L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO_KO")).thenReturn(202L);
        when(positionTokensRepository.findAllById(any())).thenReturn(List.of(token));
        when(positionTransfersRepository.findByFkTokenInOrderByFkTokenAscIdDesc(any())).thenReturn(List.of(transfer));
        when(positionTokensRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(positionTransfersRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        EventsWf event = new EventsWf();
        event.setFkTokens(99);
        event.setTipoEvento((short) 105);
        event.setOutcomeResp("OK");
        event.setPsp((short) 10);
        event.setIntermediarioPsp((short) 11);
        event.setCanale((short) 12);

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        ArgumentCaptor<List<PositionTokens>> tokenCaptor = ArgumentCaptor.forClass(List.class);
        verify(positionTokensRepository).saveAll(tokenCaptor.capture());
        PositionTokens savedToken = tokenCaptor.getValue().get(0);
        assertEquals((short) 10, savedToken.getPsp());
        assertEquals((short) 11, savedToken.getIntermediarioPsp());
        assertEquals((short) 12, savedToken.getCanale());

        ArgumentCaptor<List<PositionTransfers>> transfersCaptor = ArgumentCaptor.forClass(List.class);
        verify(positionTransfersRepository).saveAll(transfersCaptor.capture());
        PositionTransfers savedTransfer = transfersCaptor.getValue().get(0);
        assertEquals((short) 10, savedTransfer.getPsp());
        assertEquals((short) 11, savedTransfer.getIntermediarioPsp());
        assertEquals((short) 12, savedTransfer.getCanale());
    }

    @Test
    void shouldSetOutcomeKoFromPspNotifyPaymentKoWhenTokenOutcomeBlank() {
        PositionTokens token = new PositionTokens();
        token.setId(111);
        token.setOutcome("   ");

        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "REQ/RESP")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "REQ/RESP")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "REQ/RESP")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "REQ/RESP")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "REQ/RESP")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "")).thenReturn(106L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "REQ/RESP")).thenReturn(106L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO")).thenReturn(201L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO_KO")).thenReturn(202L);
        when(positionTokensRepository.findAllById(any())).thenReturn(List.of(token));
        when(positionTransfersRepository.findByFkTokenInOrderByFkTokenAscIdDesc(any())).thenReturn(List.of());
        when(positionTokensRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        EventsWf event = new EventsWf();
        event.setFkTokens(111);
        event.setTipoEvento((short) 105);
        event.setOutcomeResp("KO");

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        ArgumentCaptor<List<PositionTokens>> tokenCaptor = ArgumentCaptor.forClass(List.class);
        verify(positionTokensRepository).saveAll(tokenCaptor.capture());
        PositionTokens savedToken = tokenCaptor.getValue().get(0);
        assertEquals("KO", savedToken.getOutcome());
    }

    @Test
    void shouldUpdateTokenFromClosePaymentWhenReqOkAndRespOk() {
        PositionTokens token = new PositionTokens();
        token.setId(120);
        token.setPaymentMethod("OLD");
        token.setPsp((short) 1);
        token.setIntermediarioPsp((short) 2);
        token.setCanale((short) 3);

        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "REQ/RESP")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "REQ/RESP")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "REQ/RESP")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "REQ/RESP")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "REQ/RESP")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "")).thenReturn(106L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "REQ/RESP")).thenReturn(106L);
        when(anagraficaService.resolveEventoId("run-evt-update", "closePayment", "")).thenReturn(107L);
        when(anagraficaService.resolveEventoId("run-evt-update", "closePayment", "REQ/RESP")).thenReturn(107L);
        when(anagraficaService.resolveEventoId("run-evt-update", "closePayment-v2", "")).thenReturn(108L);
        when(anagraficaService.resolveEventoId("run-evt-update", "closePayment-v2", "REQ/RESP")).thenReturn(108L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO")).thenReturn(201L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO_KO")).thenReturn(202L);
        when(positionTokensRepository.findAllById(any())).thenReturn(List.of(token));
        when(positionTransfersRepository.findByFkTokenInOrderByFkTokenAscIdDesc(any())).thenReturn(List.of());
        when(positionTokensRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        EventsWf event = new EventsWf();
        event.setFkTokens(120);
        event.setTipoEvento((short) 107);
        event.setOutcomeReq("OK");
        event.setOutcomeResp("OK");
        event.setPaymentMethod("NEW-PM");
        event.setPsp((short) 10);
        event.setIntermediarioPsp((short) 11);
        event.setCanale((short) 12);

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        ArgumentCaptor<List<PositionTokens>> tokenCaptor = ArgumentCaptor.forClass(List.class);
        verify(positionTokensRepository).saveAll(tokenCaptor.capture());
        PositionTokens savedToken = tokenCaptor.getValue().get(0);
        assertEquals("NEW-PM", savedToken.getPaymentMethod());
        assertEquals((short) 10, savedToken.getPsp());
        assertEquals((short) 11, savedToken.getIntermediarioPsp());
        assertEquals((short) 12, savedToken.getCanale());
    }

    @Test
    void shouldSetOutcomeKoFromClosePaymentWhenReqKoRespOkAndOutcomeBlank() {
        PositionTokens token = new PositionTokens();
        token.setId(121);
        token.setOutcome(" ");

        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", "REQ/RESP")).thenReturn(101L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", "REQ/RESP")).thenReturn(102L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", "REQ/RESP")).thenReturn(103L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", "REQ/RESP")).thenReturn(104L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", "REQ/RESP")).thenReturn(105L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "")).thenReturn(106L);
        when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", "REQ/RESP")).thenReturn(106L);
        when(anagraficaService.resolveEventoId("run-evt-update", "closePayment", "")).thenReturn(107L);
        when(anagraficaService.resolveEventoId("run-evt-update", "closePayment", "REQ/RESP")).thenReturn(107L);
        when(anagraficaService.resolveEventoId("run-evt-update", "closePayment-v2", "")).thenReturn(108L);
        when(anagraficaService.resolveEventoId("run-evt-update", "closePayment-v2", "REQ/RESP")).thenReturn(108L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO")).thenReturn(201L);
        when(anagraficaService.resolveFaultCodeId("run-evt-update", "PPT_TOKEN_SCADUTO_KO")).thenReturn(202L);
        when(positionTokensRepository.findAllById(any())).thenReturn(List.of(token));
        when(positionTransfersRepository.findByFkTokenInOrderByFkTokenAscIdDesc(any())).thenReturn(List.of());
        when(positionTokensRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        EventsWf event = new EventsWf();
        event.setFkTokens(121);
        event.setTipoEvento((short) 108);
        event.setOutcomeReq("KO");
        event.setOutcomeResp("OK");

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        ArgumentCaptor<List<PositionTokens>> tokenCaptor = ArgumentCaptor.forClass(List.class);
        verify(positionTokensRepository).saveAll(tokenCaptor.capture());
        PositionTokens savedToken = tokenCaptor.getValue().get(0);
        assertEquals("KO", savedToken.getOutcome());
    }

    // ------------------------------------------------------------------------------------
    // IS_CART — il carrello si riconosce dallo split della closePayment, non da ID_CARRELLO
    // ------------------------------------------------------------------------------------

    /** Tipi evento stubbati come li risolve il servizio. closePayment-v2 e' il 108. */
    private void stubEventoIds() {
        for (String sottoTipo : List.of("", "REQ/RESP")) {
            lenient().when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcome", sottoTipo)).thenReturn(101L);
            lenient().when(anagraficaService.resolveEventoId("run-evt-update", "sendPaymentOutcomeV2", sottoTipo)).thenReturn(102L);
            lenient().when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNotice", sottoTipo)).thenReturn(103L);
            lenient().when(anagraficaService.resolveEventoId("run-evt-update", "activatePaymentNoticeV2", sottoTipo)).thenReturn(104L);
            lenient().when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPayment", sottoTipo)).thenReturn(105L);
            lenient().when(anagraficaService.resolveEventoId("run-evt-update", "pspNotifyPaymentV2", sottoTipo)).thenReturn(106L);
            lenient().when(anagraficaService.resolveEventoId("run-evt-update", "closePayment", sottoTipo)).thenReturn(107L);
            lenient().when(anagraficaService.resolveEventoId("run-evt-update", "closePayment-v2", sottoTipo)).thenReturn(108L);
        }
    }

    private EventsWf closePaymentEvent(String eventIdReq, Integer fkToken) {
        EventsWf event = new EventsWf();
        event.setFkTokens(fkToken);
        event.setTipoEvento((short) 108);
        event.setEventIdReq(eventIdReq);
        return event;
    }

    /**
     * Un carrello e' un solo evento di chiusura che ADX splitta in N righe, una per token: lo stesso
     * {@code EVENT_ID_REQ} che compare su piu' token distinti. Tutti quei token sono carrello.
     */
    @Test
    void unaClosePaymentSuPiuTokenLiMarcaTuttiComeCarrello() {
        PositionTokens primo = new PositionTokens();
        primo.setId(301);
        PositionTokens secondo = new PositionTokens();
        secondo.setId(302);

        stubEventoIds();
        when(positionTokensRepository.findAllById(any())).thenReturn(List.of(primo, secondo));
        when(positionTransfersRepository.findByFkTokenInOrderByFkTokenAscIdDesc(any())).thenReturn(List.of());
        when(positionTokensRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(
                closePaymentEvent("2026-10-14_carrello", 301),
                closePaymentEvent("2026-10-14_carrello", 302)));

        ArgumentCaptor<List<PositionTokens>> captor = ArgumentCaptor.forClass(List.class);
        verify(positionTokensRepository).saveAll(captor.capture());
        assertEquals(2, captor.getValue().size(), "entrambi i token del carrello vanno aggiornati");
        captor.getValue().forEach(token ->
                assertTrue(Boolean.TRUE.equals(token.getIsCart()),
                        "ogni token dello split e' un carrello, non solo il primo"));
    }

    /**
     * Un pagamento singolo non viene marcato, e soprattutto <strong>non viene scritto</strong>:
     * scrivere {@code false} costringerebbe a un UPDATE su ogni token che riceve un evento, anche
     * quando non e' cambiato nulla. {@code null} vale "non e' un carrello".
     */
    @Test
    void unPagamentoSingoloNonVieneMarcatoNeScritto() {
        PositionTokens token = new PositionTokens();
        token.setId(310);

        stubEventoIds();
        when(positionTokensRepository.findAllById(any())).thenReturn(List.of(token));
        when(positionTransfersRepository.findByFkTokenInOrderByFkTokenAscIdDesc(any())).thenReturn(List.of());

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(
                closePaymentEvent("2026-10-14_singolo", 310)));

        assertNull(token.getIsCart(), "un solo token sull'evento non e' un carrello");
        verify(positionTokensRepository, never()).saveAll(any());
    }

    /**
     * Regola monotona: un token gia' marcato non torna indietro. Una finestra successiva puo' vedere
     * una sola riga del gruppo e concludere erroneamente "singolo"; il contrario non e' possibile.
     * Stessa classe di difetto di {@code setOutcomeIfPresent}, dove un evento successivo cancellava
     * un dato valido.
     */
    @Test
    void ilFlagDiCarrelloNonTornaIndietro() {
        PositionTokens token = new PositionTokens();
        token.setId(320);
        token.setIsCart(Boolean.TRUE);

        stubEventoIds();
        when(positionTokensRepository.findAllById(any())).thenReturn(List.of(token));
        when(positionTransfersRepository.findByFkTokenInOrderByFkTokenAscIdDesc(any())).thenReturn(List.of());

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(
                closePaymentEvent("2026-10-15_altra_finestra", 320)));

        assertTrue(Boolean.TRUE.equals(token.getIsCart()), "il flag non puo' essere revocato");
        verify(positionTokensRepository, never()).saveAll(any());
    }

    /**
     * Un pagamento singolo le cui righe ADX sono duplicate <strong>non</strong> e' un carrello.
     *
     * <p>Non e' un caso di scuola: sulla sorgente sono state osservate 4 righe REQ per 2 soli token
     * su una stessa closePayment, e il cliente ha confermato che i duplicati esatti sono un difetto
     * di ingestion lato ADX. Se il raggruppamento contasse le <em>righe</em> invece dei
     * <em>token distinti</em>, ogni pagamento singolo duplicato verrebbe marcato come carrello — e
     * nei report la sua commissione risulterebbe "condivisa" quando non lo e'.</p>
     */
    @Test
    void unaRigaDuplicataDaAdxNonCreaUnCarrelloFinto() {
        PositionTokens token = new PositionTokens();
        token.setId(350);

        stubEventoIds();
        when(positionTokensRepository.findAllById(any())).thenReturn(List.of(token));
        when(positionTransfersRepository.findByFkTokenInOrderByFkTokenAscIdDesc(any())).thenReturn(List.of());

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(
                closePaymentEvent("2026-10-14_duplicato", 350),
                closePaymentEvent("2026-10-14_duplicato", 350)));

        assertNull(token.getIsCart(),
                "due righe sullo stesso token sono un duplicato ADX, non due avvisi");
        verify(positionTokensRepository, never()).saveAll(any());
    }

    /**
     * Un token gia' marcato non viene riscritto quando lo stesso carrello ripassa: senza la guardia
     * ogni rilettura dell'evento produrrebbe un UPDATE inutile su tutti gli N token del gruppo.
     *
     * <p>E' il caso che il test sulla monotonia <em>non</em> copre: li' il token gia' marcato vede
     * un pagamento singolo e si esce dal ramo {@code !isCart}, qui vede di nuovo un carrello e si
     * arriva alla guardia vera.</p>
     */
    @Test
    void nonRiscriveUnTokenGiaMarcatoComeCarrello() {
        PositionTokens giaMarcato = new PositionTokens();
        giaMarcato.setId(340);
        giaMarcato.setIsCart(Boolean.TRUE);
        PositionTokens nuovo = new PositionTokens();
        nuovo.setId(341);

        stubEventoIds();
        when(positionTokensRepository.findAllById(any())).thenReturn(List.of(giaMarcato, nuovo));
        when(positionTransfersRepository.findByFkTokenInOrderByFkTokenAscIdDesc(any())).thenReturn(List.of());
        when(positionTokensRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(
                closePaymentEvent("2026-10-14_stesso_carrello", 340),
                closePaymentEvent("2026-10-14_stesso_carrello", 341)));

        ArgumentCaptor<List<PositionTokens>> captor = ArgumentCaptor.forClass(List.class);
        verify(positionTokensRepository).saveAll(captor.capture());
        assertEquals(1, captor.getValue().size(),
                "solo il token non ancora marcato va scritto, l'altro e' gia' a posto");
        assertEquals(341, captor.getValue().get(0).getId());
    }

    /**
     * La fonte e' la closePayment e basta. Anche {@code sendPaymentOutcomeV2} viene splittata da
     * ADX, ma misurato su dev la closePayment e' un sovrainsieme stretto — 24.939 token contro
     * 21.330, con l'unione dei tre eventi pari esattamente a 24.939 — quindi aggiungere le altre
     * fonti non trova un carrello in piu'. Questo test fissa la scelta.
     */
    @Test
    void soloLaClosePaymentDecideIlCarrello() {
        PositionTokens primo = new PositionTokens();
        primo.setId(330);
        PositionTokens secondo = new PositionTokens();
        secondo.setId(331);

        stubEventoIds();
        when(positionTokensRepository.findAllById(any())).thenReturn(List.of(primo, secondo));
        when(positionTransfersRepository.findByFkTokenInOrderByFkTokenAscIdDesc(any())).thenReturn(List.of());

        EventsWf spoUno = new EventsWf();
        spoUno.setFkTokens(330);
        spoUno.setTipoEvento((short) 102);
        spoUno.setEventIdReq("2026-10-14_spo_splittata");
        EventsWf spoDue = new EventsWf();
        spoDue.setFkTokens(331);
        spoDue.setTipoEvento((short) 102);
        spoDue.setEventIdReq("2026-10-14_spo_splittata");

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(spoUno, spoDue));

        assertNull(primo.getIsCart(), "la sendPaymentOutcome non decide la carrellosita'");
        assertNull(secondo.getIsCart(), "la sendPaymentOutcome non decide la carrellosita'");
    }
}
