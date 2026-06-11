package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.EventsWf;
import it.pagopa.cruscotto.ingestion.entity.Position;
import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import it.pagopa.cruscotto.ingestion.repository.PositionRepository;
import it.pagopa.cruscotto.ingestion.repository.PositionTokensRepository;
import it.pagopa.cruscotto.ingestion.repository.PositionTransfersRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PositionEventUpdateServiceTest {

    @Mock
    private PositionRepository positionRepository;

    @Mock
    private PositionTokensRepository positionTokensRepository;

    @Mock
    private PositionTransfersRepository positionTransfersRepository;

    @Mock
    private AnagraficaService anagraficaService;

    @InjectMocks
    private PositionEventUpdateService positionEventUpdateService;

    @Test
    void shouldUpdateLastEventAndDateEventsInYyyyMmDdWithoutPositionDate() {
        Position position = new Position();
        position.setId(10);
        position.setDateEvent(LocalDate.parse("2026-04-12"));
        position.setDateEvents("[\"20260410\", \"20260412\"]");
        position.setLastEvent(LocalDateTime.parse("2026-04-12T08:00:00"));

        when(positionRepository.findById(10)).thenReturn(Optional.of(position));

        EventsWf event = new EventsWf();
        event.setFkPosition(10);
        event.setDateEvent(LocalDate.parse("2026-04-13"));
        event.setInsertedTimestampResp(LocalDateTime.parse("2026-04-13T10:15:00"));

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        ArgumentCaptor<Position> captor = ArgumentCaptor.forClass(Position.class);
        verify(positionRepository).save(captor.capture());
        Position saved = captor.getValue();

        assertEquals(LocalDateTime.parse("2026-04-13T10:15:00"), saved.getLastEvent());
        assertEquals("[\"20260410\", \"20260413\"]", saved.getDateEvents());
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
        when(positionTokensRepository.findById(55)).thenReturn(Optional.of(token));
        when(positionTransfersRepository.findByFkTokenOrderByIdDesc(55)).thenReturn(List.of());
        when(positionTokensRepository.save(any(PositionTokens.class))).thenAnswer(invocation -> invocation.getArgument(0));

        EventsWf event = new EventsWf();
        event.setFkTokens(55);
        event.setTipoEvento((short) 101);
        event.setOutcomeResp("OK");
        event.setOutcomeReq("OK");
        event.setInsertedTimestampReq(LocalDateTime.parse("2026-04-13T10:15:00"));
        event.setPaymentMethod("CP");

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        ArgumentCaptor<PositionTokens> tokenCaptor = ArgumentCaptor.forClass(PositionTokens.class);
        verify(positionTokensRepository).save(tokenCaptor.capture());
        PositionTokens savedToken = tokenCaptor.getValue();

        assertEquals("OK", savedToken.getOutcome());
        assertEquals(LocalDateTime.parse("2026-04-13T10:15:00"), savedToken.getPaymentDate());
        assertEquals("CP", savedToken.getPaymentMethod());
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
        when(positionTokensRepository.findById(77)).thenReturn(Optional.of(token));
        when(positionTransfersRepository.findByFkTokenOrderByIdDesc(77)).thenReturn(List.of());
        when(positionTokensRepository.save(any(PositionTokens.class))).thenAnswer(invocation -> invocation.getArgument(0));

        EventsWf event = new EventsWf();
        event.setFkTokens(77);
        event.setTipoEvento((short) 103);
        event.setOutcomeResp("OK");
        event.setCreditorRefId("CR-12345");

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        ArgumentCaptor<PositionTokens> tokenCaptor = ArgumentCaptor.forClass(PositionTokens.class);
        verify(positionTokensRepository).save(tokenCaptor.capture());
        PositionTokens savedToken = tokenCaptor.getValue();

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
        when(positionTokensRepository.findById(88)).thenReturn(Optional.of(token));
        when(positionTransfersRepository.findByFkTokenOrderByIdDesc(88)).thenReturn(List.of());
        when(positionTokensRepository.save(any(PositionTokens.class))).thenAnswer(invocation -> invocation.getArgument(0));

        EventsWf event = new EventsWf();
        event.setFkTokens(88);
        event.setTipoEvento((short) 103);
        event.setOutcomeResp("OK");
        event.setCreditorRefId("IUV-0001");

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        ArgumentCaptor<PositionTokens> tokenCaptor = ArgumentCaptor.forClass(PositionTokens.class);
        verify(positionTokensRepository).save(tokenCaptor.capture());
        PositionTokens savedToken = tokenCaptor.getValue();

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
        when(positionTokensRepository.findById(99)).thenReturn(Optional.of(token));
        when(positionTransfersRepository.findByFkTokenOrderByIdDesc(99)).thenReturn(List.of(transfer));
        when(positionTokensRepository.save(any(PositionTokens.class))).thenAnswer(invocation -> invocation.getArgument(0));
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

        ArgumentCaptor<PositionTokens> tokenCaptor = ArgumentCaptor.forClass(PositionTokens.class);
        verify(positionTokensRepository).save(tokenCaptor.capture());
        PositionTokens savedToken = tokenCaptor.getValue();
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
        when(positionTokensRepository.findById(111)).thenReturn(Optional.of(token));
        when(positionTransfersRepository.findByFkTokenOrderByIdDesc(111)).thenReturn(List.of());
        when(positionTokensRepository.save(any(PositionTokens.class))).thenAnswer(invocation -> invocation.getArgument(0));

        EventsWf event = new EventsWf();
        event.setFkTokens(111);
        event.setTipoEvento((short) 105);
        event.setOutcomeResp("KO");

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        ArgumentCaptor<PositionTokens> tokenCaptor = ArgumentCaptor.forClass(PositionTokens.class);
        verify(positionTokensRepository).save(tokenCaptor.capture());
        PositionTokens savedToken = tokenCaptor.getValue();
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
        when(positionTokensRepository.findById(120)).thenReturn(Optional.of(token));
        when(positionTransfersRepository.findByFkTokenOrderByIdDesc(120)).thenReturn(List.of());
        when(positionTokensRepository.save(any(PositionTokens.class))).thenAnswer(invocation -> invocation.getArgument(0));

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

        ArgumentCaptor<PositionTokens> tokenCaptor = ArgumentCaptor.forClass(PositionTokens.class);
        verify(positionTokensRepository).save(tokenCaptor.capture());
        PositionTokens savedToken = tokenCaptor.getValue();
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
        when(positionTokensRepository.findById(121)).thenReturn(Optional.of(token));
        when(positionTransfersRepository.findByFkTokenOrderByIdDesc(121)).thenReturn(List.of());
        when(positionTokensRepository.save(any(PositionTokens.class))).thenAnswer(invocation -> invocation.getArgument(0));

        EventsWf event = new EventsWf();
        event.setFkTokens(121);
        event.setTipoEvento((short) 108);
        event.setOutcomeReq("KO");
        event.setOutcomeResp("OK");

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-evt-update", Instant.now());
        positionEventUpdateService.updatePositionAfterEvents(ctx, List.of(event));

        ArgumentCaptor<PositionTokens> tokenCaptor = ArgumentCaptor.forClass(PositionTokens.class);
        verify(positionTokensRepository).save(tokenCaptor.capture());
        PositionTokens savedToken = tokenCaptor.getValue();
        assertEquals("KO", savedToken.getOutcome());
    }
}

