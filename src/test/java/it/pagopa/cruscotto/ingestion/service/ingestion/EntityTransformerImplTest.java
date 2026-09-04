package it.pagopa.cruscotto.ingestion.service.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.EventsWf;
import it.pagopa.cruscotto.ingestion.entity.ExtraInfo;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EntityTransformerImplTest {

    @Mock
    private AnagraficaService anagraficaService;

    @Mock
    private PositionRepository positionRepository;

    @Mock
    private PositionTokensRepository positionTokensRepository;

    @Mock
    private PositionTokenRegistryReader positionTokenRegistryReader;

    private EntityTransformerImpl transformer;

    @BeforeEach
    void setUp() {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // Real resolver over the mocked repositories: the existing pruning assertions (which stub
        // the registry reader) keep exercising the actual lookup path, now shared.
        transformer = new EntityTransformerImpl(mapper, anagraficaService, positionRepository,
                positionTokensRepository,
                new CanonicalTokenResolver(positionTokensRepository, positionTokenRegistryReader));
    }

    @Test
    void shouldFallbackDateEventForPositionTokensWhenDateEventIsNull() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", null);
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-04-07T15:35:15.405567Z"));

        PositionTokens mapped = transformer.transform(row, PositionTokens.class);

        assertEquals(LocalDate.parse("2026-04-07"), mapped.getDateEvent());
    }

    @Test
    void shouldFallbackDateEventForPositionTokensWhenDateEventIsBlank() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "   ");
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-04-08T10:00:00Z"));

        PositionTokens mapped = transformer.transform(row, PositionTokens.class);

        assertEquals(LocalDate.parse("2026-04-08"), mapped.getDateEvent());
    }

    @Test
    void shouldFallbackDateEventForPositionTransfers() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "");
        row.put("inserted_timestamp", Instant.parse("2026-04-09T10:00:00Z"));

        PositionTransfers mapped = transformer.transform(row, PositionTransfers.class);

        assertEquals(LocalDate.parse("2026-04-09"), mapped.getDateEvent());
    }

    @Test
    void shouldFallbackDateEventForExtraInfo() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("date_event", null);
        row.put("insertedTimestamp", Instant.parse("2026-04-10T10:00:00Z"));

        ExtraInfo mapped = transformer.transform(row, ExtraInfo.class);

        assertEquals(LocalDate.parse("2026-04-10"), mapped.getDateEvent());
    }

    @Test
    void shouldUseRespTimestampFirstForEventsWfDateEventFallback() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", null);
        row.put("INSERTED_TIMESTAMP_REQ", Instant.parse("2026-04-11T08:00:00Z"));
        row.put("INSERTED_TIMESTAMP_RESP", Instant.parse("2026-04-12T09:00:00Z"));

        EventsWf mapped = transformer.transform(row, EventsWf.class);

        assertEquals(LocalDate.parse("2026-04-12"), mapped.getDateEvent());
    }

    @Test
    void shouldFailPositionTokensTransformationWhenFkPositionIsMissing() {
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "2026-04-08");
        row.put("NAV", "NAV-001");
        row.put("PA_EMITTENTE", "PA-001");
        row.put("TOKEN", "abc-token");

        when(positionRepository.findLatestIdByBusinessKey("NAV-001", "PA-001", LocalDate.parse("2026-04-08")))
                .thenReturn(Optional.empty());

        EntityTransformer.TransformationException ex = assertThrows(
                EntityTransformer.TransformationException.class,
                () -> transformer.transform(row, PositionTokens.class,
                        new RunContext(EntityName.POSITION_TOKENS.name(), "run-pt", Instant.now()),
                        EntityName.POSITION_TOKENS)
        );

        assertEquals(true, ex.getMessage().contains("Missing required FK fkPosition"));
    }

    @Test
    void shouldAllowPositionTokensTransformationWhenTokenIsMissingAndFkPositionExists() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "2026-04-08");
        row.put("NAV", "NAV-001");
        row.put("PA_EMITTENTE", "PA-001");
        row.put("IUV", "01000014903654436");
        row.put("TOKEN", "   ");

        when(positionRepository.findLatestIdByBusinessKey("NAV-001", "PA-001", LocalDate.parse("2026-04-08")))
                .thenReturn(Optional.of(42));

        PositionTokens mapped = transformer.transform(row, PositionTokens.class,
                new RunContext(EntityName.POSITION_TOKENS.name(), "run-pt-token", Instant.now()),
                EntityName.POSITION_TOKENS);

        assertEquals(42, mapped.getFkPosition());
        assertEquals(null, mapped.getToken());
    }

    @Test
    void shouldMapPositionTokensWithFeeAndAmount() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "2026-04-08");
        row.put("NAV", "NAV-001");
        row.put("PA_EMITTENTE", "PA-001");
        row.put("IUV", "01000014903654436");
        row.put("TOKEN", "token-abc");
        row.put("AMOUNT", 100.50);
        row.put("FEE", 2.50);
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-04-08T10:00:00Z"));

        when(positionRepository.findLatestIdByBusinessKey("NAV-001", "PA-001", LocalDate.parse("2026-04-08")))
                .thenReturn(Optional.of(42));

        PositionTokens mapped = transformer.transform(row, PositionTokens.class,
                new RunContext(EntityName.POSITION_TOKENS.name(), "run-pt-fee", Instant.now()),
                EntityName.POSITION_TOKENS);

        assertEquals(42, mapped.getFkPosition());
        assertEquals(0, mapped.getAmount().compareTo(new java.math.BigDecimal("100.50")));
        assertEquals(0, mapped.getFee().compareTo(new java.math.BigDecimal("2.50")));
    }

    @Test
    void shouldKeepEventsFkTokensNullWhenTokenPresentButMissingAndPositionResolved() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "2026-04-12");
        row.put("NAV", "NAV-002");
        row.put("PA_EMITTENTE", "PA-002");
        row.put("IUV", "IUV-002");
        row.put("TOKEN", "evt-token");
        row.put("INSERTED_TIMESTAMP_RESP", Instant.parse("2026-04-12T09:00:00Z"));

        when(positionRepository.findLatestIdByBusinessKey("NAV-002", "PA-002", LocalDate.parse("2026-04-12")))
                .thenReturn(Optional.of(99));
        when(positionTokensRepository.findCanonicalByToken("evt-token".getBytes()))
                .thenReturn(Optional.empty());

        EventsWf mapped = transformer.transform(row, EventsWf.class,
                new RunContext(EntityName.EVENTS_WF.name(), "run-ewf", Instant.now()),
                EntityName.EVENTS_WF);

        assertNull(mapped.getFkTokens());
        assertEquals(99, mapped.getFkPosition());
        verify(positionTokensRepository, never()).findLatestIdByPositionAndIuv(anyInt(), anyString(), any());
    }

    @Test
    void shouldPrunePositionLookupToDateEventRangeOfWindow() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("NAV", "NAV-900");
        row.put("PA_EMITTENTE", "PA-900");
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-04-12T09:00:00Z"));

        Position existing = new Position();
        existing.setId(910);
        when(positionRepository.findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
                org.mockito.ArgumentMatchers.eq("NAV-900"),
                org.mockito.ArgumentMatchers.eq("PA-900"),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any()))
                .thenReturn(Optional.of(existing));

        transformer.transform(row, Position.class,
                new RunContext(EntityName.POSITION.name(), "run-pos-prune", Instant.now()),
                EntityName.POSITION);

        org.mockito.ArgumentCaptor<LocalDate> dateFrom = org.mockito.ArgumentCaptor.forClass(LocalDate.class);
        org.mockito.ArgumentCaptor<LocalDate> dateTo = org.mockito.ArgumentCaptor.forClass(LocalDate.class);
        org.mockito.ArgumentCaptor<LocalDateTime> tsFrom = org.mockito.ArgumentCaptor.forClass(LocalDateTime.class);
        org.mockito.ArgumentCaptor<LocalDateTime> tsTo = org.mockito.ArgumentCaptor.forClass(LocalDateTime.class);
        verify(positionRepository)
                .findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
                        org.mockito.ArgumentMatchers.eq("NAV-900"),
                        org.mockito.ArgumentMatchers.eq("PA-900"),
                        dateFrom.capture(),
                        dateTo.capture(),
                        tsFrom.capture(),
                        tsTo.capture());

        // DATE_EVENT range must equal the calendar days spanned by the 24h window.
        assertEquals(tsFrom.getValue().toLocalDate(), dateFrom.getValue());
        assertEquals(tsTo.getValue().toLocalDate(), dateTo.getValue());
        assertEquals(tsTo.getValue().minusHours(24), tsFrom.getValue());
    }

    @Test
    void shouldSetPositionIdForUpdateWhenBusinessKeyExistsIn24hWindow() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("NAV", "NAV-100");
        row.put("PA_EMITTENTE", "PA-100");
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-04-12T09:00:00Z"));

        Position existing = new Position();
        existing.setId(77);
        when(positionRepository.findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
                org.mockito.ArgumentMatchers.eq("NAV-100"),
                org.mockito.ArgumentMatchers.eq("PA-100"),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any()))
                .thenReturn(Optional.of(existing));

        Position mapped = transformer.transform(row, Position.class,
                new RunContext(EntityName.POSITION.name(), "run-pos-24h", Instant.now()),
                EntityName.POSITION);

        assertEquals(77, mapped.getId());
    }

    @Test
    void shouldCachePositionLookupHitForSameBusinessKeyAndTimestamp() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("NAV", "NAV-101");
        row.put("PA_EMITTENTE", "PA-101");
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-04-12T10:00:00Z"));

        Position existing = new Position();
        existing.setId(78);
        when(positionRepository.findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
                org.mockito.ArgumentMatchers.eq("NAV-101"),
                org.mockito.ArgumentMatchers.eq("PA-101"),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any()))
                .thenReturn(Optional.of(existing));

        RunContext ctx = new RunContext(EntityName.POSITION.name(), "run-pos-cache-hit", Instant.now());
        Position first = transformer.transform(row, Position.class, ctx, EntityName.POSITION);
        Position second = transformer.transform(row, Position.class, ctx, EntityName.POSITION);

        assertEquals(78, first.getId());
        assertEquals(78, second.getId());
        verify(positionRepository, times(1))
                .findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
                        org.mockito.ArgumentMatchers.eq("NAV-101"),
                        org.mockito.ArgumentMatchers.eq("PA-101"),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any());
    }

    @Test
    void shouldCachePositionLookupMissForSameBusinessKeyAndTimestamp() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("NAV", "NAV-102");
        row.put("PA_EMITTENTE", "PA-102");
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-04-12T11:00:00Z"));

        when(positionRepository.findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
                org.mockito.ArgumentMatchers.eq("NAV-102"),
                org.mockito.ArgumentMatchers.eq("PA-102"),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any()))
                .thenReturn(Optional.empty());

        RunContext ctx = new RunContext(EntityName.POSITION.name(), "run-pos-cache-miss", Instant.now());
        Position first = transformer.transform(row, Position.class, ctx, EntityName.POSITION);
        Position second = transformer.transform(row, Position.class, ctx, EntityName.POSITION);

        assertNull(first.getId());
        assertNull(second.getId());
        verify(positionRepository, times(1))
                .findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
                        org.mockito.ArgumentMatchers.eq("NAV-102"),
                        org.mockito.ArgumentMatchers.eq("PA-102"),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any());
    }

    @Test
    void shouldCachePositionDateFallbackLookupForRepeatedRows() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "2026-04-16");
        row.put("NAV", "NAV-200");
        row.put("PA_EMITTENTE", "PA-200");
        row.put("TOKEN", "token-date-cache");
        row.put("IUV", "IUV-200");
        // no INSERTED_TIMESTAMP -> force date fallback lookup

        when(positionRepository.findLatestIdByBusinessKey("NAV-200", "PA-200", LocalDate.parse("2026-04-16")))
                .thenReturn(Optional.of(201));

        RunContext ctx = new RunContext(EntityName.POSITION_TOKENS.name(), "run-pos-date-cache", Instant.now());
        PositionTokens first = transformer.transform(row, PositionTokens.class, ctx, EntityName.POSITION_TOKENS);
        PositionTokens second = transformer.transform(row, PositionTokens.class, ctx, EntityName.POSITION_TOKENS);

        assertEquals(201, first.getFkPosition());
        assertEquals(201, second.getFkPosition());
        verify(positionRepository, times(1))
                .findLatestIdByBusinessKey("NAV-200", "PA-200", LocalDate.parse("2026-04-16"));
    }

    @Test
    void shouldCacheCanonicalTokenLookupForRepeatedRows() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "2026-04-17");
        row.put("TOKEN", "evt-token-cache");
        row.put("NAV", "NAV-300");
        row.put("PA_EMITTENTE", "PA-300");
        row.put("IS_EVENT_MULTI_PAYMENT", true);
        row.put("INSERTED_TIMESTAMP_RESP", Instant.parse("2026-04-17T09:00:00Z"));

        PositionTokens token = new PositionTokens();
        token.setId(301);
        token.setFkPosition(302);
        when(positionTokensRepository.findCanonicalByToken("evt-token-cache".getBytes())).thenReturn(Optional.of(token));

        RunContext ctx = new RunContext(EntityName.EVENTS_WF.name(), "run-token-cache", Instant.now());
        EventsWf first = transformer.transform(row, EventsWf.class, ctx, EntityName.EVENTS_WF);
        EventsWf second = transformer.transform(row, EventsWf.class, ctx, EntityName.EVENTS_WF);

        assertEquals(301, first.getFkTokens());
        assertEquals(301, second.getFkTokens());
        assertEquals(302, first.getFkPosition());
        assertEquals(302, second.getFkPosition());
        verify(positionTokensRepository, times(1)).findCanonicalByToken("evt-token-cache".getBytes());
        verify(positionTokensRepository, org.mockito.Mockito.never()).findById(org.mockito.ArgumentMatchers.anyInt());
    }

    @Test
    void shouldResolveEventsPositionFromTokenWhenNavIsNotReliable() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "2026-04-12");
        row.put("TOKEN", "evt-token");
        row.put("NAV", "WRONG-NAV");
        row.put("PA_EMITTENTE", "WRONG-PA");
        row.put("IS_EVENT_MULTI_PAYMENT", true);
        row.put("INSERTED_TIMESTAMP_RESP", Instant.parse("2026-04-12T09:00:00Z"));

        PositionTokens token = new PositionTokens();
        token.setId(11);
        token.setFkPosition(33);
        when(positionTokensRepository.findCanonicalByToken("evt-token".getBytes())).thenReturn(Optional.of(token));

        EventsWf mapped = transformer.transform(row, EventsWf.class,
                new RunContext(EntityName.EVENTS_WF.name(), "run-ewf-token-first", Instant.now()),
                EntityName.EVENTS_WF);

        assertEquals(11, mapped.getFkTokens());
        assertEquals(33, mapped.getFkPosition());
    }

    @Test
    void shouldPruneCanonicalTokenLookupUsingRegistryFirstDateEvent() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "2026-04-20");
        row.put("TOKEN", "evt-token-pruned");
        row.put("NAV", "WRONG-NAV");
        row.put("PA_EMITTENTE", "WRONG-PA");
        row.put("IS_EVENT_MULTI_PAYMENT", true);
        row.put("INSERTED_TIMESTAMP_RESP", Instant.parse("2026-04-20T09:00:00Z"));

        PositionTokens token = new PositionTokens();
        token.setId(501);
        token.setFkPosition(502);
        LocalDate firstDateEvent = LocalDate.parse("2026-04-15");
        when(positionTokenRegistryReader.findFirstDateEventByToken("evt-token-pruned".getBytes()))
                .thenReturn(Optional.of(firstDateEvent));
        when(positionTokensRepository.findCanonicalByTokenAndDate("evt-token-pruned".getBytes(), firstDateEvent))
                .thenReturn(Optional.of(token));

        EventsWf mapped = transformer.transform(row, EventsWf.class,
                new RunContext(EntityName.EVENTS_WF.name(), "run-ewf-pruned", Instant.now()),
                EntityName.EVENTS_WF);

        assertEquals(501, mapped.getFkTokens());
        assertEquals(502, mapped.getFkPosition());
        verify(positionTokensRepository, times(1))
                .findCanonicalByTokenAndDate("evt-token-pruned".getBytes(), firstDateEvent);
        verify(positionTokensRepository, org.mockito.Mockito.never())
                .findCanonicalByToken("evt-token-pruned".getBytes());
    }

    @Test
    void shouldKeepEventsFkTokensNullWhenTokenMissingFromPositionTokens() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "2026-04-12");
        row.put("TOKEN", "evt-token-miss");
        row.put("IUV", "IUV-ABC-1");
        row.put("NAV", "NAV-003");
        row.put("PA_EMITTENTE", "PA-003");
        row.put("INSERTED_TIMESTAMP_RESP", Instant.parse("2026-04-12T09:00:00Z"));

        when(positionRepository.findLatestIdByBusinessKey("NAV-003", "PA-003", LocalDate.parse("2026-04-12")))
                .thenReturn(Optional.of(33));
        when(positionTokensRepository.findCanonicalByToken("evt-token-miss".getBytes())).thenReturn(Optional.empty());
        when(positionTokensRepository.findLatestIdByTokenAndDate("evt-token-miss".getBytes(), LocalDate.parse("2026-04-12")))
                .thenReturn(Optional.empty());

        EventsWf mapped = transformer.transform(row, EventsWf.class,
                new RunContext(EntityName.EVENTS_WF.name(), "run-ewf-fallback", Instant.now()),
                EntityName.EVENTS_WF);

        assertNull(mapped.getFkTokens());
        assertEquals(33, mapped.getFkPosition());
        verify(positionTokensRepository, never()).findLatestIdByPositionAndIuv(anyInt(), anyString(), any());
    }

    @Test
    void shouldNotPopulateEventIdReqFromUniqueIdOnRespRow() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "2026-04-12");
        row.put("IUV", "IUV-RESP");
        row.put("NAV", "NAV-005");
        row.put("PA_EMITTENTE", "PA-005");
        row.put("INSERTED_TIMESTAMP_RESP", Instant.parse("2026-04-12T09:00:00Z"));
        row.put("UNIQUE_ID", "2026-04-12_resp-123");
        row.put("EVENT_ID_RESP", "2026-04-12_resp-123");

        when(positionRepository.findLatestIdByBusinessKey("NAV-005", "PA-005", LocalDate.parse("2026-04-12")))
                .thenReturn(Optional.of(55));

        EventsWf mapped = transformer.transform(row, EventsWf.class,
                new RunContext(EntityName.EVENTS_WF.name(), "run-ewf-resp", Instant.now()),
                EntityName.EVENTS_WF);

        assertNull(mapped.getEventIdReq());
        assertEquals("2026-04-12_resp-123", mapped.getEventIdResp());
    }

    @Test
    void shouldPopulateEventIdReqOnReqRow() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "2026-04-12");
        row.put("IUV", "IUV-REQ");
        row.put("NAV", "NAV-006");
        row.put("PA_EMITTENTE", "PA-006");
        row.put("INSERTED_TIMESTAMP_REQ", Instant.parse("2026-04-12T09:00:00Z"));
        row.put("UNIQUE_ID", "2026-04-12_req-123");
        row.put("EVENT_ID_REQ", "2026-04-12_req-123");

        when(positionRepository.findLatestIdByBusinessKey("NAV-006", "PA-006", LocalDate.parse("2026-04-12")))
                .thenReturn(Optional.of(66));

        EventsWf mapped = transformer.transform(row, EventsWf.class,
                new RunContext(EntityName.EVENTS_WF.name(), "run-ewf-req", Instant.now()),
                EntityName.EVENTS_WF);

        assertEquals("2026-04-12_req-123", mapped.getEventIdReq());
        assertNull(mapped.getEventIdResp());
    }

    @Test
    void shouldKeepEventsFkTokensNullWhenAdxRowHasNoToken() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "2026-04-12");
        row.put("IUV", "IUV-NO-TOKEN");
        row.put("NAV", "NAV-004");
        row.put("PA_EMITTENTE", "PA-004");
        row.put("INSERTED_TIMESTAMP_RESP", Instant.parse("2026-04-12T09:00:00Z"));

        when(positionRepository.findLatestIdByBusinessKey("NAV-004", "PA-004", LocalDate.parse("2026-04-12")))
                .thenReturn(Optional.of(44));

        EventsWf mapped = transformer.transform(row, EventsWf.class,
                new RunContext(EntityName.EVENTS_WF.name(), "run-ewf-no-token", Instant.now()),
                EntityName.EVENTS_WF);

        assertEquals(44, mapped.getFkPosition());
        assertNull(mapped.getFkTokens());
        verify(positionTokensRepository, never()).findLatestIdByPositionAndIuv(anyInt(), anyString(), any());
    }

    @Test
    void shouldFailPositionTokensWhenNavAndPaEmittenteAreAbsentAndFkPositionCannotBeResolved() {
        Map<String, Object> row = new HashMap<>();
        row.put("TOKEN", "token-only-abc");
        row.put("DATE_EVENT", "2026-05-01");
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-05-01T10:00:00Z"));
        // NAV and PA_EMITTENTE intentionally absent

        EntityTransformer.TransformationException ex = assertThrows(
                EntityTransformer.TransformationException.class,
                () -> transformer.transform(row, PositionTokens.class,
                        new RunContext(EntityName.POSITION_TOKENS.name(), "run-pt-token-only", Instant.now()),
                        EntityName.POSITION_TOKENS)
        );

        assertEquals(true, ex.getMessage().contains("Missing required FK fkPosition"));
    }

    @Test
    void shouldNotLoadExistingTokenStateForPositionTokens() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("TOKEN", "token-new-1");
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-04-13T09:00:00Z"));
        row.put("DATE_EVENT", "2026-04-13");
        row.put("NAV", "NAV-001");
        row.put("PA_EMITTENTE", "PA-001");
        when(positionRepository.findLatestIdByBusinessKey("NAV-001", "PA-001", LocalDate.parse("2026-04-13")))
                .thenReturn(Optional.of(42));

        PositionTokens mapped = transformer.transform(row, PositionTokens.class,
                new RunContext(EntityName.POSITION_TOKENS.name(), "run-pt-new-token", Instant.now()),
                EntityName.POSITION_TOKENS);

        assertEquals(42, mapped.getFkPosition());
        assertNull(mapped.getId());
        assertNull(mapped.getFee());
    }

    @Test
    void shouldResolveTransferFkTokenByTokenWithoutSettingUpdateId() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "2026-04-15");
        row.put("TOKEN", "transfer-token-1");
        row.put("PA_TRANSFER", "PA-T-1");
        row.put("ID_TRANSFER", 1);
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-04-15T10:00:00Z"));

        PositionTokens token = new PositionTokens();
        token.setId(55);
        when(positionTokensRepository.findCanonicalByToken("transfer-token-1".getBytes())).thenReturn(Optional.of(token));

        PositionTransfers mapped = transformer.transform(row, PositionTransfers.class,
                new RunContext(EntityName.POSITION_TRANSFERS.name(), "run-tr", Instant.now()),
                EntityName.POSITION_TRANSFERS);

        assertEquals(55, mapped.getFkToken());
        assertNull(mapped.getId());
    }
}
