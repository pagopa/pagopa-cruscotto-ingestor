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
import java.util.List;
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
    void shouldMapInsertedTimestampForExtraInfo() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-04-10T10:15:30Z"));
        row.put("INFO_NAME", "email");
        row.put("INFO_VALUE", "value-1");

        ExtraInfo mapped = transformer.transform(row, ExtraInfo.class);

        assertEquals(LocalDateTime.parse("2026-04-10T10:15:30"), mapped.getInsertedTimestamp());
    }

    @Test
    void shouldMapInsertedTimestampForPositionTokens() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("TOKEN", "token-ts-1");
        row.put("NAV", "NAV-TS");
        row.put("PA_EMITTENTE", "PA-TS");
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-04-11T08:20:00Z"));

        PositionTokens mapped = transformer.transform(row, PositionTokens.class);

        assertEquals(LocalDateTime.parse("2026-04-11T08:20:00"), mapped.getInsertedTimestamp());
    }

    @Test
    void shouldMapInsertedTimestampForPositionTokensIgnoringPaymentDateFallback() throws Exception {
        // inserted_timestamp e' provenienza sorgente: se manca INSERTED_TIMESTAMP NON deve ripiegare
        // su PAYMENT_DATE (che serve solo alla FK-resolution) -> resta null.
        Map<String, Object> row = new HashMap<>();
        row.put("TOKEN", "token-ts-2");
        row.put("NAV", "NAV-TS2");
        row.put("PA_EMITTENTE", "PA-TS2");
        row.put("DATE_EVENT", "2026-04-11");
        row.put("PAYMENT_DATE", Instant.parse("2026-04-11T23:00:00Z"));

        PositionTokens mapped = transformer.transform(row, PositionTokens.class);

        assertNull(mapped.getInsertedTimestamp());
    }

    @Test
    void shouldNotDerivePaymentDateFromInsertedTimestampAtTokenInsert() throws Exception {
        // Fix bug: al primo insert (activatePaymentNotice, senza PAYMENT_DATE) payment_date NON deve
        // essere derivato da INSERTED_TIMESTAMP (= creazione). Altrimenti l'arricchimento event-driven
        // (PositionEventUpdateService, che scrive solo se null) non valorizza mai la data reale di pagamento.
        Map<String, Object> row = new HashMap<>();
        row.put("TOKEN", "token-pd-1");
        row.put("NAV", "NAV-PD");
        row.put("PA_EMITTENTE", "PA-PD");
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-04-11T08:20:00Z"));

        PositionTokens mapped = transformer.transform(row, PositionTokens.class);

        assertNull(mapped.getPaymentDate());
        // inserted_timestamp (colonna passiva) resta valorizzato: erano identici, ora sono distinti
        assertEquals(LocalDateTime.parse("2026-04-11T08:20:00"), mapped.getInsertedTimestamp());
    }

    @Test
    void shouldMapInsertedTimestampForPositionTransfers() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("TOKEN", "transfer-ts-1");
        row.put("PA_TRANSFER", "PA-T");
        row.put("ID_TRANSFER", 1);
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-04-12T09:05:00Z"));

        PositionTransfers mapped = transformer.transform(row, PositionTransfers.class);

        assertEquals(LocalDateTime.parse("2026-04-12T09:05:00"), mapped.getInsertedTimestamp());
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
    void positionMergeAnchorsTheInMemoryWindowOnBirthNotOnTheIncomingEvent() throws Exception {
        // Rule 7.1: l'evento dell'11/09 si aggancia alla POSITION nata il 10/09. La cache in-memory
        // che alimenta la finestra 24h deve restare ancorata alla NASCITA: se registrasse il
        // timestamp dell'evento, la finestra scorrerebbe in avanti e un evento del 12/09 verrebbe
        // accorpato alla stessa riga, riproducendo in-run l'accorpamento a catena.
        Position born = new Position();
        born.setId(777);
        born.setNav("NAV-001");
        born.setPaEmittente("PA-001");
        born.setDateEvent(LocalDate.parse("2026-09-10"));
        born.setInsertedTimestamp(LocalDateTime.parse("2026-09-10T11:02:11"));
        when(positionRepository
                .findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
                        any(), any(), any(), any(), any(), any()))
                .thenReturn(Optional.of(born));

        Map<String, Object> row = new HashMap<>();
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-09-11T06:23:07Z"));
        row.put("NAV", "NAV-001");
        row.put("PA_EMITTENTE", "PA-001");

        RunContext ctx = new RunContext(EntityName.POSITION.name(), "run-anchor", Instant.now());
        Position mapped = transformer.transform(row, Position.class, ctx, EntityName.POSITION);

        assertEquals(777, mapped.getId());

        BatchLocalCache cache = ctx.getBatchLocalCache();
        // ancorata alla nascita: un evento entro 24h dal 10/09 11:02:11 la trova...
        assertEquals(777, cache.findPositionInWindow("NAV-001", "PA-001",
                LocalDateTime.parse("2026-09-11T06:23:07")));
        // ...uno oltre le 24h dalla NASCITA no (se fosse ancorata all'evento, lo troverebbe)
        assertNull(cache.findPositionInWindow("NAV-001", "PA-001",
                LocalDateTime.parse("2026-09-12T06:00:00")));
    }

    @Test
    void shouldResolvePositionTokensFkFromAdditionalDateEventsWhenPositionWasBornThePreviousDay() throws Exception {
        // Scenario reale: POSITION nata il 10/09, evento dell'11/09 assorbito da rule 7.1 come giorno
        // aggiuntivo. Un token dell'11/09 non trova nulla ne' con la finestra 24h (nascita troppo
        // indietro) ne' con DATE_EVENT esatto: deve risolvere la FK guardando DATE_EVENTS.
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "2026-09-11");
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-09-11T20:00:00Z"));
        row.put("NAV", "NAV-001");
        row.put("PA_EMITTENTE", "PA-001");
        row.put("TOKEN", "token-additional-day");

        when(positionRepository
                .findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
                        any(), any(), any(), any(), any(), any()))
                .thenReturn(Optional.empty());
        when(positionRepository.findLatestIdByBusinessKey("NAV-001", "PA-001", LocalDate.parse("2026-09-11")))
                .thenReturn(Optional.empty());

        Position absorbing = new Position();
        absorbing.setId(123);
        absorbing.setNav("NAV-001");
        absorbing.setPaEmittente("PA-001");
        absorbing.setDateEvent(LocalDate.parse("2026-09-10"));
        absorbing.setInsertedTimestamp(LocalDateTime.parse("2026-09-10T11:02:11"));
        absorbing.setDateEvents("[\"2026-09-11\"]");
        when(positionRepository.findByNavAndPaEmittenteAndDateEventBetweenOrderByInsertedTimestampDescIdDesc(
                "NAV-001", "PA-001", LocalDate.parse("2026-09-10"), LocalDate.parse("2026-09-11")))
                .thenReturn(List.of(absorbing));

        PositionTokens mapped = transformer.transform(row, PositionTokens.class,
                new RunContext(EntityName.POSITION_TOKENS.name(), "run-additional-day", Instant.now()),
                EntityName.POSITION_TOKENS);

        assertEquals(123, mapped.getFkPosition());
    }

    @Test
    void shouldResolvePositionTokensFkWhenDateEventsUsesBasicIsoFormat() throws Exception {
        // PositionEventUpdateService serializza DATE_EVENTS in BASIC_ISO (20260911): la risoluzione
        // FK deve riconoscere anche questo formato, altrimenti fallisce a seconda di quale flusso
        // ha toccato per ultimo la POSITION.
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "2026-09-11");
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-09-11T20:00:00Z"));
        row.put("NAV", "NAV-001");
        row.put("PA_EMITTENTE", "PA-001");
        row.put("TOKEN", "token-basic-iso");

        when(positionRepository
                .findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
                        any(), any(), any(), any(), any(), any()))
                .thenReturn(Optional.empty());
        when(positionRepository.findLatestIdByBusinessKey("NAV-001", "PA-001", LocalDate.parse("2026-09-11")))
                .thenReturn(Optional.empty());

        Position absorbing = new Position();
        absorbing.setId(456);
        absorbing.setDateEvent(LocalDate.parse("2026-09-10"));
        absorbing.setInsertedTimestamp(LocalDateTime.parse("2026-09-10T11:02:11"));
        absorbing.setDateEvents("[\"20260911\"]");
        when(positionRepository.findByNavAndPaEmittenteAndDateEventBetweenOrderByInsertedTimestampDescIdDesc(
                "NAV-001", "PA-001", LocalDate.parse("2026-09-10"), LocalDate.parse("2026-09-11")))
                .thenReturn(List.of(absorbing));

        PositionTokens mapped = transformer.transform(row, PositionTokens.class,
                new RunContext(EntityName.POSITION_TOKENS.name(), "run-basic-iso", Instant.now()),
                EntityName.POSITION_TOKENS);

        assertEquals(456, mapped.getFkPosition());
    }

    @Test
    void shouldFailPositionTokensWhenAdditionalDateEventsDoNotContainTheRequestedDay() {
        // L'array esiste ma non contiene il giorno del token: la FK non va inventata, il record
        // deve finire in staging.
        Map<String, Object> row = new HashMap<>();
        row.put("DATE_EVENT", "2026-09-11");
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-09-11T20:00:00Z"));
        row.put("NAV", "NAV-001");
        row.put("PA_EMITTENTE", "PA-001");
        row.put("TOKEN", "token-other-day");

        when(positionRepository
                .findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
                        any(), any(), any(), any(), any(), any()))
                .thenReturn(Optional.empty());
        when(positionRepository.findLatestIdByBusinessKey("NAV-001", "PA-001", LocalDate.parse("2026-09-11")))
                .thenReturn(Optional.empty());

        Position other = new Position();
        other.setId(999);
        other.setDateEvent(LocalDate.parse("2026-09-10"));
        other.setInsertedTimestamp(LocalDateTime.parse("2026-09-10T11:02:11"));
        other.setDateEvents("[\"2026-09-12\"]");
        when(positionRepository.findByNavAndPaEmittenteAndDateEventBetweenOrderByInsertedTimestampDescIdDesc(
                "NAV-001", "PA-001", LocalDate.parse("2026-09-10"), LocalDate.parse("2026-09-11")))
                .thenReturn(List.of(other));

        EntityTransformer.TransformationException ex = assertThrows(
                EntityTransformer.TransformationException.class,
                () -> transformer.transform(row, PositionTokens.class,
                        new RunContext(EntityName.POSITION_TOKENS.name(), "run-no-match", Instant.now()),
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

    // ── Regole sendPaymentOutcome sul percorso VIVO di POSITION_TOKENS ─────────────────────────
    // Le stesse regole esistevano anche in PositionTokensTransformer, che non era invocato da
    // nessuno: i suoi test erano verdi su codice morto, ed e' per questo che il difetto qui sotto
    // non era stato intercettato. La copertura sta ora dove la regola gira davvero.

    /**
     * Regressione: lo stream dei token non proietta {@code OUTCOME_REQ}, mentre {@code outcomeResp}
     * ripiega su {@code OUTCOME}. Una riga di {@code sendPaymentOutcome} con {@code OUTCOME = 'OK'}
     * entrava quindi nel ramo OK e scriveva {@code outcome = null}: l'esito veniva azzerato proprio
     * sulle righe che portano il risultato del pagamento, e il token risultava non incassato.
     */
    @Test
    void sendPaymentOutcomeMustNotClearTheTokenOutcome() throws Exception {
        Map<String, Object> row = spoTokenRow();
        row.put("OUTCOME", "OK");

        PositionTokens mapped = transformer.transform(row, PositionTokens.class,
                new RunContext(EntityName.POSITION_TOKENS.name(), "run-spo", Instant.now()),
                EntityName.POSITION_TOKENS);

        assertEquals("OK", mapped.getOutcome());
    }

    /**
     * Requisito, punto 3 dichiarato "NON IN AND col punto 2": con {@code OUTCOME_RESP = OK} il
     * metodo di pagamento va valorizzato se il touchpoint e' PSP, <strong>indipendentemente</strong>
     * dall'incasso. Era annidato dentro il blocco di {@code PAYMENT_DATE}, quindi un pagamento
     * fallito non lo riceveva.
     */
    @Test
    void paymentMethodIsIndependentOfPaymentDateOnOutcomeRespOk() throws Exception {
        Map<String, Object> row = spoTokenRow();
        row.put("OUTCOME_RESP", "OK");
        row.put("OUTCOME_REQ", "KO");

        PositionTokens mapped = transformer.transform(row, PositionTokens.class,
                new RunContext(EntityName.POSITION_TOKENS.name(), "run-spo", Instant.now()),
                EntityName.POSITION_TOKENS);

        assertEquals("KO", mapped.getOutcome());
        // Nessun incasso: l'invariante PAYMENT_DATE IS NOT NULL <=> OUTCOME = 'OK' va preservata.
        assertNull(mapped.getPaymentDate());
        assertEquals("CP", mapped.getPaymentMethod());
    }

    /** Con {@code OUTCOME_REQ = OK} l'incasso c'e': data di pagamento dall'evento di richiesta. */
    @Test
    void paymentDateComesFromTheRequestTimestampWhenOutcomeReqIsOk() throws Exception {
        Map<String, Object> row = spoTokenRow();
        row.put("OUTCOME_RESP", "OK");
        row.put("OUTCOME_REQ", "OK");
        row.put("INSERTED_TIMESTAMP_REQ", Instant.parse("2026-09-25T10:15:30Z"));

        PositionTokens mapped = transformer.transform(row, PositionTokens.class,
                new RunContext(EntityName.POSITION_TOKENS.name(), "run-spo", Instant.now()),
                EntityName.POSITION_TOKENS);

        assertEquals("OK", mapped.getOutcome());
        assertEquals(LocalDateTime.parse("2026-09-25T10:15:30"), mapped.getPaymentDate());
        assertEquals("CP", mapped.getPaymentMethod());
    }

    /**
     * Ramo KO: ammesso solo con faultcode di token scaduto e Touchpoint PSP. Con un faultcode
     * estraneo non si scrive nulla — ne' esito ne' data — perche' l'evento non dice nulla sull'esito
     * del pagamento.
     */
    @Test
    void outcomeRespKoWithUnrelatedFaultCodeLeavesTheTokenUntouched() throws Exception {
        Map<String, Object> row = spoTokenRow();
        row.put("OUTCOME_RESP", "KO");
        row.put("OUTCOME_REQ", "OK");
        row.put("FAULT_CODE", "PPT_STAZIONE_INT_PA_IRRAGGIUNGIBILE");

        PositionTokens mapped = transformer.transform(row, PositionTokens.class,
                new RunContext(EntityName.POSITION_TOKENS.name(), "run-spo", Instant.now()),
                EntityName.POSITION_TOKENS);

        assertNull(mapped.getOutcome());
        assertNull(mapped.getPaymentDate());
    }

    /** Ramo KO conforme: esito dall'evento, nessun incasso senza {@code OUTCOME_REQ = OK}, metodo valorizzato. */
    @Test
    void expiredTokenFromPspTouchpointSetsOutcomeAndPaymentMethodWithoutPaymentDate() throws Exception {
        Map<String, Object> row = spoTokenRow();
        row.put("OUTCOME_RESP", "KO");
        row.put("OUTCOME_REQ", "KO");
        row.put("FAULT_CODE", "PPT_TOKEN_SCADUTO");

        PositionTokens mapped = transformer.transform(row, PositionTokens.class,
                new RunContext(EntityName.POSITION_TOKENS.name(), "run-spo", Instant.now()),
                EntityName.POSITION_TOKENS);

        assertEquals("KO", mapped.getOutcome());
        assertNull(mapped.getPaymentDate());
        assertEquals("CP", mapped.getPaymentMethod());
    }

    /**
     * Riga dello stream token per una sendPaymentOutcome. NAV e PA_EMITTENTE servono solo a far
     * risolvere la FK verso POSITION, che il percorso vivo pretende: senza, la transform abortisce
     * prima di arrivare alle regole in esame.
     */
    private Map<String, Object> spoTokenRow() {
        Position position = new Position();
        position.setId(777);
        when(positionRepository
                .findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
                        any(), any(), any(), any(), any(), any()))
                .thenReturn(Optional.of(position));

        Map<String, Object> row = new HashMap<>();
        row.put("TIPO_EVENTO", "sendPaymentOutcome");
        row.put("SOTTO_TIPO_EVENTO", "REQ/RESP");
        row.put("NAV", "NAV-SPO");
        row.put("PA_EMITTENTE", "PA-SPO");
        row.put("TOUCHPOINT", "Touchpoint PSP");
        row.put("PAYMENT_METHOD", "CP");
        row.put("TOKEN", "spo-token-1");
        row.put("IUV", "IUV-1");
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-09-25T10:15:30Z"));
        return row;
    }
}
