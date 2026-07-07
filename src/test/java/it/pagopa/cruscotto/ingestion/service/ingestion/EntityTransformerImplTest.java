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
import it.pagopa.cruscotto.ingestion.repository.PositionTokensRepository;
import it.pagopa.cruscotto.ingestion.service.AnagraficaService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EntityTransformerImplTest {

    @Mock
    private AnagraficaService anagraficaService;

    @Mock
    private PositionRepository positionRepository;

    @Mock
    private PositionTokensRepository positionTokensRepository;

    private EntityTransformerImpl transformer;

    @BeforeEach
    void setUp() {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        transformer = new EntityTransformerImpl(mapper, anagraficaService, positionRepository, positionTokensRepository);
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
    void shouldFailEventsWfTransformationWhenFkTokensIsMissing() {
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

        EntityTransformer.TransformationException ex = assertThrows(
                EntityTransformer.TransformationException.class,
                () -> transformer.transform(row, EventsWf.class,
                        new RunContext(EntityName.EVENTS_WF.name(), "run-ewf", Instant.now()),
                        EntityName.EVENTS_WF)
        );

        assertEquals(true, ex.getMessage().contains("Missing required FK fkTokens"));
    }

    @Test
    void shouldSetPositionIdForUpdateWhenBusinessKeyExistsIn24hWindow() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("NAV", "NAV-100");
        row.put("PA_EMITTENTE", "PA-100");
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-04-12T09:00:00Z"));

        Position existing = new Position();
        existing.setId(77);
        when(positionRepository.findFirstByNavAndPaEmittenteAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
                org.mockito.ArgumentMatchers.eq("NAV-100"),
                org.mockito.ArgumentMatchers.eq("PA-100"),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any()))
                .thenReturn(Optional.of(existing));

        Position mapped = transformer.transform(row, Position.class,
                new RunContext(EntityName.POSITION.name(), "run-pos-24h", Instant.now()),
                EntityName.POSITION);

        assertEquals(77, mapped.getId());
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
        when(positionTokensRepository.findById(11)).thenReturn(Optional.of(token));

        EventsWf mapped = transformer.transform(row, EventsWf.class,
                new RunContext(EntityName.EVENTS_WF.name(), "run-ewf-token-first", Instant.now()),
                EntityName.EVENTS_WF);

        assertEquals(11, mapped.getFkTokens());
        assertEquals(33, mapped.getFkPosition());
    }

    @Test
    void shouldResolveEventsTokenByPositionAndIuvWhenTokenLookupMisses() throws Exception {
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
        when(positionTokensRepository.findLatestIdByPositionAndIuv(33, "IUV-ABC-1", LocalDate.parse("2026-04-12")))
                .thenReturn(Optional.of(11));

        EventsWf mapped = transformer.transform(row, EventsWf.class,
                new RunContext(EntityName.EVENTS_WF.name(), "run-ewf-fallback", Instant.now()),
                EntityName.EVENTS_WF);

        assertEquals(11, mapped.getFkTokens());
        assertEquals(33, mapped.getFkPosition());
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
