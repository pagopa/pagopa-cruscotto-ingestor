package it.pagopa.cruscotto.ingestion.service.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.EventsWf;
import it.pagopa.cruscotto.ingestion.entity.ExtraInfo;
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
        when(positionTokensRepository.findLatestIdByPositionAndIuv(99, "IUV-002", LocalDate.parse("2026-04-12")))
                .thenReturn(Optional.empty());
        when(positionTokensRepository.findLatestIdByTokenAndDate("evt-token".getBytes(), LocalDate.parse("2026-04-12")))
                .thenReturn(Optional.empty());

        EntityTransformer.TransformationException ex = assertThrows(
                EntityTransformer.TransformationException.class,
                () -> transformer.transform(row, EventsWf.class,
                        new RunContext(EntityName.EVENTS_WF.name(), "run-ewf", Instant.now()),
                        EntityName.EVENTS_WF)
        );

        assertEquals(true, ex.getMessage().contains("Missing required FK fkTokens"));
    }
}



