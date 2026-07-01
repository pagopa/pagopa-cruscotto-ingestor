package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EventsWf;
import it.pagopa.cruscotto.ingestion.entity.Position;
import it.pagopa.cruscotto.ingestion.repository.PositionRepository;
import it.pagopa.cruscotto.ingestion.repository.PositionTokensRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventsWfTransformerTest {

    @Mock
    private PositionRepository positionRepository;

    @Mock
    private PositionTokensRepository positionTokensRepository;

    @Test
    void shouldResolveFkPositionWhenOnlyInsertedTimestampIsPresent() throws Exception {
        EntityTransformerImpl baseTransformer = mock(EntityTransformerImpl.class);
        doNothing().when(baseTransformer).resolveAllAnagrafiche(anyString(), org.mockito.ArgumentMatchers.anyMap());
        EventsWfTransformer transformer = new EventsWfTransformer(baseTransformer, positionRepository, positionTokensRepository);

        Position position = new Position();
        position.setId(161);
        position.setNav("002920000061434245");
        position.setPaEmittente("00147990923");
        position.setInsertedTimestamp(LocalDateTime.parse("2026-03-23T00:00:45.003884"));
        when(positionRepository.findFirstByNavAndPaEmittenteAndInsertedTimestampLessThanEqualOrderByInsertedTimestampDescIdDesc(
                "002920000061434245",
                "00147990923",
                LocalDateTime.parse("2026-03-23T01:30:01.634866")))
                .thenReturn(Optional.of(position));

        Map<String, Object> row = Map.of(
                "INSERTED_TIMESTAMP", Instant.parse("2026-03-23T01:30:01.634866Z"),
                "NAV", "002920000061434245",
                "PA_EMITTENTE", "00147990923",
                "IUV", "920000061434245",
                "SESSION_ID", "a8d00710-195f-48df-bbb0-ebb2caa22a8f",
                "UNIQUE_ID", "2026-03-23_-3985532687961615946",
                "TIPO_EVENTO", Short.valueOf((short) 1),
                "OUTCOME", "OK"
        );

        EventsWf event = transformer.transform(row, new RunContext("EVENTS_WF", "run-1", Instant.now()));

        assertNotNull(event);
        assertEquals(161, event.getFkPosition());
        assertNull(event.getFkTokens());
        assertEquals(LocalDateTime.parse("2026-03-23T01:30:01.634866"), event.getInsertedTimestampResp());
        assertEquals(event.getInsertedTimestampResp().toLocalDate(), event.getDateEvent());
    }
}
