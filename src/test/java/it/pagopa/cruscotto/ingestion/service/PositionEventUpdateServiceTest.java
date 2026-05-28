package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.EventsWf;
import it.pagopa.cruscotto.ingestion.entity.Position;
import it.pagopa.cruscotto.ingestion.repository.PositionRepository;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PositionEventUpdateServiceTest {

    @Mock
    private PositionRepository positionRepository;

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
}

