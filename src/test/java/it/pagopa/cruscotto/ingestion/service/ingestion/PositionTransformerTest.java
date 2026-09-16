package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.Position;
import it.pagopa.cruscotto.ingestion.repository.PositionRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PositionTransformerTest {

    @Mock
    private PositionRepository positionRepository;

    @Mock
    private EntityTransformerImpl baseTransformer;

    private PositionTransformer transformer() {
        return new PositionTransformer(positionRepository, baseTransformer);
    }

    private RunContext ctx() {
        return new RunContext("POSITION", "run-clamp", Instant.parse("2026-08-14T22:38:00Z"));
    }

    @Test
    void clampsOversizedPaEmittenteAndNavToColumnWidth() throws Exception {
        when(positionRepository.findLatestByBusinessKeyWithin24h(any(), any(), any()))
                .thenReturn(Optional.empty());

        String oversizedPa = "CODICE CLIENTE: 1000947410 ESEGUITO DA: FARINA ALBERTO ".repeat(10); // > 255
        String oversizedNav = "4".repeat(300);
        Map<String, Object> row = new HashMap<>();
        row.put("NAV", oversizedNav);
        row.put("PA_EMITTENTE", oversizedPa);
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-08-14T20:41:59Z"));

        Position position = transformer().transform(row, ctx());

        assertEquals(255, position.getPaEmittente().length());
        assertEquals(oversizedPa.substring(0, 255), position.getPaEmittente());
        assertEquals(255, position.getNav().length());
        assertEquals(oversizedNav.substring(0, 255), position.getNav());

        // The 24h dedup lookup must use the SAME clamped values that get persisted, otherwise cache,
        // SELECT and INSERT would diverge for the poison record.
        ArgumentCaptor<String> navCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> paCaptor = ArgumentCaptor.forClass(String.class);
        verify(positionRepository).findLatestByBusinessKeyWithin24h(navCaptor.capture(), paCaptor.capture(), any());
        assertEquals(255, navCaptor.getValue().length());
        assertEquals(255, paCaptor.getValue().length());
    }

    @Test
    void leavesValuesWithinLimitUnchanged() throws Exception {
        when(positionRepository.findLatestByBusinessKeyWithin24h(any(), any(), any()))
                .thenReturn(Optional.empty());

        String nav = "420261282009930208";
        String paEmittente = "80087670016";
        Map<String, Object> row = new HashMap<>();
        row.put("NAV", nav);
        row.put("PA_EMITTENTE", paEmittente);
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-08-14T20:41:59Z"));

        Position position = transformer().transform(row, ctx());

        assertEquals(nav, position.getNav());
        assertEquals(paEmittente, position.getPaEmittente());
        verify(positionRepository).findLatestByBusinessKeyWithin24h(eq(nav), eq(paEmittente), any());
    }

    @Test
    void toleratesNullBusinessValues() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("INSERTED_TIMESTAMP", Instant.parse("2026-08-14T20:41:59Z"));

        Position position = transformer().transform(row, ctx());

        assertSame(null, position.getNav());
        assertSame(null, position.getPaEmittente());
    }
}
