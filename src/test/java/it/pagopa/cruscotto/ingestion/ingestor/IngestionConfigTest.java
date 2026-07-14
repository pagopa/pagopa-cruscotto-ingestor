package it.pagopa.cruscotto.ingestion.ingestor;

import it.pagopa.cruscotto.ingestion.entity.EntityName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IngestionConfigTest {

    @Test
    void shouldResolveCatchupMaxDurationForEventsWf() {
        IngestionConfig config = new IngestionConfig();
        config.getGuardrails().setMaxDuration(Duration.ofMinutes(15));
        config.getEventsWf().getCatchup().setEnabled(true);
        config.getEventsWf().getCatchup().setMaxDuration(Duration.ofMinutes(30));

        Duration resolved = config.resolveMaxDurationForRun(EntityName.EVENTS_WF.name(), true);

        assertEquals(Duration.ofMinutes(30), resolved);
    }

    @Test
    void shouldResolveCatchupWindowOnlyWhenLagThresholdExceeded() {
        IngestionConfig config = new IngestionConfig();
        config.getAdx().setWindows(Map.of(EntityName.EVENTS_WF, Duration.ofMinutes(2)));
        config.getEventsWf().getCatchup().setEnabled(true);
        config.getEventsWf().getCatchup().setLagThreshold(Duration.ofMinutes(30));
        config.getEventsWf().getCatchup().setWindow(Duration.ofMinutes(8));

        Duration windowLagHigh = config.resolveWindowForRun(
                EntityName.EVENTS_WF,
                Instant.parse("2026-07-14T10:00:00Z"),
                Instant.parse("2026-07-14T11:00:00Z"));
        Duration windowLagLow = config.resolveWindowForRun(
                EntityName.EVENTS_WF,
                Instant.parse("2026-07-14T10:40:00Z"),
                Instant.parse("2026-07-14T11:00:00Z"));

        assertEquals(Duration.ofMinutes(8), windowLagHigh);
        assertEquals(Duration.ofMinutes(2), windowLagLow);
    }
}
