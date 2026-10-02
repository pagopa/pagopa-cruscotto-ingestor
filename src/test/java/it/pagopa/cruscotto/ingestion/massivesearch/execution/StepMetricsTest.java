package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * La diagnostica e' uno strumento di supporto: non deve mai alterare l'esito di un'esecuzione.
 * Qui si verifica che sia serializzabile (finisce in una colonna {@code jsonb}) e che distingua
 * "non misurato" da zero.
 */
class StepMetricsTest {

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void nullValuesAreIgnoredSoNotMeasuredIsNotConfusedWithZero() {
        StepMetrics metrics = StepMetrics.create()
            .with("rows", 0L)
            .with("from", null)
            .with(null, "ignorata");

        assertThat(metrics.asMap()).containsOnlyKeys("rows");
        assertThat(metrics.asMap()).containsEntry("rows", 0L);
    }

    @Test
    void emptyMetricsAreRecognisedSoTheColumnStaysNull() {
        assertThat(StepMetrics.create().isEmpty()).isTrue();
        assertThat(StepMetrics.create().with("rows", 1L).isEmpty()).isFalse();
    }

    /** Le date sono presenti fra le metriche della finestra: devono essere serializzabili. */
    @Test
    void serialisesTheValuesActuallyEmittedByThePipeline() throws Exception {
        StepMetrics metrics = StepMetrics.create()
            .with("shape", "UNION")
            .with("rows", 1234L)
            .with("reused", false)
            .with("batchSize", 500)
            .with("from", LocalDateTime.parse("2026-03-01T00:00:00"))
            .with("keysPerSec", 42L);

        String json = objectMapper.writeValueAsString(metrics.asMap());

        assertThat(json).contains("\"shape\":\"UNION\"");
        assertThat(json).contains("\"rows\":1234");
        assertThat(json).contains("\"reused\":false");
        // ISO e non array [2026,3,1,0,0]: la colonna deve restare leggibile e filtrabile con
        // metrics->>'from', indipendentemente da come e' configurato l'ObjectMapper iniettato.
        assertThat(json).contains("\"from\":\"2026-03-01T00:00\"");
    }

    /** Il tipo del template/report e' un enum: va in JSON come nome, non come oggetto. */
    @Test
    void enumsAreStoredByName() {
        StepMetrics metrics = StepMetrics.create().with("resolvedTemplate", ReportType.POSITION);

        assertThat(metrics.asMap()).containsEntry("resolvedTemplate", "POSITION");
    }

    @Test
    void theCollectedViewIsImmutableSoCallersCannotAlterItAfterwards() {
        StepMetrics metrics = StepMetrics.create().with("rows", 1L);

        assertThat(metrics.asMap()).hasSize(1);
        org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
            () -> metrics.asMap().put("altra", 2L));
    }
}
