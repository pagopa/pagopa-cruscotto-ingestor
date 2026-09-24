package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.massivesearch.perimeter.PerimeterFileRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifica la risoluzione della finestra di analisi da {@code search_filter.filter_json} e, in
 * particolare, la validazione del {@code paymentPeriod}: date invertite o range degenere devono far
 * fallire lo step (esecuzione FAILED) invece di produrre silenziosamente un report vuoto.
 */
class AnalysisWindowResolverTest {

    private final UUID instanceId = UUID.randomUUID();
    private PerimeterFileRepository perimeterFileRepository;
    private AnalysisWindowResolver resolver;

    @BeforeEach
    void setUp() {
        perimeterFileRepository = mock(PerimeterFileRepository.class);
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        resolver = new AnalysisWindowResolver(perimeterFileRepository, objectMapper);
    }

    private void givenFilterJson(String json) {
        when(perimeterFileRepository.readFilterJson(instanceId)).thenReturn(Optional.of(json));
    }

    @Test
    void noFilterRow_returnsNoneWindow() {
        when(perimeterFileRepository.readFilterJson(instanceId)).thenReturn(Optional.empty());
        assertThat(resolver.resolve(instanceId).hasBounds()).isFalse();
    }

    @Test
    void noPaymentPeriod_returnsNoneWindow() {
        givenFilterJson("{\"touchpoints\":[\"PAGOPA_CHECKOUT\"]}");
        assertThat(resolver.resolve(instanceId).hasBounds()).isFalse();
    }

    @Test
    void validPeriod_returnsBoundedWindow() {
        givenFilterJson("{\"paymentPeriod\":{\"from\":\"2026-03-23T00:30:00\",\"to\":\"2026-03-23T02:00:00\"}}");
        AnalysisWindow window = resolver.resolve(instanceId);
        assertThat(window.fromInclusive()).isEqualTo(LocalDateTime.parse("2026-03-23T00:30:00"));
        assertThat(window.toExclusive()).isEqualTo(LocalDateTime.parse("2026-03-23T02:00:00"));
    }

    @Test
    void invertedDates_throwsExecutionException() {
        // from > to: date invertite -> deve fallire lo step ANALYSIS_WINDOW.
        givenFilterJson("{\"paymentPeriod\":{\"from\":\"2026-03-23T02:00:00\",\"to\":\"2026-03-23T00:30:00\"}}");
        assertThatThrownBy(() -> resolver.resolve(instanceId))
            .isInstanceOf(MassiveSearchExecutionException.class)
            .hasMessageContaining("must be strictly before");
    }

    @Test
    void equalDates_throwsExecutionException() {
        // from == to: range degenere [from, from) -> vuoto per costruzione, trattato come input errato.
        givenFilterJson("{\"paymentPeriod\":{\"from\":\"2026-03-23T00:30:00\",\"to\":\"2026-03-23T00:30:00\"}}");
        assertThatThrownBy(() -> resolver.resolve(instanceId))
            .isInstanceOf(MassiveSearchExecutionException.class);
    }

    @Test
    void openLowerBound_isAllowed() {
        givenFilterJson("{\"paymentPeriod\":{\"to\":\"2026-03-23T02:00:00\"}}");
        AnalysisWindow window = resolver.resolve(instanceId);
        assertThat(window.fromInclusive()).isNull();
        assertThat(window.toExclusive()).isEqualTo(LocalDateTime.parse("2026-03-23T02:00:00"));
    }

    @Test
    void openUpperBound_isAllowed() {
        givenFilterJson("{\"paymentPeriod\":{\"from\":\"2026-03-23T00:30:00\"}}");
        AnalysisWindow window = resolver.resolve(instanceId);
        assertThat(window.fromInclusive()).isEqualTo(LocalDateTime.parse("2026-03-23T00:30:00"));
        assertThat(window.toExclusive()).isNull();
    }

    @Test
    void malformedJson_staysLenientNoWindow() {
        // JSON malformato != date sbagliate: resta permissivo (nessuna finestra), non fallisce.
        givenFilterJson("{not-json");
        assertThat(resolver.resolve(instanceId).hasBounds()).isFalse();
    }
}
