package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.perimeter.PerimeterFileRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;
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
    private MassiveSearchProperties properties;
    private AnalysisWindowResolver resolver;

    @BeforeEach
    void setUp() {
        perimeterFileRepository = mock(PerimeterFileRepository.class);
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        properties = new MassiveSearchProperties();
        resolver = new AnalysisWindowResolver(perimeterFileRepository, objectMapper, properties);
    }

    private void givenFilterJson(String json) {
        when(perimeterFileRepository.readFilterJson(instanceId)).thenReturn(Optional.of(json));
    }

    /** Il default di codice e' 0 (ambienti senza purge): il lookback va abilitato esplicitamente. */
    private void givenLookbackMonths(int months) {
        properties.getExecution().setDefaultLookbackMonths(months);
    }

    @Test
    void noFilterRow_withoutLookbackAnalysesFullHistory() {
        when(perimeterFileRepository.readFilterJson(instanceId)).thenReturn(Optional.empty());
        assertThat(resolver.resolve(instanceId).hasBounds()).isFalse();
    }

    @Test
    void noPaymentPeriod_withoutLookbackAnalysesFullHistory() {
        givenFilterJson("{\"touchpoints\":[\"PAGOPA_CHECKOUT\"]}");
        assertThat(resolver.resolve(instanceId).hasBounds()).isFalse();
    }

    @Test
    void noFilterRow_appliesDefaultLookbackWhenConfigured() {
        givenLookbackMonths(6);
        when(perimeterFileRepository.readFilterJson(instanceId)).thenReturn(Optional.empty());
        assertDefaultLookback(resolver.resolve(instanceId));
    }

    @Test
    void noPaymentPeriod_appliesDefaultLookbackWhenConfigured() {
        givenLookbackMonths(6);
        givenFilterJson("{\"touchpoints\":[\"PAGOPA_CHECKOUT\"]}");
        assertDefaultLookback(resolver.resolve(instanceId));
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
    void openLowerBound_getsDefaultLookbackWhenCompatible() {
        givenLookbackMonths(6);
        LocalDateTime to = LocalDateTime.now().plusDays(1);
        givenFilterJson("{\"paymentPeriod\":{\"to\":\"" + to + "\"}}");
        AnalysisWindow window = resolver.resolve(instanceId);
        assertDefaultLookback(window);
        assertThat(window.toExclusive()).isEqualTo(to);
    }

    @Test
    void openLowerBound_defaultIsSkippedWhenItWouldEmptyThePeriod() {
        givenLookbackMonths(6);
        // Periodo interamente precedente al lookback: applicare il default lo renderebbe vuoto.
        givenFilterJson("{\"paymentPeriod\":{\"to\":\"2020-03-23T02:00:00\"}}");
        AnalysisWindow window = resolver.resolve(instanceId);
        assertThat(window.fromInclusive()).isNull();
        assertThat(window.toExclusive()).isEqualTo(LocalDateTime.parse("2020-03-23T02:00:00"));
    }

    @Test
    void openUpperBound_isAllowed() {
        givenFilterJson("{\"paymentPeriod\":{\"from\":\"2026-03-23T00:30:00\"}}");
        AnalysisWindow window = resolver.resolve(instanceId);
        assertThat(window.fromInclusive()).isEqualTo(LocalDateTime.parse("2026-03-23T00:30:00"));
        assertThat(window.toExclusive()).isNull();
    }

    @Test
    void malformedJson_staysLenientWithDefaultLookback() {
        givenLookbackMonths(6);
        // JSON malformato != date sbagliate: resta permissivo (solo il default), non fallisce.
        givenFilterJson("{not-json");
        assertDefaultLookback(resolver.resolve(instanceId));
    }

    /** Il default e' una regola funzionale: la ricerca guarda solo gli ultimi N mesi. */
    private void assertDefaultLookback(AnalysisWindow window) {
        LocalDateTime expected = LocalDateTime.now().minusMonths(properties.getExecution().getDefaultLookbackMonths());
        assertThat(window.fromInclusive()).isCloseTo(expected, within(1, ChronoUnit.MINUTES));
    }
}
