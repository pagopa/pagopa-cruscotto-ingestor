package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Contratto di deserializzazione di {@code search_filter.filter_json} (scritto dal BE come
 * {@code SearchBulkFilterDTO}). Il {@code paymentPeriod} e' un datetime al secondo: se il campo fosse
 * mappato a LocalDate, la stringa con ora/min/sec farebbe fallire il parse -> ricerca FILTER KO.
 */
class PerimeterFilterTest {

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void deserializesPaymentPeriodWithSecondPrecision() throws Exception {
        // Formato prodotto dal BE (LocalDateTime): ora/minuti/secondi.
        String json = "{\"paymentPeriod\":{\"from\":\"2026-03-23T10:15:30\",\"to\":\"2026-03-24T18:45:59\"}}";

        PerimeterFilter filter = objectMapper.readValue(json, PerimeterFilter.class);

        assertNotNull(filter.getPaymentPeriod());
        assertEquals(LocalDateTime.parse("2026-03-23T10:15:30"), filter.getPaymentPeriod().getFrom());
        assertEquals(LocalDateTime.parse("2026-03-24T18:45:59"), filter.getPaymentPeriod().getTo());
    }

    @Test
    void ignoresUnknownPropertiesAndTolueratesMissingPeriod() throws Exception {
        String json = "{\"touchpoints\":[\"APP\"],\"psps\":[1,2],\"unknownFutureField\":123}";

        PerimeterFilter filter = objectMapper.readValue(json, PerimeterFilter.class);

        assertNull(filter.getPaymentPeriod());
        assertEquals(List.of("APP"), filter.getTouchpoints());
        assertEquals(List.of(1, 2), filter.getPsps());
    }

    @Test
    void deserializesCreditorsAsIntegerIds() throws Exception {
        // Contratto BE aggiornato: creditors passa da List<String> a List<Integer> (id anagrafica).
        String json = "{\"creditors\":[1,2],\"channels\":[3],\"stations\":[4],\"technologicalPartners\":[5]}";

        PerimeterFilter filter = objectMapper.readValue(json, PerimeterFilter.class);

        assertEquals(List.of(1, 2), filter.getCreditors());
        assertEquals(List.of(3), filter.getChannels());
        assertEquals(List.of(4), filter.getStations());
        assertEquals(List.of(5), filter.getTechnologicalPartners());
    }
}
