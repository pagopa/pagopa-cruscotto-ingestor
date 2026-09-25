package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PerimeterQueryBuilderTest {

    private final PerimeterQueryBuilder builder = new PerimeterQueryBuilder(new DbSchemaConfig());

    @Test
    void paymentPeriodIsBoundAsPreciseDateTimeFromInclusiveToExclusive() {
        PerimeterFilter filter = new PerimeterFilter();
        PerimeterFilter.PaymentPeriod period = new PerimeterFilter.PaymentPeriod();
        LocalDateTime from = LocalDateTime.parse("2026-03-23T10:15:30");
        LocalDateTime to = LocalDateTime.parse("2026-03-24T18:45:59");
        period.setFrom(from);
        period.setTo(to);
        filter.setPaymentPeriod(period);

        PerimeterQuery query = builder.build(filter);

        // Finestra su inserted_timestamp (non payment_date): from inclusivo, to esclusivo, senza
        // troncamento al giorno (niente atStartOfDay/plusDays)
        assertTrue(query.sql().contains("t.inserted_timestamp >= :paymentFrom"), query.sql());
        assertTrue(query.sql().contains("t.inserted_timestamp < :paymentTo"), query.sql());
        assertEquals(from, query.params().getValue("paymentFrom"));
        assertEquals(to, query.params().getValue("paymentTo"));
    }

    @Test
    void creditorsAreResolvedFromAnagIdsToCodice() {
        PerimeterFilter filter = new PerimeterFilter();
        filter.setCreditors(List.of(1, 2));

        PerimeterQuery query = builder.build(filter);

        // Il BE invia gli id di anag_pa_emittente, mentre position.pa_emittente contiene il codice:
        // il confronto diretto id/codice non troverebbe mai righe.
        assertTrue(query.sql().contains("p.pa_emittente IN (SELECT pae.codice FROM"), query.sql());
        assertTrue(query.sql().contains("anag_pa_emittente pae WHERE pae.id IN (:creditors)"), query.sql());
        assertEquals(List.of(1, 2), query.params().getValue("creditors"));
    }

    @Test
    void noPaymentPeriodProducesNoDateBounds() {
        PerimeterQuery query = builder.build(new PerimeterFilter());

        assertTrue(!query.sql().contains(":paymentFrom") && !query.sql().contains(":paymentTo"), query.sql());
    }
}
