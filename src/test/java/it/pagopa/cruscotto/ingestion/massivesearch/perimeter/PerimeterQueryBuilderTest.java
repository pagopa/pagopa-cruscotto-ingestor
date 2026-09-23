package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

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

        // from inclusivo, to esclusivo, senza troncamento al giorno (niente atStartOfDay/plusDays)
        assertTrue(query.sql().contains("t.payment_date >= :paymentFrom"), query.sql());
        assertTrue(query.sql().contains("t.payment_date < :paymentTo"), query.sql());
        assertEquals(from, query.params().getValue("paymentFrom"));
        assertEquals(to, query.params().getValue("paymentTo"));
    }

    @Test
    void noPaymentPeriodProducesNoDateBounds() {
        PerimeterQuery query = builder.build(new PerimeterFilter());

        assertTrue(!query.sql().contains(":paymentFrom") && !query.sql().contains(":paymentTo"), query.sql());
    }
}
