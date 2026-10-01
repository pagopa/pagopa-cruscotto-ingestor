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
    void paymentPeriodAlsoEmitsDateEventBoundsForPartitionPruning() {
        PerimeterFilter filter = new PerimeterFilter();
        PerimeterFilter.PaymentPeriod period = new PerimeterFilter.PaymentPeriod();
        period.setFrom(LocalDateTime.parse("2026-03-23T10:15:30"));
        period.setTo(LocalDateTime.parse("2026-03-24T18:45:59"));
        filter.setPaymentPeriod(period);

        PerimeterQuery query = builder.build(filter);

        // Senza questi bound il JOIN apre tutte le partizioni mensili di position_tokens.
        // '<=' sul bound superiore: paymentTo e' esclusivo sul timestamp ma inclusivo sulla date.
        assertTrue(query.sql().contains("t.date_event >= CAST(:paymentFrom AS date)"), query.sql());
        assertTrue(query.sql().contains("t.date_event <= CAST(:paymentTo AS date)"), query.sql());
    }

    @Test
    void dateEventBoundsFollowTheProvidedPeriodBoundsIndividually() {
        PerimeterFilter filter = new PerimeterFilter();
        PerimeterFilter.PaymentPeriod period = new PerimeterFilter.PaymentPeriod();
        period.setFrom(LocalDateTime.parse("2026-03-23T10:15:30"));
        filter.setPaymentPeriod(period);

        PerimeterQuery query = builder.build(filter);

        assertTrue(query.sql().contains("t.date_event >= CAST(:paymentFrom AS date)"), query.sql());
        assertTrue(!query.sql().contains(":paymentTo"), query.sql());
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
    void technologicalPartnersAreFilteredOnTheirOwnSideColumn() {
        PerimeterFilter filter = new PerimeterFilter();
        filter.setTechnologicalPartnersPa(List.of(7, 8));
        filter.setTechnologicalPartnersPsp(List.of(9));

        PerimeterQuery query = builder.build(filter);

        // anag_intermediario_pa e anag_intermediario_psp hanno sequenze indipendenti: ogni id deve
        // colpire solo la colonna del proprio lato, altrimenti si generano falsi positivi.
        assertTrue(query.sql().contains("t.intermediario_pa IN (:technologicalPartnersPa)"), query.sql());
        assertTrue(query.sql().contains("t.intermediario_psp IN (:technologicalPartnersPsp)"), query.sql());
        assertTrue(!query.sql().contains("t.intermediario_pa IN (:technologicalPartnersPsp)"), query.sql());
        assertTrue(!query.sql().contains("t.intermediario_psp IN (:technologicalPartnersPa)"), query.sql());
        assertEquals(List.of(7, 8), query.params().getValue("technologicalPartnersPa"));
        assertEquals(List.of(9), query.params().getValue("technologicalPartnersPsp"));
    }

    @Test
    void technologicalPartnerSidesAreCombinedInAnd() {
        PerimeterFilter filter = new PerimeterFilter();
        filter.setTechnologicalPartnersPa(List.of(7));
        filter.setTechnologicalPartnersPsp(List.of(9));

        PerimeterQuery query = builder.build(filter);

        // Due dimensioni indipendenti, come psp/canale/stazione: il token deve soddisfarle entrambe.
        assertTrue(query.sql().contains("t.intermediario_pa IN (:technologicalPartnersPa)"
            + " AND t.intermediario_psp IN (:technologicalPartnersPsp)"), query.sql());
        assertTrue(!query.sql().contains(" OR "), query.sql());
    }

    @Test
    void noTechnologicalPartnersProducesNoIntermediaryCondition() {
        PerimeterQuery query = builder.build(new PerimeterFilter());

        assertTrue(!query.sql().contains("intermediario_pa") && !query.sql().contains("intermediario_psp"), query.sql());
    }

    @Test
    void noPaymentPeriodProducesNoDateBounds() {
        PerimeterQuery query = builder.build(new PerimeterFilter());

        assertTrue(!query.sql().contains(":paymentFrom") && !query.sql().contains(":paymentTo"), query.sql());
    }
}
