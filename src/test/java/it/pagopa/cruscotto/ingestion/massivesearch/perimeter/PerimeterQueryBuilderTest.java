package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    // ---- Posizioni senza tentativi (D5) -------------------------------------------------------

    private static PerimeterFilter periodOnly() {
        PerimeterFilter filter = new PerimeterFilter();
        PerimeterFilter.PaymentPeriod period = new PerimeterFilter.PaymentPeriod();
        period.setFrom(LocalDateTime.parse("2026-01-01T00:00:00"));
        period.setTo(LocalDateTime.parse("2026-02-01T00:00:00"));
        filter.setPaymentPeriod(period);
        return filter;
    }

    @Test
    void periodWithoutTokenFiltersAlsoSelectsPositionsByTheirOwnDate() {
        // Scenario del cliente: posizione a gennaio senza tentativi, tentativo a marzo. Cercando
        // gennaio la chiave deve entrare nel perimetro, altrimenti il report Position non puo'
        // produrne la riga. Il solo JOIN sui tentativi non la troverebbe mai.
        String sql = builder.build(periodOnly()).sql();

        String positionBranch = sql.substring(sql.indexOf(" UNION ") + " UNION ".length());
        assertTrue(positionBranch.startsWith("SELECT p.pa_emittente AS pa, p.nav AS nav"), sql);
        assertFalse(positionBranch.contains("position_tokens"), "il ramo posizioni non deve richiedere tentativi: " + sql);
        assertTrue(positionBranch.contains("p.inserted_timestamp >= :paymentFrom"), sql);
        assertTrue(positionBranch.contains("p.inserted_timestamp < :paymentTo"), sql);
        // Pruning anche su position: senza, il ramo aprirebbe tutte le partizioni mensili.
        assertTrue(positionBranch.contains("p.date_event >= CAST(:paymentFrom AS date)"), sql);
        assertTrue(positionBranch.contains("p.date_event <= CAST(:paymentTo AS date)"), sql);
    }

    @Test
    void periodWithoutTokenFiltersKeepsTheTokenBranchForTheAttemptReports() {
        // I report Tentativi e Transfer selezionano per data del token: a cavallo di un bound
        // posizione e tentativo possono cadere da lati opposti, quindi il ramo tentativi resta.
        String sql = builder.build(periodOnly()).sql();

        String tokenBranch = sql.substring(0, sql.indexOf(" UNION "));
        assertTrue(tokenBranch.contains("JOIN sert_ingestor.position_tokens t ON t.fk_position = p.id"), sql);
        assertTrue(tokenBranch.contains("t.inserted_timestamp >= :paymentFrom"), sql);
        assertTrue(tokenBranch.contains("t.date_event >= CAST(:paymentFrom AS date)"), sql);
        assertFalse(tokenBranch.contains("p.inserted_timestamp"), sql);
    }

    @Test
    void theUnionDeduplicatesAndIsOrderedOnce() {
        String sql = builder.build(periodOnly()).sql();

        // UNION e non UNION ALL: la stessa chiave puo' uscire da entrambi i rami.
        assertTrue(sql.contains(" UNION SELECT "), sql);
        assertFalse(sql.contains("UNION ALL"), sql);
        assertFalse(sql.contains("DISTINCT"), "ridondante sotto UNION: " + sql);
        assertTrue(sql.endsWith(" ORDER BY pa, nav"), sql);
        assertEquals(sql.indexOf("ORDER BY"), sql.lastIndexOf("ORDER BY"), sql);
    }

    @ParameterizedTest
    @MethodSource("tokenOnlyFilters")
    void anyTokenFilterExcludesPositionsWithoutAttempts(PerimeterFilter filter) {
        // Una posizione senza tentativi non puo' soddisfare un filtro sul tentativo: il ramo
        // posizioni non deve comparire, e la query resta quella di prima.
        PerimeterFilter.PaymentPeriod period = new PerimeterFilter.PaymentPeriod();
        period.setFrom(LocalDateTime.parse("2026-01-01T00:00:00"));
        filter.setPaymentPeriod(period);

        String sql = builder.build(filter).sql();

        assertFalse(sql.contains("UNION"), sql);
        assertTrue(sql.startsWith("SELECT DISTINCT p.pa_emittente AS pa, p.nav AS nav"), sql);
        assertTrue(sql.contains("JOIN sert_ingestor.position_tokens t ON t.fk_position = p.id"), sql);
        assertFalse(sql.contains("p.inserted_timestamp"), sql);
    }

    static Stream<PerimeterFilter> tokenOnlyFilters() {
        PerimeterFilter statuses = new PerimeterFilter();
        statuses.setPaymentStatuses(List.of(PerimeterPaymentStatus.OK));
        PerimeterFilter touchpoints = new PerimeterFilter();
        touchpoints.setTouchpoints(List.of("Touchpoint PSP"));
        PerimeterFilter methods = new PerimeterFilter();
        methods.setPaymentMethods(List.of("CP"));
        PerimeterFilter amount = new PerimeterFilter();
        PerimeterFilter.AmountFilter exact = new PerimeterFilter.AmountFilter();
        exact.setExact(BigDecimal.TEN);
        amount.setAmount(exact);
        PerimeterFilter psps = new PerimeterFilter();
        psps.setPsps(List.of(1));
        PerimeterFilter partnersPa = new PerimeterFilter();
        partnersPa.setTechnologicalPartnersPa(List.of(1));
        PerimeterFilter partnersPsp = new PerimeterFilter();
        partnersPsp.setTechnologicalPartnersPsp(List.of(1));
        PerimeterFilter channels = new PerimeterFilter();
        channels.setChannels(List.of(1));
        PerimeterFilter stations = new PerimeterFilter();
        stations.setStations(List.of(1));
        return Stream.of(statuses, touchpoints, methods, amount, psps, partnersPa, partnersPsp, channels, stations);
    }

    @Test
    void creditorsApplyToBothBranches() {
        // L'ente creditore e' un attributo della posizione: deve restringere anche il ramo posizioni,
        // altrimenti la UNION reintrodurrebbe gli enti esclusi.
        PerimeterFilter filter = periodOnly();
        filter.setCreditors(List.of(1));

        String sql = builder.build(filter).sql();

        String tokenBranch = sql.substring(0, sql.indexOf(" UNION "));
        String positionBranch = sql.substring(sql.indexOf(" UNION "));
        assertTrue(tokenBranch.contains("p.pa_emittente IN (SELECT pae.codice FROM"), sql);
        assertTrue(positionBranch.contains("p.pa_emittente IN (SELECT pae.codice FROM"), sql);
    }

    @Test
    void noFiltersAtAllSkipsTheTokenJoin() {
        // Senza periodo ne' filtri sul tentativo ogni chiave del ramo tentativi e' anche una
        // posizione: il JOIN sarebbe solo costo, su tutte le partizioni.
        String sql = builder.build(new PerimeterFilter()).sql();

        assertFalse(sql.contains("position_tokens"), sql);
        assertFalse(sql.contains("UNION"), sql);
        assertTrue(sql.startsWith("SELECT DISTINCT p.pa_emittente AS pa, p.nav AS nav FROM sert_ingestor.position p"), sql);
    }

    @Test
    void anEmptyPeriodObjectBehavesLikeNoPeriod() {
        PerimeterFilter filter = new PerimeterFilter();
        filter.setPaymentPeriod(new PerimeterFilter.PaymentPeriod());

        String sql = builder.build(filter).sql();

        assertFalse(sql.contains("UNION"), sql);
        assertFalse(sql.contains("position_tokens"), sql);
    }

    @Test
    void bothBranchesExposeTheColumnsReadByTheGenerator() {
        // PerimeterCsvGenerator legge le colonne per nome ("nav", "pa").
        String sql = builder.build(periodOnly()).sql();

        assertEquals(2, sql.split("SELECT p.pa_emittente AS pa, p.nav AS nav", -1).length - 1, sql);
    }
}
