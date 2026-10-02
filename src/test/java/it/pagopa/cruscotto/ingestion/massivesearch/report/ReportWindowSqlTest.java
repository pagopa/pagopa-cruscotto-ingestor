package it.pagopa.cruscotto.ingestion.massivesearch.report;

import it.pagopa.cruscotto.ingestion.massivesearch.execution.AnalysisWindow;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the analysis-window fragment: the predicate must be emitted only for the bounds actually
 * present (so it stays indexable) and must pair every {@code inserted_timestamp} bound with the
 * redundant {@code date_event} bound that lets PostgreSQL prune the monthly partitions.
 */
class ReportWindowSqlTest {

    private static final LocalDateTime FROM = LocalDateTime.parse("2026-03-01T00:00:00");
    private static final LocalDateTime TO = LocalDateTime.parse("2026-04-01T00:00:00");

    @Test
    void unboundedWindowEmitsNoPredicate() {
        assertEquals("", ReportWindowSql.tokenWindow("t", AnalysisWindow.none()));
        assertEquals("", ReportWindowSql.tokenWindow("t", null));
    }

    /**
     * Il bound correlato e' asimmetrico: un figlio non puo' precedere il proprio padre (token
     * agganciato a una posizione nata nelle 24h precedenti, transfer dallo stesso evento del token,
     * extra info entro la sessione di pagamento). Il margine serve solo a coprire la mezzanotte.
     */
    @Test
    void childOfTokenAppliesTheMarginOnlyForward() {
        String sql = ReportWindowSql.childOfToken("tr", "t", 2);

        assertTrue(sql.contains("tr.date_event >= t.date_event"), sql);
        assertTrue(sql.contains("tr.date_event <= t.date_event + CAST(:childMarginDays AS integer)"), sql);
        assertFalse(sql.contains("- CAST(:childMarginDays AS integer)"), sql);
    }

    @Test
    void aNonPositiveMarginDisablesBothChildBounds() {
        AnalysisWindow window = new AnalysisWindow(FROM, TO);

        assertEquals("", ReportWindowSql.childOfToken("tr", "t", 0));
        assertEquals("", ReportWindowSql.childOfToken("tr", "t", -1));
        assertEquals("", ReportWindowSql.childOfWindowStart("tr", window, 0));
        assertEquals("", ReportWindowSql.childOfWindowStart("tr", window, -1));
        assertEquals("", ReportWindowSql.childOfTokenUpTo("tr", "t", "t.parent_last_date", 0));
    }

    /**
     * Il bound costante dalla finestra deve emettere <strong>solo</strong> l'estremo inferiore:
     * {@code child.date_event >= token.date_event >= date(winFrom)} e' sempre vero, mentre il
     * simmetrico superiore escluderebbe di nuovo le righe create da una SPO tardiva, annullando
     * {@link ReportWindowSql#childOfTokenUpTo}. Guardia contro quella regressione.
     */
    @Test
    void childOfWindowStartEmitsTheLowerBoundOnly() {
        String sql = ReportWindowSql.childOfWindowStart("tr", new AnalysisWindow(FROM, TO), 2);

        assertTrue(sql.contains("tr.date_event >= CAST(:winFrom AS date)"), sql);
        assertFalse(sql.contains(":winTo"), sql);
        // Nessun riferimento al padre: e' l'assenza di correlazione a renderlo potabile in planning.
        assertFalse(sql.contains("t.date_event"), sql);
    }

    @Test
    void childOfWindowStartEmitsNothingWithoutALowerBound() {
        assertEquals("", ReportWindowSql.childOfWindowStart("tr", AnalysisWindow.none(), 2));
        assertEquals("", ReportWindowSql.childOfWindowStart("tr", null, 2));
        assertEquals("", ReportWindowSql.childOfWindowStart("tr", new AnalysisWindow(null, TO), 2));
    }

    /**
     * L'estremo superiore dei figli del token e' l'ultima data che ha toccato la posizione, non un
     * margine fisso: copre le {@code extra_info} generate da una {@code sendPaymentOutcome} tardiva,
     * che portano la data dell'evento tardivo ma il {@code fk_token} del token originale.
     */
    @Test
    void childOfTokenUpToUsesTheParentLastDateAsUpperBound() {
        String sql = ReportWindowSql.childOfTokenUpTo("ei", "t", "t.parent_last_date", 2);

        assertTrue(sql.contains("ei.date_event >= t.date_event"), sql);
        assertTrue(sql.contains("ei.date_event <= t.parent_last_date + CAST(:childMarginDays AS integer)"), sql);
        assertFalse(sql.contains("- CAST(:childMarginDays"), sql);
    }

    /** L'ultima data della posizione include i giorni registrati in {@code date_events}. */
    @Test
    void positionLastDateCombinesBirthAndDateEvents() {
        String expr = ReportWindowSql.positionLastDate("p2");

        assertTrue(expr.startsWith("GREATEST(p2.date_event"), expr);
        assertTrue(expr.contains("jsonb_array_elements_text"), expr);
        // Il cast a date deve restare protetto dal filtro sul formato: un elemento malformato
        // abortirebbe l'intera query del report.
        assertTrue(expr.contains("de.d ~ "), expr);
        assertTrue(expr.contains("MAX(de.d::date)"), expr);
        // Difesa contro un date_events non-array (jsonb_array_elements_text fallirebbe a runtime).
        assertTrue(expr.contains("jsonb_typeof(p2.date_events) = 'array'"), expr);
    }

    @Test
    void boundedWindowEmitsDirectComparisons() {
        String sql = ReportWindowSql.tokenWindow("t", new AnalysisWindow(FROM, TO));

        assertTrue(sql.contains("t.inserted_timestamp >= CAST(:winFrom AS timestamp)"), sql);
        assertTrue(sql.contains("t.inserted_timestamp < CAST(:winTo AS timestamp)"), sql);
        // La vecchia forma ":winFrom IS NULL OR ..." restava nel piano e non era indicizzabile.
        assertFalse(sql.contains("IS NULL"), sql);
    }

    @Test
    void onlyThePresentBoundIsEmitted() {
        String openUpper = ReportWindowSql.tokenWindow("t", new AnalysisWindow(FROM, null));
        assertTrue(openUpper.contains(":winFrom"), openUpper);
        assertFalse(openUpper.contains(":winTo"), openUpper);

        String openLower = ReportWindowSql.tokenWindow("t", new AnalysisWindow(null, TO));
        assertFalse(openLower.contains(":winFrom"), openLower);
        assertTrue(openLower.contains(":winTo"), openLower);
    }

    @Test
    void everyTimestampBoundIsPairedWithThePartitionKeyBound() {
        // date_event = date(inserted_timestamp) per costruzione (insert registry-gated, UPDATE che non
        // tocca ne' l'una ne' l'altra): il bound ridondante non cambia il result set ma consente il
        // partition pruning, senza il quale ogni LATERAL apre tutte le ~25 partizioni mensili.
        String both = ReportWindowSql.tokenWindow("t", new AnalysisWindow(FROM, TO));
        assertTrue(both.contains("t.date_event >= CAST(:winFrom AS date)"), both);
        assertTrue(both.contains("t.date_event <= CAST(:winTo AS date)"), both);

        // Il bound di pruning segue sempre quello vero: mai da solo, mai per un bound assente.
        String openUpper = ReportWindowSql.tokenWindow("t", new AnalysisWindow(FROM, null));
        assertTrue(openUpper.contains("t.date_event >= CAST(:winFrom AS date)"), openUpper);
        assertFalse(openUpper.contains("<= CAST(:winTo AS date)"), openUpper);

        assertFalse(ReportWindowSql.tokenWindow("t", AnalysisWindow.none()).contains("date_event"));
    }

    @Test
    void theUpperPartitionBoundIsInclusiveOnTheTruncatedDate() {
        // winTo e' esclusivo sul timestamp, ma la sua troncatura a date e' l'ultimo giorno ammissibile
        // e va inclusa: con '<' si perderebbero i token dell'ultimo giorno della finestra.
        String sql = ReportWindowSql.tokenWindow("t", new AnalysisWindow(FROM, TO));
        assertTrue(sql.contains("t.date_event <= CAST(:winTo AS date)"), sql);
        assertFalse(sql.contains("t.date_event < CAST(:winTo AS date)"), sql);
    }

    @Test
    void bindsOnlyTheParametersReferencedByThePredicate() {
        MapSqlParameterSource both = new MapSqlParameterSource();
        ReportWindowSql.bind(both, new AnalysisWindow(FROM, TO));
        assertEquals(FROM, both.getValue("winFrom"));
        assertEquals(TO, both.getValue("winTo"));

        MapSqlParameterSource lowerOnly = new MapSqlParameterSource();
        ReportWindowSql.bind(lowerOnly, new AnalysisWindow(FROM, null));
        assertEquals(FROM, lowerOnly.getValue("winFrom"));
        assertFalse(lowerOnly.hasValue("winTo"));

        MapSqlParameterSource unbounded = new MapSqlParameterSource();
        ReportWindowSql.bind(unbounded, AnalysisWindow.none());
        assertFalse(unbounded.hasValue("winFrom"));
        assertFalse(unbounded.hasValue("winTo"));
        assertNull(unbounded.getValues().get("winFrom"));
    }
}
