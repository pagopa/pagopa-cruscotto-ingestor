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
